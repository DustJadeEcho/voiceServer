"""TTS (Text-to-Speech) — MiMo-V2.5-TTS with 24kHz→16kHz resampling.

Two paths:
  - synthesize_stream(): SSE streaming via the OpenAI-compatible gateway.
    First audio chunk arrives ~1.5s after request (measured), vs 5-9s for the
    full non-streaming body — this is the main first-response latency lever.
  - synthesize(): non-streaming fallback (also used by test_apis.py).
"""

import asyncio
import base64
import json
import logging
from collections.abc import AsyncGenerator

import httpx
from openai import OpenAI

import config
from audio import StreamResampler24k16k, resample_pcm

logger = logging.getLogger("tts")


class TTSEngine:
    """MiMo-V2.5-TTS engine (OpenAI-compatible chat/completions with audio).

    Output: 24kHz PCM16LE mono → resampled to 16kHz for the device.
    """

    def __init__(self):
        self._client = OpenAI(
            api_key=config.TTS_API_KEY,
            base_url=config.TTS_BASE_URL,
            timeout=config.API_TIMEOUT,
        )
        # Shared async client for streaming: connection pooling across sentences
        self._aclient = httpx.AsyncClient(
            base_url=config.TTS_BASE_URL,
            headers={"Authorization": f"Bearer {config.TTS_API_KEY}"},
            timeout=httpx.Timeout(connect=5.0, read=config.API_TIMEOUT,
                                  write=10.0, pool=5.0),
        )
        self._model = config.TTS_MODEL
        self._voice = config.TTS_VOICE
        self._style = config.TTS_STYLE
        self._native_rate = config.TTS_NATIVE_RATE
        self._target_rate = config.DEVICE_SAMPLE_RATE

    def _messages(self, text: str) -> list[dict]:
        """Mimo TTS message format: user=style instruction, assistant=text."""
        return [
            {"role": "user", "content": self._style},
            {"role": "assistant", "content": text},
        ]

    # ── Streaming path ───────────────────────────────────────────────────────

    async def synthesize_stream(self, text: str) -> AsyncGenerator[bytes, None]:
        """Yield 16kHz/16bit/mono PCM chunks as the gateway synthesizes.

        Raises on any transport/protocol error; caller decides fallback.
        SSE chunk shape (captured live): choices[0].delta.audio.data = base64 PCM.
        """
        if not text.strip():
            return

        body = {
            "model": self._model,
            "messages": self._messages(text),
            "audio": {"format": "pcm16", "voice": self._voice},
            "stream": True,
        }
        resampler = StreamResampler24k16k()
        total = 0
        async with self._aclient.stream("POST", "/chat/completions",
                                        json=body) as resp:
            if resp.status_code != 200:
                detail = (await resp.aread())[:200]
                raise RuntimeError(f"TTS stream HTTP {resp.status_code}: {detail!r}")
            async for line in resp.aiter_lines():
                if not line.startswith("data:"):
                    continue
                payload = line[5:].strip()
                if not payload or payload == "[DONE]":
                    continue
                try:
                    delta = json.loads(payload)["choices"][0]["delta"]
                except (json.JSONDecodeError, KeyError, IndexError):
                    continue
                audio = delta.get("audio") if isinstance(delta, dict) else None
                data = audio.get("data") if isinstance(audio, dict) else None
                if not data:
                    continue
                pcm16k = resampler.process(base64.b64decode(data))
                if pcm16k:
                    total += len(pcm16k)
                    yield pcm16k
        if total == 0:
            raise RuntimeError("TTS stream produced no audio")
        logger.debug("TTS stream done: %dB 16k PCM for '%s...'", total, text[:30])

    async def aclose(self) -> None:
        await self._aclient.aclose()

    # ── Non-streaming path (fallback + test_apis.py) ─────────────────────────

    async def synthesize(self, text: str) -> bytes:
        """Synthesize text to 16kHz/16bit/mono PCM bytes (whole clip at once)."""
        if not text.strip():
            return b""

        loop = asyncio.get_running_loop()
        try:
            pcm_24k = await loop.run_in_executor(None, self._synthesize_sync, text)
            pcm_16k = resample_pcm(pcm_24k, self._native_rate, self._target_rate)
            return pcm_16k
        except Exception as e:
            logger.error("TTS error for text '%s...': %s", text[:40], e)
            raise

    def _synthesize_sync(self, text: str) -> bytes:
        """Synchronous TTS call (runs in thread pool)."""
        try:
            completion = self._client.chat.completions.create(
                model=self._model,
                messages=self._messages(text),
                audio={"format": "pcm16", "voice": self._voice},
                stream=False,
            )

            message = completion.choices[0].message
            if not hasattr(message, "audio") or not message.audio:
                raise RuntimeError("TTS response has no audio data")

            pcm_bytes = base64.b64decode(message.audio.data)
            if not pcm_bytes:
                raise RuntimeError("TTS returned empty audio")

            logger.debug("TTS synthesized %d bytes (24kHz) for '%s...'",
                         len(pcm_bytes), text[:30])
            return pcm_bytes

        except Exception as e:
            logger.error("TTS API call failed: %s", e)
            raise
