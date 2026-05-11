"""TTS (Text-to-Speech) — MiMo-V2.5-TTS with 24kHz→16kHz resampling."""

import asyncio
import base64
import logging
from typing import Optional

from openai import OpenAI

import config
from audio import resample_pcm

logger = logging.getLogger("tts")


class TTSEngine:
    """MiMo-V2.5-TTS engine.

    Uses the OpenAI-compatible API at api.xiaomimimo.com.
    Non-streaming mode (streaming not yet available for TTS).
    Output: 24kHz PCM16LE mono → resampled to 16kHz for device.
    """

    def __init__(self):
        self._client = OpenAI(
            api_key=config.TTS_API_KEY,
            base_url=config.TTS_BASE_URL,
            timeout=config.API_TIMEOUT,
        )
        self._model = config.TTS_MODEL
        self._voice = config.TTS_VOICE
        self._style = config.TTS_STYLE
        self._native_rate = config.TTS_NATIVE_RATE
        self._target_rate = config.DEVICE_SAMPLE_RATE

    async def synthesize(self, text: str) -> bytes:
        """Synthesize text to 16kHz/16bit/mono PCM bytes.

        Args:
            text: Chinese text to synthesize

        Returns:
            Raw PCM bytes (16kHz, 16-bit, little-endian, mono)

        Raises:
            RuntimeError: on API error or empty response
        """
        if not text.strip():
            return b""

        loop = asyncio.get_event_loop()
        try:
            pcm_24k = await loop.run_in_executor(None, self._synthesize_sync, text)
            pcm_16k = resample_pcm(pcm_24k, self._native_rate, self._target_rate)
            return pcm_16k
        except Exception as e:
            logger.error("TTS error for text '%s...': %s", text[:40], e)
            raise

    def _synthesize_sync(self, text: str) -> bytes:
        """Synchronous TTS call (runs in thread pool).

        Messages format (per Mimo TTS docs):
        - role: user  → style instruction
        - role: assistant → text to synthesize
        """
        messages = [
            {"role": "user", "content": self._style},
            {"role": "assistant", "content": text},
        ]

        try:
            completion = self._client.chat.completions.create(
                model=self._model,
                messages=messages,
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
