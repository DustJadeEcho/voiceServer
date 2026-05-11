"""Session — manages a single voice interaction lifecycle.

State machine:
    IDLE → RECORDING → PROCESSING → SENDING → DONE / ERROR
"""

import asyncio
import enum
import logging
import time
from typing import Callable, Coroutine, Dict, Optional

import config
from audio import decode_uplink, encode_downlink, chunk_pcm, PCM_PAYLOAD_SIZE
from asr import ASREngine
from llm import LLMClient
from tts import TTSEngine

logger = logging.getLogger("session")


class State(enum.Enum):
    IDLE = "idle"
    RECORDING = "recording"
    PROCESSING = "processing"
    SENDING = "sending"
    DONE = "done"
    ERROR = "error"


# Callback type: publish(topic: str, payload: bytes, qos: int)
PublishFn = Callable[[str, bytes, int], Coroutine]


class Session:
    """Single voice interaction session.

    Lifecycle:
        1. Created on "start" control message
        2. Receives audio chunks via handle_audio_chunk()
        3. Pipeline triggered on "stop" control message
        4. Cleans up after completion or timeout
    """

    def __init__(
        self,
        session_id: str,
        publish_fn: PublishFn,
        asr: ASREngine,
        llm: LLMClient,
        tts: TTSEngine,
        max_duration: int = 60,
    ):
        self.session_id = session_id
        self.state = State.IDLE
        self._publish = publish_fn
        self._asr = asr
        self._llm = llm
        self._tts = tts

        self._chunks: Dict[int, bytes] = {}  # seq → pcm bytes
        self._total_bytes = 0
        self._max_duration = max_duration
        self._created_at = time.monotonic()
        self._task: Optional[asyncio.Task] = None

    @property
    def age(self) -> float:
        """Seconds since session creation."""
        return time.monotonic() - self._created_at

    def start(self):
        """Transition to RECORDING state."""
        if self.state != State.IDLE:
            logger.warning("[%s] start() called in state %s", self.session_id, self.state)
            return
        self.state = State.RECORDING
        logger.info("[%s] Recording started (max_duration=%ds)", self.session_id, self._max_duration)

    def handle_audio_chunk(self, data: bytes) -> bool:
        """Process an incoming audio chunk.

        Returns True if accepted, False if dropped (wrong state / overflow).
        """
        if self.state not in (State.IDLE, State.RECORDING):
            logger.warning("[%s] Audio chunk in state %s, dropping", self.session_id, self.state)
            return False

        try:
            sid, seq, pcm = decode_uplink(data)
        except ValueError as e:
            logger.warning("[%s] Bad packet: %s", self.session_id, e)
            return False

        if sid != self.session_id:
            logger.warning("[%s] Mismatched session_id %s, dropping", self.session_id, sid)
            return False

        # Auto-transition to RECORDING if we receive audio before start
        if self.state == State.IDLE:
            self.state = State.RECORDING

        # Memory limit check
        if self._total_bytes + len(pcm) > config.MAX_PCM_SIZE:
            logger.warning("[%s] PCM size limit reached (%d bytes), dropping chunk",
                           self.session_id, self._total_bytes)
            return False

        self._chunks[seq] = pcm
        self._total_bytes += len(pcm)
        return True

    async def handle_stop(self):
        """Process stop signal — assemble audio and run the AI pipeline.

        This is the main pipeline entry point.
        """
        if self.state != State.RECORDING:
            logger.warning("[%s] stop() in state %s, ignoring", self.session_id, self.state)
            return

        self.state = State.PROCESSING
        logger.info("[%s] Stop received, %d chunks (%d bytes), assembling...",
                     self.session_id, len(self._chunks), self._total_bytes)

        # Assemble PCM in sequence order
        pcm_data = self._assemble_pcm()
        if not pcm_data:
            await self._send_error("no_audio")
            return

        try:
            await asyncio.wait_for(
                self._run_pipeline(pcm_data),
                timeout=config.SESSION_TIMEOUT,
            )
        except asyncio.TimeoutError:
            logger.warning("[%s] Pipeline timed out", self.session_id)
            await self._send_error("timeout")
        except Exception as e:
            logger.error("[%s] Pipeline error: %s", self.session_id, e, exc_info=True)
            await self._send_error("internal_error")

    def _assemble_pcm(self) -> bytes:
        """Assemble PCM chunks in sequence order, detect gaps."""
        if not self._chunks:
            return b""

        sorted_seqs = sorted(self._chunks.keys())

        # Detect gaps
        expected = set(range(sorted_seqs[0], sorted_seqs[-1] + 1))
        missing = expected - set(sorted_seqs)
        if missing:
            logger.warning("[%s] Missing sequences: %s", self.session_id,
                           sorted(missing)[:20])

        # Concatenate (missing chunks are silently skipped — graceful degradation)
        parts = [self._chunks[seq] for seq in sorted_seqs]
        return b"".join(parts)

    async def _run_pipeline(self, pcm_data: bytes):
        """ASR → LLM (stream) → TTS (per sentence) → MQTT send.

        Implements graceful degradation per design doc:
        - LLM stream interrupt → send partial TTS + error
        - TTS single fail → skip; 2 consecutive → abort
        - Always tries to send whatever it can
        """
        # ── Step 1: ASR ───────────────────────────────────────────────────
        logger.info("[%s] Running ASR...", self.session_id)
        try:
            text = await self._asr.recognize(pcm_data)
            logger.info("[%s] ASR result: %s", self.session_id, text[:100])
        except Exception as e:
            logger.error("[%s] ASR failed: %s", self.session_id, e)
            await self._send_error("asr_failed")
            return

        if not text.strip():
            await self._send_error("empty_speech")
            return

        # ── Step 2: LLM streaming + TTS per sentence ─────────────────────
        logger.info("[%s] Running LLM + TTS pipeline...", self.session_id)
        self.state = State.SENDING
        down_seq = 0
        consecutive_tts_failures = 0
        llm_interrupted = False
        partial_content = False

        try:
            async for sentence in self._llm.stream_sentences(text):
                logger.info("[%s] TTS for: %s", self.session_id, sentence[:50])

                try:
                    pcm_out = await self._tts.synthesize(sentence)
                    consecutive_tts_failures = 0
                    partial_content = True

                    # Send TTS audio as MQTT chunks
                    down_seq = await self._send_audio_chunks(pcm_out, down_seq)

                except Exception as e:
                    consecutive_tts_failures += 1
                    logger.error("[%s] TTS failed for sentence (%d consecutive): %s",
                                 self.session_id, consecutive_tts_failures, e)

                    if consecutive_tts_failures >= 2:
                        logger.error("[%s] 2 consecutive TTS failures, aborting", self.session_id)
                        await self._send_error("tts_failed")
                        return

                    # Single failure — skip and continue with next sentence

        except Exception as e:
            # LLM stream interrupted
            llm_interrupted = True
            logger.error("[%s] LLM stream interrupted: %s", self.session_id, e)

        # ── Step 3: Send completion signal ────────────────────────────────
        if llm_interrupted and not partial_content:
            # LLM failed with no output at all
            await self._send_error("llm_failed")
        elif llm_interrupted:
            # LLM interrupted but we sent partial content
            await self._send_error("llm_stream_interrupted")
        else:
            # Success
            await self._send_done()

    async def _send_audio_chunks(self, pcm_data: bytes, start_seq: int) -> int:
        """Send PCM data as MQTT chunks. Returns next sequence number."""
        chunks = chunk_pcm(pcm_data, PCM_PAYLOAD_SIZE)
        for i, chunk in enumerate(chunks):
            seq = start_seq + i
            packet = encode_downlink(self.session_id, seq, chunk)
            await self._publish(config.TOPIC_DOWN_AUDIO, packet, 0)  # QoS 0
        return start_seq + len(chunks)

    async def _send_done(self):
        """Send done control message."""
        import json
        msg = json.dumps({"event": "done", "session_id": self.session_id})
        await self._publish(config.TOPIC_DOWN_CONTROL, msg.encode(), 1)  # QoS 1
        self.state = State.DONE
        logger.info("[%s] Session done", self.session_id)

    async def _send_error(self, reason: str):
        """Send error control message."""
        import json
        msg = json.dumps({"event": "error", "session_id": self.session_id, "reason": reason})
        await self._publish(config.TOPIC_DOWN_CONTROL, msg.encode(), 1)  # QoS 1
        self.state = State.ERROR
        logger.warning("[%s] Session error: %s", self.session_id, reason)

    def cleanup(self):
        """Release resources."""
        self._chunks.clear()
        self._total_bytes = 0
        if self._task and not self._task.done():
            self._task.cancel()
        logger.debug("[%s] Session cleaned up", self.session_id)
