"""Session — manages a single voice interaction lifecycle.

State machine:
    IDLE → RECORDING → PROCESSING → SENDING → DONE / ERROR

Latency/robustness features:
    - stop-grace: stop(QoS1) can overtake trailing audio(QoS0); wait briefly.
    - TTS pipelining: synthesize sentence N+1 while sentence N is being sent.
    - Downlink pacing: initial burst then real-time rate, so the MCU's 1-second
      ring buffer (32 KB) never overflows.
"""

import asyncio
import enum
import json
import logging
import time
from collections.abc import AsyncGenerator, Awaitable, Callable

import config
from audio import chunk_pcm, encode_packet
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
PublishFn = Callable[[str, bytes, int], Awaitable[None]]


class Session:
    """Single voice interaction session (keyed by uint32 session number)."""

    def __init__(
        self,
        session_id: int,
        publish_fn: PublishFn,
        asr,
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

        self._chunks: dict[int, bytes] = {}  # seq → pcm bytes
        self._total_bytes = 0
        self._max_duration = max_duration
        self._created_at = time.monotonic()
        self._task: asyncio.Task | None = None

        # Downlink pacing state (token bucket over wall clock)
        self._pace_t0: float | None = None
        self._pace_sent = 0          # bytes already sent
        self._tts_fail_streak = 0    # consecutive TTS failures

    @property
    def age(self) -> float:
        """Seconds since session creation."""
        return time.monotonic() - self._created_at

    def start(self) -> None:
        """Transition to RECORDING state."""
        if self.state != State.IDLE:
            logger.warning("[%s] start() called in state %s", self.session_id, self.state)
            return
        self.state = State.RECORDING
        logger.info("[%s] Recording started (max_duration=%ds)",
                    self.session_id, self._max_duration)

    def handle_audio_chunk(self, seq: int, pcm: bytes) -> bool:
        """Store one already-decoded uplink chunk.

        Returns True if accepted, False if dropped (wrong state / overflow).
        """
        if self.state not in (State.IDLE, State.RECORDING):
            logger.warning("[%s] Audio chunk in state %s, dropping",
                           self.session_id, self.state)
            return False

        # Auto-transition to RECORDING if audio arrives before start
        if self.state == State.IDLE:
            self.state = State.RECORDING

        if self._total_bytes + len(pcm) > config.MAX_PCM_SIZE:
            logger.warning("[%s] PCM size limit reached (%d bytes), dropping chunk",
                           self.session_id, self._total_bytes)
            return False

        self._chunks[seq] = pcm
        self._total_bytes += len(pcm)
        return True

    async def handle_stop(self) -> None:
        """Process stop signal — assemble audio and run the AI pipeline."""
        if self.state != State.RECORDING:
            logger.warning("[%s] stop() in state %s, ignoring", self.session_id, self.state)
            return

        # stop travels on QoS1 and may overtake the last QoS0 audio chunks —
        # give stragglers a moment to arrive before assembling.
        await asyncio.sleep(config.STOP_GRACE_SECONDS)

        self.state = State.PROCESSING
        logger.info("[%s] Stop received, %d chunks (%d bytes), assembling...",
                    self.session_id, len(self._chunks), self._total_bytes)

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
        expected = set(range(sorted_seqs[0], sorted_seqs[-1] + 1))
        missing = expected - set(sorted_seqs)
        if missing:
            logger.warning("[%s] Missing sequences: %s", self.session_id,
                           sorted(missing)[:20])

        return b"".join(self._chunks[seq] for seq in sorted_seqs)

    # ── Pipeline ─────────────────────────────────────────────────────────────

    async def _run_pipeline(self, pcm_data: bytes) -> None:
        """ASR → LLM (stream, sentence split) → TTS (pipelined) → paced send.

        Graceful degradation per design doc: send whatever was produced,
        then report done / partial error.
        """
        # ── Step 1: ASR ──────────────────────────────────────────────────
        logger.info("[%s] Running ASR (%.2fs audio)...",
                    self.session_id, len(pcm_data) / config.DEVICE_BYTES_PER_SEC)
        t0 = time.monotonic()
        try:
            text = await self._asr.recognize(pcm_data)
        except Exception as e:
            logger.error("[%s] ASR failed: %s", self.session_id, e)
            await self._send_error("asr_failed")
            return
        logger.info("[%s] ASR (%.2fs): %s", self.session_id,
                    time.monotonic() - t0, text[:100])

        if not text.strip():
            await self._send_error("empty_speech")
            return

        # ── Step 2: LLM stream → pipelined TTS → paced downlink ─────────
        self.state = State.SENDING
        down_seq = 0
        llm_interrupted = False
        sent_any = False

        try:
            async for pcm_out in self._tts_pipeline(text):
                if pcm_out:
                    down_seq = await self._send_audio_chunks(pcm_out, down_seq)
                    sent_any = True
        except _TTSAbort:
            await self._send_error("tts_failed")
            return
        except Exception as e:
            llm_interrupted = True
            logger.error("[%s] LLM stream interrupted: %s", self.session_id, e)

        # ── Step 3: completion signal ────────────────────────────────────
        if llm_interrupted and not sent_any:
            await self._send_error("llm_failed")
        elif llm_interrupted:
            await self._send_error("llm_stream_interrupted")
        else:
            await self._send_done()

    async def _tts_pipeline(self, text: str) -> AsyncGenerator[bytes, None]:
        """Yield synthesized PCM per sentence, overlapping TTS(N+1) with send(N).

        A sentence's synthesis task is created as soon as the sentence is
        complete; the previous result is yielded (and sent, paced) while the
        next synthesis runs in the background.
        """
        pending: asyncio.Task | None = None
        try:
            async for sentence in self._llm.stream_sentences(text):
                task = asyncio.create_task(self._tts_safe(sentence))
                if pending is not None:
                    yield await pending
                pending = task
            if pending is not None:
                yield await pending
                pending = None
        finally:
            if pending is not None:      # generator aborted mid-flight
                pending.cancel()

    async def _tts_safe(self, sentence: str) -> bytes:
        """TTS with failure policy: skip single failures, abort after 2 in a row."""
        logger.info("[%s] TTS: %s", self.session_id, sentence[:50])
        try:
            pcm = await self._tts.synthesize(sentence)
            self._tts_fail_streak = 0
            return pcm
        except Exception as e:
            self._tts_fail_streak += 1
            logger.error("[%s] TTS failed (%d consecutive): %s",
                         self.session_id, self._tts_fail_streak, e)
            if self._tts_fail_streak >= 2:
                raise _TTSAbort() from e
            return b""                   # single failure → skip this sentence

    async def _send_audio_chunks(self, pcm_data: bytes, start_seq: int) -> int:
        """Send PCM as paced packets. Returns next sequence number.

        Pacing: allow DOWN_BURST_SECONDS of audio to go out immediately
        (pre-fills the MCU ring buffer), then hold to real-time rate so the
        32 KB (1 s) ring never overflows regardless of answer length.
        """
        loop = asyncio.get_running_loop()
        if self._pace_t0 is None:
            self._pace_t0 = loop.time()
            self._pace_sent = 0

        burst_bytes = int(config.DOWN_BURST_SECONDS * config.DEVICE_BYTES_PER_SEC)
        seq = start_seq
        for chunk in chunk_pcm(pcm_data):
            # Time at which this chunk is allowed to leave (token bucket)
            ahead = self._pace_sent - burst_bytes
            if ahead > 0:
                target = self._pace_t0 + ahead / config.DEVICE_BYTES_PER_SEC
                delay = target - loop.time()
                if delay > 0:
                    await asyncio.sleep(delay)

            packet = encode_packet(self.session_id, seq, chunk)
            await self._publish(config.TOPIC_DOWN_AUDIO, packet, 0)  # QoS 0
            self._pace_sent += len(chunk)
            seq += 1
        return seq

    # ── Control messages ────────────────────────────────────────────────────

    async def _send_done(self) -> None:
        msg = json.dumps({"event": "done", "session": self.session_id})
        await self._publish(config.TOPIC_DOWN_CONTROL, msg.encode(), 1)  # QoS 1
        self.state = State.DONE
        logger.info("[%s] Session done", self.session_id)

    async def _send_error(self, reason: str) -> None:
        msg = json.dumps({"event": "error", "session": self.session_id, "reason": reason})
        await self._publish(config.TOPIC_DOWN_CONTROL, msg.encode(), 1)  # QoS 1
        self.state = State.ERROR
        logger.warning("[%s] Session error: %s", self.session_id, reason)

    def cleanup(self) -> None:
        """Release resources."""
        self._chunks.clear()
        self._total_bytes = 0
        if self._task and not self._task.done():
            self._task.cancel()
        logger.debug("[%s] Session cleaned up", self.session_id)


class _TTSAbort(Exception):
    """Two consecutive TTS failures — abort the session."""
