"""Session — manages a single voice interaction lifecycle.

State machine:
    IDLE → RECORDING → PROCESSING → SENDING → DONE / ERROR

Latency/robustness features:
    - Filler clip: a pre-synthesized "让我查一下" clip starts playing the moment
      stop arrives (<1s perceived response); the real answer queues behind it.
    - Streaming ASR: uplink chunks are fed to iFlytek live during recording;
      stop → final text in ~0.3s (batch re-recognition only as fallback).
    - Streaming TTS: sentence audio is forwarded to the MCU as the gateway
      synthesizes it (first chunk ~1.5s) instead of waiting for the full clip.
    - TTS pipelining: sentence N+1 synthesizes while sentence N streams out.
    - Downlink pacing: capped token bucket, so the MCU's 2-second ring buffer
      (64 KB) never overflows even after a network stall.
"""

import asyncio
import enum
import json
import logging
import time
from collections.abc import AsyncGenerator, Awaitable, Callable

import config
from audio import PCM_CHUNK, chunk_pcm, encode_packet
from llm import LLMClient
from tts import TTSEngine

logger = logging.getLogger("session")

# LLM 连续两次空流时的兜底播报（走非流式 TTS，一次合成整句）
_LLM_EMPTY_FALLBACK = "抱歉，我刚才没有组织好语言，请再问我一次吧。"


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
        filler_pcm: bytes = b"",
    ):
        self.session_id = session_id
        self.state = State.IDLE
        self._publish = publish_fn
        self._asr = asr
        self._llm = llm
        self._tts = tts

        self._chunks: dict[int, bytes] = {}  # seq → pcm bytes (batch-ASR fallback)
        self._total_bytes = 0
        self._max_duration = max_duration
        self._created_at = time.monotonic()
        self._task: asyncio.Task | None = None
        self._asr_stream = None              # live-feed ASR session (or None)
        self._send_buf = bytearray()         # accumulates stream PCM into full packets

        # Filler clip: sent immediately on stop while ASR→LLM→TTS runs.
        # The answer's packets must queue strictly *behind* it (the MCU plays
        # in arrival order), so _send_audio_chunks awaits the filler task.
        self._filler_pcm = filler_pcm
        self._filler_task: asyncio.Task | None = None
        self._down_seq = 0                   # continuous across filler + answer

        # Downlink pacing state (token bucket over wall clock)
        self._pace_t0: float | None = None
        self._pace_sent = 0          # bytes already sent
        self._pace_tokens = 0.0      # token bucket credit (bytes), capped at burst
        self._pace_last = 0.0        # last refill timestamp
        self._tts_fail_streak = 0    # consecutive TTS failures

    @property
    def age(self) -> float:
        """Seconds since session creation."""
        return time.monotonic() - self._created_at

    def start(self) -> None:
        """Transition to RECORDING state and open the live ASR stream."""
        if self.state != State.IDLE:
            logger.warning("[%s] start() called in state %s", self.session_id, self.state)
            return
        self.state = State.RECORDING
        self._open_asr_stream()
        logger.info("[%s] Recording started (max_duration=%ds)",
                    self.session_id, self._max_duration)

    def _open_asr_stream(self) -> None:
        """Connect to iFlytek while the MCU is still recording (~3s of free time)."""
        if self._asr_stream is not None:
            return
        open_stream = getattr(self._asr, "open_stream", None)
        if open_stream is None:
            return                       # engine without streaming support
        try:
            self._asr_stream = open_stream()
            self._asr_stream.start()
        except Exception as e:
            logger.warning("[%s] ASR stream open failed (%s), will use batch ASR",
                           self.session_id, e)
            self._asr_stream = None

    def handle_audio_chunk(self, seq: int, pcm: bytes) -> bool:
        """Store one already-decoded uplink chunk and feed the live ASR stream.

        Returns True if accepted, False if dropped (wrong state / overflow).
        """
        if self.state not in (State.IDLE, State.RECORDING):
            logger.warning("[%s] Audio chunk in state %s, dropping",
                           self.session_id, self.state)
            return False

        # Auto-transition to RECORDING if audio arrives before start
        if self.state == State.IDLE:
            self.state = State.RECORDING
            self._open_asr_stream()

        if self._total_bytes + len(pcm) > config.MAX_PCM_SIZE:
            logger.warning("[%s] PCM size limit reached (%d bytes), dropping chunk",
                           self.session_id, self._total_bytes)
            return False

        self._chunks[seq] = pcm
        self._total_bytes += len(pcm)
        if self._asr_stream is not None:
            self._asr_stream.feed(pcm)   # non-blocking (internal queue)
        return True

    async def handle_stop(self) -> None:
        """Process stop signal — finalize ASR and run the AI pipeline."""
        if self.state != State.RECORDING:
            logger.warning("[%s] stop() in state %s, ignoring", self.session_id, self.state)
            return

        self.state = State.PROCESSING
        logger.info("[%s] Stop received, %d chunks (%d bytes)",
                    self.session_id, len(self._chunks), self._total_bytes)

        # 垫场语音：不等 ASR/LLM/TTS，立刻开播——用户 1 秒内就听到回应，
        # 真实回答由 _send_audio_chunks 的排队机制自然衔接在其后。
        if self._filler_pcm:
            self._filler_task = asyncio.create_task(self._send_filler())

        try:
            await asyncio.wait_for(
                self._run_pipeline(),
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

    async def _run_pipeline(self) -> None:
        """ASR (stream-first) → LLM stream → TTS stream → paced send.

        Graceful degradation per design doc: send whatever was produced,
        then report done / partial error.
        """
        # ── Step 1: ASR — live stream finalize, batch fallback ───────────
        text: str | None = None
        t0 = time.monotonic()
        stream, self._asr_stream = self._asr_stream, None
        if stream is not None:
            try:
                loop = asyncio.get_running_loop()
                # All audio queued before stop is already fed (in-order queue);
                # finish() sends the last frame and waits for the final text.
                text = await loop.run_in_executor(None, stream.finish)
                logger.info("[%s] ASR (stream, %.2fs): %s", self.session_id,
                            time.monotonic() - t0, text[:100])
            except Exception as e:
                logger.warning("[%s] ASR stream failed (%s), falling back to batch",
                               self.session_id, e)

        if text is None:
            # stop(QoS1) may have overtaken trailing audio(QoS0) — the batch
            # path still needs the old straggler grace before assembling.
            await asyncio.sleep(config.STOP_GRACE_SECONDS)
            pcm_data = self._assemble_pcm()
            if not pcm_data:
                await self._send_error("no_audio")
                return
            logger.info("[%s] Running batch ASR (%.2fs audio)...",
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

        # ── Step 2: LLM stream → streaming TTS → paced downlink ─────────
        self.state = State.SENDING
        llm_interrupted = False
        sent_any = False

        # 网关偶发返回 HTTP 200 但整条流无任何 content token——不报异常也不产
        # 音频，设备只听到垫场语音就收到 done。空流重试一次，仍空则播兜底句。
        for attempt in (1, 2):
            try:
                async for pcm_out in self._tts_pipeline(text):
                    if not pcm_out:
                        continue
                    # Accumulate stream chunks into full packets; the tail is
                    # flushed after the pipeline ends (MCU accepts short packets).
                    self._send_buf += pcm_out
                    n_full = len(self._send_buf) // PCM_CHUNK * PCM_CHUNK
                    if n_full:
                        await self._send_audio_chunks(bytes(self._send_buf[:n_full]))
                        del self._send_buf[:n_full]
                        sent_any = True
            except _TTSAbort:
                await self._send_error("tts_failed")
                return
            except Exception as e:
                llm_interrupted = True
                logger.error("[%s] LLM stream interrupted: %s", self.session_id, e)
            if sent_any or self._send_buf or llm_interrupted:
                break
            if attempt == 1:
                logger.warning("[%s] LLM produced no audio, retrying once",
                               self.session_id)

        if self._send_buf:
            await self._send_audio_chunks(bytes(self._send_buf))
            self._send_buf.clear()
            sent_any = True

        if not sent_any and not llm_interrupted:
            logger.warning("[%s] LLM empty twice, sending canned fallback",
                           self.session_id)
            try:
                pcm = await self._tts.synthesize(_LLM_EMPTY_FALLBACK)
                if pcm:
                    await self._send_audio_chunks(pcm)
                    sent_any = True
            except Exception as e:
                logger.error("[%s] Fallback TTS failed: %s", self.session_id, e)
            if not sent_any:
                await self._send_error("llm_failed")
                return

        # ── Step 3: completion signal ────────────────────────────────────
        if llm_interrupted and not sent_any:
            await self._send_error("llm_failed")
        elif llm_interrupted:
            await self._send_error("llm_stream_interrupted")
        else:
            await self._send_done()

    async def _tts_pipeline(self, text: str) -> AsyncGenerator[bytes, None]:
        """Yield PCM in sentence order; chunks flow as they are synthesized.

        Producer schedules one feeder task per LLM sentence (streaming TTS →
        per-sentence chunk queue). The outer queue maxsize bounds how many
        sentences synthesize ahead (gateway-friendly). The consumer drains
        sentence queues strictly in order — the first sentence's first chunk
        goes downstream the moment the gateway produces it (~1.5s), instead
        of after the whole clip (5-9s measured on this gateway).
        """
        outer: asyncio.Queue[tuple[asyncio.Queue, asyncio.Task] | None] = \
            asyncio.Queue(maxsize=2)
        feeders: list[asyncio.Task] = []

        async def synth_to_queue(sentence: str,
                                 chunks: asyncio.Queue) -> None:
            try:
                await self._tts_safe_stream(sentence, chunks)
            finally:
                await chunks.put(None)          # sentence end marker

        async def producer() -> None:
            try:
                async for sentence in self._llm.stream_sentences(text):
                    chunks: asyncio.Queue[bytes | None] = asyncio.Queue()
                    task = asyncio.create_task(synth_to_queue(sentence, chunks))
                    feeders.append(task)
                    await outer.put((chunks, task))   # maxsize 反压: 限制超前合成数
            finally:
                await outer.put(None)           # 结束标记（异常时也要放行消费端）

        prod = asyncio.create_task(producer())
        try:
            while True:
                entry = await outer.get()
                if entry is None:
                    break
                chunks, task = entry
                while True:
                    c = await chunks.get()
                    if c is None:
                        break
                    yield c
                await task                      # 句级失败策略在此浮出（_TTSAbort）
            await prod                          # LLM 流异常在此浮出（交给上层归类）
        finally:
            prod.cancel()                       # 生成器被中止时清理生产者
            for t in feeders:
                if not t.done():
                    t.cancel()

    async def _tts_safe_stream(self, sentence: str, chunks: asyncio.Queue) -> None:
        """Stream one sentence's TTS into `chunks`, with the failure policy:
        stream error before any audio → one non-streaming retry;
        skip the sentence on failure; abort after 2 consecutive failures.
        """
        logger.info("[%s] TTS: %s", self.session_id, sentence[:50])
        got_audio = False
        try:
            if config.TTS_STREAM:
                try:
                    async for c in self._tts.synthesize_stream(sentence):
                        got_audio = True
                        await chunks.put(c)
                except Exception as e:
                    if got_audio:
                        raise                # mid-stream loss: keep sent part, count failure
                    logger.warning("[%s] TTS stream failed pre-audio (%s), "
                                   "retrying non-streaming", self.session_id, e)
                    pcm = await self._tts.synthesize(sentence)
                    if pcm:
                        got_audio = True
                        await chunks.put(pcm)
            else:
                pcm = await self._tts.synthesize(sentence)
                if pcm:
                    got_audio = True
                    await chunks.put(pcm)
            self._tts_fail_streak = 0
        except Exception as e:
            self._tts_fail_streak += 1
            logger.error("[%s] TTS failed (%d consecutive): %s",
                         self.session_id, self._tts_fail_streak, e)
            if self._tts_fail_streak >= 2:
                raise _TTSAbort() from e
            # single failure → skip this sentence

    # ── Filler clip ──────────────────────────────────────────────────────────

    async def _send_filler(self) -> None:
        """Send the pre-synthesized filler clip through the normal paced path."""
        logger.info("[%s] Sending filler clip (%.1fs)", self.session_id,
                    len(self._filler_pcm) / config.DEVICE_BYTES_PER_SEC)
        await self._send_audio_chunks(self._filler_pcm)

    async def _wait_filler(self) -> None:
        """Block until the filler clip is fully sent (no-op when none/self)."""
        task = self._filler_task
        if task is None or task is asyncio.current_task():
            return
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.cancelled():             # filler cancelled, not us — continue
                pass
            else:
                raise
        except Exception as e:
            logger.warning("[%s] Filler send failed: %s", self.session_id, e)
        finally:
            if task.done():
                self._filler_task = None

    def _cancel_filler(self) -> None:
        if self._filler_task is not None and not self._filler_task.done():
            self._filler_task.cancel()
        self._filler_task = None

    async def _send_audio_chunks(self, pcm_data: bytes) -> None:
        """Send PCM as paced packets, continuing the session sequence number.

        Ordering: the MCU plays packets in arrival order, so the answer must
        wait until the filler clip has been fully published — every caller
        except the filler task itself blocks on _wait_filler() first.

        Pacing: token bucket with **capped** capacity (= DOWN_BURST_SECONDS).
        Unlike an open-ended "burst + elapsed×rate" budget, the bucket cannot
        accumulate credit while the network stalls — so after a transit hiccup
        packets resume at real-time rate instead of flooding the MCU ring
        (observed: 4 s stall → catch-up burst → ring overflow, 0.5 s dropped).
        """
        await self._wait_filler()

        loop = asyncio.get_running_loop()
        burst_bytes = config.DOWN_BURST_SECONDS * config.DEVICE_BYTES_PER_SEC
        if self._pace_t0 is None:                 # first send: full bucket
            self._pace_t0 = loop.time()
            self._pace_tokens = burst_bytes
            self._pace_last = self._pace_t0

        for chunk in chunk_pcm(pcm_data):
            # Refill tokens at real-time rate, capped at bucket size
            now = loop.time()
            self._pace_tokens = min(
                burst_bytes,
                self._pace_tokens + (now - self._pace_last) * config.DEVICE_BYTES_PER_SEC,
            )
            self._pace_last = now
            deficit = len(chunk) - self._pace_tokens
            if deficit > 0:                       # not enough credit → wait it out
                await asyncio.sleep(deficit / config.DEVICE_BYTES_PER_SEC)
                self._pace_tokens = len(chunk)    # after sleep, exactly enough
                self._pace_last = loop.time()
            self._pace_tokens -= len(chunk)

            packet = encode_packet(self.session_id, self._down_seq, chunk)
            await self._publish(config.TOPIC_DOWN_AUDIO, packet, 0)  # QoS 0
            self._pace_sent += len(chunk)
            self._down_seq += 1

    # ── Control messages ────────────────────────────────────────────────────

    async def _send_done(self) -> None:
        await self._wait_filler()        # done 不能越过仍在下发的垫场音频
        msg = json.dumps({"event": "done", "session": self.session_id})
        await self._publish(config.TOPIC_DOWN_CONTROL, msg.encode(), 1)  # QoS 1
        self.state = State.DONE
        logger.info("[%s] Session done", self.session_id)

    async def _send_error(self, reason: str) -> None:
        self._cancel_filler()            # 出错即中止垫场，别再耗带宽
        msg = json.dumps({"event": "error", "session": self.session_id, "reason": reason})
        await self._publish(config.TOPIC_DOWN_CONTROL, msg.encode(), 1)  # QoS 1
        self.state = State.ERROR
        logger.warning("[%s] Session error: %s", self.session_id, reason)

    def cleanup(self) -> None:
        """Release resources."""
        if self._asr_stream is not None:
            self._asr_stream.abort()
            self._asr_stream = None
        self._cancel_filler()
        self._chunks.clear()
        self._total_bytes = 0
        if self._task and not self._task.done():
            self._task.cancel()
        logger.debug("[%s] Session cleaned up", self.session_id)


class _TTSAbort(Exception):
    """Two consecutive TTS failures — abort the session."""
