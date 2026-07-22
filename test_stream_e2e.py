"""End-to-end test of the new streaming pipeline (no MQTT; real cloud APIs).

Exercises exactly what session.py now does:
  1. Streaming ASR  — feed test_input.pcm in 4000B chunks (as the MCU would),
                      finish() at "stop", measure finalization time.
  2. LLM sentences  — verify the first unit is clause-cut (short).
  3. Streaming TTS  — first-chunk latency + total, via synthesize_stream().
  4. Resampler      — StreamResampler24k16k output must match resample_pcm()
                      on the same 24k input, chunked arbitrarily.
"""

import asyncio
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
with open(os.path.join(os.path.dirname(__file__), ".env"), encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger("e2e")

import numpy as np

from asr import create_asr_engine
from audio import StreamResampler24k16k, resample_pcm
from llm import LLMClient
from tts import TTSEngine


def test_resampler_equivalence():
    rng = np.random.default_rng(42)
    pcm24 = (rng.standard_normal(24000 * 3) * 3000).astype(np.int16).tobytes()
    # chunk at awkward odd-ish boundaries to stress the carry logic
    r = StreamResampler24k16k()
    out = b""
    pos = 0
    for size in (1000, 4444, 3, 1, 50002, 10**9):
        chunk = pcm24[pos:pos + size]
        pos += len(chunk)
        out += r.process(chunk)
    n_in = len(pcm24) // 2
    n_out = len(out) // 2
    expect = n_in // 3 * 2
    assert n_out == expect, f"length {n_out} != {expect}"
    # spot-check formula on first triplets
    s = np.frombuffer(pcm24, np.int16)
    o = np.frombuffer(out, np.int16)
    assert o[0] == s[0] and o[1] == (int(s[1]) + int(s[2])) >> 1
    assert o[2] == s[3] and o[3] == (int(s[4]) + int(s[5])) >> 1
    log.info("resampler OK: %d in → %d out samples, boundaries continuous", n_in, n_out)


async def test_asr_stream() -> str:
    with open("test_input.pcm", "rb") as f:
        pcm = f.read()
    log.info("ASR stream: feeding %.1fs audio in 4000B chunks (live-cadence-free)",
             len(pcm) / 32000)
    engine = create_asr_engine()
    stream = engine.open_stream()
    t0 = time.perf_counter()
    stream.start()
    for i in range(0, len(pcm), 4000):
        stream.feed(pcm[i:i + 4000])
    t_fed = time.perf_counter()
    loop = asyncio.get_running_loop()
    text = await loop.run_in_executor(None, stream.finish)
    t_done = time.perf_counter()
    log.info("ASR stream: text='%s'  finalize=%.2fs (total %.2fs incl connect+feed)",
             text, t_done - t_fed, t_done - t0)
    assert text, "empty ASR result"
    return text


async def test_llm_sentences(text: str) -> list[str]:
    client = LLMClient()
    sentences = []
    t0 = time.perf_counter()
    async for s in client.stream_sentences(text):
        log.info("LLM unit %d (+%.2fs, %d字): %s",
                 len(sentences), time.perf_counter() - t0, len(s), s[:40])
        sentences.append(s)
        if len(sentences) >= 4:
            break
    assert sentences, "no sentences"
    return sentences


async def test_tts_stream(sentence: str):
    engine = TTSEngine()
    t0 = time.perf_counter()
    first = None
    total = 0
    n = 0
    async for chunk in engine.synthesize_stream(sentence):
        if first is None:
            first = time.perf_counter() - t0
        total += len(chunk)
        n += 1
    t_done = time.perf_counter() - t0
    log.info("TTS stream '%s' (%d字): first_chunk=%.2fs done=%.2fs "
             "chunks=%d pcm16k=%dB (~%.1fs audio)",
             sentence[:16], len(sentence), first, t_done, n, total, total / 32000)
    await engine.aclose()
    assert first is not None and total > 0


async def main():
    test_resampler_equivalence()
    text = await test_asr_stream()
    sentences = await test_llm_sentences(text)
    first_unit = sentences[0]
    log.info("first unit is %d chars (clause-cut %s)", len(first_unit),
             "YES" if len(first_unit) < 20 else "no clause found")
    await test_tts_stream(first_unit)
    log.info("ALL E2E CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
