import asyncio
import logging
import sys
import os
from dotenv import load_dotenv

# Set environment variables from .env before importing config
load_dotenv()

import config
from asr import create_asr_engine
from llm import LLMClient
from tts import TTSEngine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("api_test")

INPUT_PCM = "test_input.pcm"
OUTPUT_PCM = "test_output.pcm"          # merged full result
SEGMENT_FMT = "test_output_{:02d}.pcm"  # per-sentence segments

# Pace TTS calls to respect upstream rate limits (gateway returns 503 under load)
TTS_PACING_SEC = 1.5
# Per Lele.md §8: abort after this many consecutive TTS segment failures
MAX_CONSECUTIVE_TTS_FAILS = 2


async def run_asr() -> str:
    """ASR: test_input.pcm (16kHz/16bit/mono) -> text."""
    logger.info("[ASR] recognizing %s ...", INPUT_PCM)
    if not os.path.exists(INPUT_PCM):
        logger.error("[ASR] input file %s not found", INPUT_PCM)
        return ""

    with open(INPUT_PCM, "rb") as f:
        pcm_data = f.read()
    logger.info("[ASR] loaded %d bytes (~%.1fs)", len(pcm_data), len(pcm_data) / 2 / 16000)

    engine = create_asr_engine()
    try:
        text = await engine.recognize(pcm_data)
        if text:
            logger.info("[ASR] result: '%s'", text)
        else:
            logger.warning("[ASR] empty result")
        return text
    except Exception as e:
        logger.error("[ASR] error: %s", e)
        return ""


async def run_llm(user_text: str) -> list:
    """LLM: text -> list of streamed sentences."""
    if not user_text:
        logger.warning("[LLM] skipped (no ASR text)")
        return []
    logger.info("[LLM] prompt: '%s'", user_text)
    client = LLMClient()
    sentences = []
    try:
        async for sentence in client.stream_sentences(user_text):
            logger.info("[LLM] sentence %d: %s", len(sentences), sentence)
            sentences.append(sentence)
    except Exception as e:
        logger.error("[LLM] stream error (keeping %d sentences): %s", len(sentences), e)
    return sentences


async def run_tts(sentences: list):
    """TTS: synthesize each sentence to its own segment file, then merge.

    Mirrors Lele.md streaming-TTS design: one segment per sentence.
    Paces calls and tolerates per-segment failures (skip; abort after 2 in a row).
    """
    if not sentences:
        logger.warning("[TTS] skipped (no sentences)")
        return

    engine = TTSEngine()
    merged = bytearray()
    saved_segments = []
    consecutive_fails = 0

    for i, sentence in enumerate(sentences):
        if i > 0:
            await asyncio.sleep(TTS_PACING_SEC)  # respect API rate limit

        logger.info("[TTS] segment %d: %s", i, sentence[:40])
        try:
            pcm = await engine.synthesize(sentence)
            if not pcm:
                raise RuntimeError("empty PCM")
            seg_path = SEGMENT_FMT.format(i)
            with open(seg_path, "wb") as f:
                f.write(pcm)
            merged.extend(pcm)
            saved_segments.append(seg_path)
            consecutive_fails = 0
            logger.info("[TTS] segment %d OK -> %s (%d bytes, ~%.1fs)",
                        i, seg_path, len(pcm), len(pcm) / 2 / config.DEVICE_SAMPLE_RATE)
        except Exception as e:
            consecutive_fails += 1
            logger.error("[TTS] segment %d FAILED (%d in a row): %s",
                         i, consecutive_fails, e)
            if consecutive_fails >= MAX_CONSECUTIVE_TTS_FAILS:
                logger.error("[TTS] aborting after %d consecutive failures (Lele.md §8)",
                             consecutive_fails)
                break

    if merged:
        with open(OUTPUT_PCM, "wb") as f:
            f.write(merged)
        logger.info("[TTS] merged %d/%d segments -> %s (%d bytes, ~%.1fs)",
                    len(saved_segments), len(sentences), OUTPUT_PCM,
                    len(merged), len(merged) / 2 / config.DEVICE_SAMPLE_RATE)
    else:
        logger.error("[TTS] no audio produced; %s not written", OUTPUT_PCM)


async def main():
    # End-to-end pipeline: ASR -> LLM -> per-sentence TTS (mirrors Lele.md data flow)
    user_text = await run_asr()
    sentences = await run_llm(user_text)
    await run_tts(sentences)


if __name__ == "__main__":
    if not os.path.exists(".env"):
        logger.error(".env file not found!")
        sys.exit(1)
    asyncio.run(main())
