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

async def test_llm():
    logger.info("Testing LLM API...")
    client = LLMClient()
    user_text = "你好，请问你是谁？"
    full_response = ""
    try:
        async for sentence in client.stream_sentences(user_text):
            logger.info(f"LLM Sentence: {sentence}")
            full_response += sentence
        if full_response:
            logger.info("LLM API test PASSED")
            return full_response
        else:
            logger.error("LLM API test FAILED: Empty response")
            return None
    except Exception as e:
        logger.error(f"LLM API test ERROR: {e}")
        return None

async def test_tts(text):
    if not text:
        logger.warning("Skipping TTS test (no text)")
        return
    logger.info("Testing TTS API...")
    engine = TTSEngine()
    try:
        pcm_data = await engine.synthesize(text)
        if pcm_data and len(pcm_data) > 0:
            logger.info(f"TTS API test PASSED (received {len(pcm_data)} bytes)")
            # Save a sample to check
            with open("test_tts_output.pcm", "wb") as f:
                f.write(pcm_data)
            logger.info("Saved TTS output to test_tts_output.pcm")
        else:
            logger.error("TTS API test FAILED: Empty PCM data")
    except Exception as e:
        logger.error(f"TTS API test ERROR: {e}")

async def test_asr():
    logger.info("Testing ASR API (using dummy PCM if no file provided)...")
    # Xunfei ASR requires real voice PCM. If we don't have one, this might fail or return empty.
    # But we can at least test the connection and auth.
    engine = create_asr_engine()

    # Try to read an existing PCM if available, else use 1s of silence
    pcm_path = "test_audio_16k.pcm"
    if os.path.exists(pcm_path):
        with open(pcm_path, "rb") as f:
            pcm_data = f.read()
    else:
        logger.info("No test_audio_16k.pcm found, using 1s of silence (16kHz/16bit/mono)")
        pcm_data = b'\x00\x00' * 16000

    try:
        text = await engine.recognize(pcm_data)
        logger.info(f"ASR Result: '{text}'")
        logger.info("ASR API test COMPLETED (result depends on audio quality)")
    except Exception as e:
        logger.error(f"ASR API test ERROR: {e}")

async def main():
    # 1. Test LLM
    response_text = await test_llm()

    # 2. Test TTS using LLM's response
    await test_tts(response_text)

    # 3. Test ASR
    await test_asr()

if __name__ == "__main__":
    if not os.path.exists(".env"):
        logger.error(".env file not found!")
        sys.exit(1)
    asyncio.run(main())
