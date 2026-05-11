"""LLM client — OpenAI-compatible streaming with sentence segmentation."""

import asyncio
import logging
import re
from typing import AsyncGenerator, List

from openai import OpenAI

import config

logger = logging.getLogger("llm")

# Sentence-ending punctuation (Chinese + English)
_SENTENCE_END = re.compile(r"[。！？!?\.\n]")

# Fallback max characters per sentence if no punctuation found
_MAX_SENTENCE_CHARS = 200


class LLMClient:
    """Streaming LLM client with sentence-level segmentation.

    Uses synchronous openai library in a thread pool executor
    to avoid blocking the asyncio event loop.
    """

    def __init__(self):
        self._client = OpenAI(
            api_key=config.LLM_API_KEY,
            base_url=config.LLM_BASE_URL,
            timeout=config.API_TIMEOUT,
        )
        self._model = config.LLM_MODEL
        self._system_prompt = config.LLM_SYSTEM_PROMPT

    async def stream_sentences(self, user_text: str) -> AsyncGenerator[str, None]:
        """Stream LLM response, yielding complete sentences one at a time.

        Accumulates tokens, splits on sentence-ending punctuation.
        Yields sentences as they are completed.
        """
        messages = [
            {"role": "system", "content": self._system_prompt},
            {"role": "user", "content": user_text},
        ]

        loop = asyncio.get_event_loop()
        buffer = ""

        try:
            # Run synchronous streaming in thread pool
            stream = await loop.run_in_executor(
                None, self._create_stream, messages
            )

            for chunk in stream:
                if not chunk.choices:
                    continue
                delta = chunk.choices[0].delta
                token = delta.content or ""
                if not token:
                    continue

                buffer += token

                # Yield complete sentences
                while True:
                    match = _SENTENCE_END.search(buffer)
                    if match:
                        end = match.end()
                        sentence = buffer[:end].strip()
                        buffer = buffer[end:]
                        if sentence:
                            logger.debug("LLM sentence: %s", sentence[:60])
                            yield sentence
                    else:
                        break

                    # If no punctuation found and buffer is very long,
                    # yield a chunk to avoid unbounded accumulation
                    if len(buffer) > _MAX_SENTENCE_CHARS * 2:
                        # Try to split at a reasonable point
                        cut = _find_soft_break(buffer, _MAX_SENTENCE_CHARS)
                        if cut > 0:
                            sentence = buffer[:cut].strip()
                            buffer = buffer[cut:]
                            if sentence:
                                logger.debug("LLM soft-break sentence: %s", sentence[:60])
                                yield sentence

            # Flush remaining buffer
            if buffer.strip():
                logger.debug("LLM flush: %s", buffer.strip()[:60])
                yield buffer.strip()

        except Exception as e:
            logger.error("LLM stream error: %s", e)
            # Yield whatever we have so far (graceful degradation)
            if buffer.strip():
                yield buffer.strip()
            raise

    def _create_stream(self, messages: List[dict]):
        """Create synchronous streaming completion (runs in thread)."""
        return self._client.chat.completions.create(
            model=self._model,
            messages=messages,
            stream=True,
        )


def _find_soft_break(text: str, target: int) -> int:
    """Find a reasonable break point near `target` position.

    Looks for whitespace, comma, or Chinese comma near the target.
    """
    search_range = min(50, target // 4)
    start = max(0, target - search_range)
    end = min(len(text), target + search_range)

    for i in range(end - 1, start - 1, -1):
        if text[i] in " \t,，、；;":
            return i + 1

    # No good break found — hard cut at target
    return target if target < len(text) else 0
