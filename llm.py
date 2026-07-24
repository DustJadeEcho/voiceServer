"""LLM client — OpenAI-compatible streaming with sentence segmentation."""

import asyncio
import logging
import re
from collections.abc import AsyncGenerator, Callable

from openai import OpenAI

import config

logger = logging.getLogger("llm")

# Sentence-ending punctuation (Chinese + English)
_SENTENCE_END = re.compile(r"[。！？!?\.\n]")

# First TTS unit only: also cut at clause marks so synthesis starts ASAP.
# TTS setup costs ~1.5s per request regardless of length, so only the FIRST
# unit is worth shrinking; later units stay full sentences for prosody.
_FIRST_CLAUSE_END = re.compile(r"[，,、；;：:。！？!?\.\n]")
_MIN_FIRST_CLAUSE = 6           # 太短的碎片不值得单独起一次 TTS 请求

# Fallback max characters per sentence if no punctuation found
_MAX_SENTENCE_CHARS = 200


class LLMClient:
    """Streaming LLM client with sentence-level segmentation.

    Uses synchronous openai library in a thread pool executor
    to avoid blocking the asyncio event loop.
    """

    def __init__(self, context_provider: Callable[[], str] | None = None):
        self._client = OpenAI(
            api_key=config.LLM_API_KEY,
            base_url=config.LLM_BASE_URL,
            timeout=config.API_TIMEOUT,
        )
        self._model = config.LLM_MODEL
        self._system_prompt = config.LLM_SYSTEM_PROMPT
        # 每次请求时调用，返回追加到 system prompt 的实时上下文（船载传感器数据）
        self._context_provider = context_provider

    async def stream_sentences(self, user_text: str) -> AsyncGenerator[str, None]:
        """Stream LLM response, yielding complete sentences one at a time.

        The OpenAI SDK stream is a *synchronous* iterator; iterating it directly
        in a coroutine would block the whole event loop between tokens. Every
        next() is therefore pushed to the default thread pool.
        """
        system_prompt = self._system_prompt
        if self._context_provider is not None:
            try:
                context = self._context_provider()
            except Exception as e:
                logger.warning("Context provider failed, continuing without: %s", e)
                context = ""
            if context:
                system_prompt = f"{system_prompt}\n\n{context}"

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_text},
        ]

        loop = asyncio.get_running_loop()
        buffer = ""
        t_start = loop.time()
        first_token_at: float | None = None
        yielded_any = False

        try:
            stream = await loop.run_in_executor(None, self._create_stream, messages)
            stream_iter = iter(stream)

            while True:
                chunk = await loop.run_in_executor(None, _next_or_none, stream_iter)
                if chunk is None:
                    break
                if not chunk.choices:
                    continue
                token = chunk.choices[0].delta.content or ""
                if not token:
                    continue
                if first_token_at is None:
                    first_token_at = loop.time()
                    # TTFT 是整条链路延时的最大变量（上游网关/模型决定），
                    # 常态化打印便于横向比较不同 LLM_BASE_URL/LLM_MODEL
                    logger.info("LLM first token in %.2fs", first_token_at - t_start)

                buffer += token

                # Yield complete sentences (first unit: clause-level early cut)
                while True:
                    if yielded_any:
                        match = _SENTENCE_END.search(buffer)
                        cut_at = match.end() if match else 0
                    else:
                        cut_at = 0
                        for m in _FIRST_CLAUSE_END.finditer(buffer):
                            if m.end() >= _MIN_FIRST_CLAUSE:
                                cut_at = m.end()
                                break
                    if not cut_at:
                        break
                    sentence = buffer[:cut_at].strip()
                    buffer = buffer[cut_at:]
                    if sentence:
                        logger.debug("LLM sentence: %s", sentence[:60])
                        yielded_any = True
                        yield sentence

                # If no punctuation and the buffer grows unbounded, soft-break it
                if len(buffer) > _MAX_SENTENCE_CHARS * 2:
                    cut = _find_soft_break(buffer, _MAX_SENTENCE_CHARS)
                    if cut > 0:
                        sentence = buffer[:cut].strip()
                        buffer = buffer[cut:]
                        if sentence:
                            logger.debug("LLM soft-break sentence: %s", sentence[:60])
                            yielded_any = True
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

    def _create_stream(self, messages: list[dict]):
        """Create synchronous streaming completion (runs in thread)."""
        return self._client.chat.completions.create(
            model=self._model,
            messages=messages,
            stream=True,
            max_tokens=config.LLM_MAX_TOKENS,   # 语音回答无需长文，截断可显著压 TTFT/总时长
        )


def _next_or_none(it):
    """next() wrapper for run_in_executor (StopIteration → None sentinel)."""
    try:
        return next(it)
    except StopIteration:
        return None


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
