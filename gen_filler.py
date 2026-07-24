"""Generate the filler clip (filler.pcm) with the same TTS voice as answers.

Run once (and after changing text/voice), locally or on the server:
    python gen_filler.py                 # default text below
    python gen_filler.py 自定义的垫场文案   # custom text

Output:
    filler.pcm — 16 kHz / 16-bit / mono raw PCM, loaded by server.py at startup
    filler.wav — same audio with a WAV header, for auditioning on a PC

字数即时长（约 4 字/秒）：文案越长，好情况下真实回答被垫场拖得越晚——
听到回答的时刻 = max(垫场时长, 流水线延迟)。默认文案 ~5s，刚好盖住常态
TTFT；坏情况（TTFT 抖到 10s）垫场结束后仍会有一段静默，属正常。
"""

import asyncio
import sys

import config
from audio import pcm_to_wav
from tts import TTSEngine

DEFAULT_TEXT = "好的，我先来查询一下现在的水质数据和定位信息，请您稍等几秒钟。"


async def main() -> None:
    text = " ".join(sys.argv[1:]).strip() or DEFAULT_TEXT
    print(f"Voice : {config.TTS_VOICE} @ {config.TTS_MODEL}")
    print(f"Text  : {text}")

    engine = TTSEngine()
    try:
        pcm = await engine.synthesize(text)   # already resampled to 16 kHz
    finally:
        await engine.aclose()

    if not pcm:
        print("TTS returned no audio", file=sys.stderr)
        sys.exit(1)

    with open(config.FILLER_PCM_PATH, "wb") as f:
        f.write(pcm)
    wav_path = config.FILLER_PCM_PATH.rsplit(".", 1)[0] + ".wav"
    with open(wav_path, "wb") as f:
        f.write(pcm_to_wav(pcm, config.DEVICE_SAMPLE_RATE))

    duration = len(pcm) / config.DEVICE_BYTES_PER_SEC
    print(f"Wrote {config.FILLER_PCM_PATH} ({len(pcm)} bytes, {duration:.1f}s)")
    print(f"Wrote {wav_path} (for listening)")


if __name__ == "__main__":
    asyncio.run(main())
