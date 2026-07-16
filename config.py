"""Voice server configuration — all values from environment variables."""

import os
import sys

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ─── MQTT ────────────────────────────────────────────────────────────────────
MQTT_HOST = os.environ.get("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))    # plain 1883 (TLS 8883 later)
MQTT_USERNAME = os.environ.get("MQTT_USERNAME", "")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD", "")
MQTT_CA_CERT = os.environ.get("MQTT_CA_CERT", "")  # set to enable TLS

# Topics — must match MCU mqtt_connect.h
TOPIC_UP_AUDIO = "/qhmu/lele/mcu/audio/pcm/up"
TOPIC_UP_CONTROL = "/qhmu/lele/mcu/audio/wm8978/control"
TOPIC_DOWN_AUDIO = "/qhmu/lele/mcu/audio/pcm/down"
TOPIC_DOWN_CONTROL = "/qhmu/lele/mcu/audio/ubuntu/control"

# ─── ASR (iFlytek 语音听写 流式版) ───────────────────────────────────────────
XUNFEI_APPID = os.environ.get("XUNFEI_APPID", "")
XUNFEI_API_KEY = os.environ.get("XUNFEI_API_KEY", "")
XUNFEI_API_SECRET = os.environ.get("XUNFEI_API_SECRET", "")

# ─── LLM (OpenAI-compatible) ─────────────────────────────────────────────────
LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "https://api.openai.com/v1")
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-4o-mini")
LLM_SYSTEM_PROMPT = os.environ.get(
    "LLM_SYSTEM_PROMPT",
    "你是一个友好的语音助手，请用简洁的中文回答用户的问题。回答要口语化，适合语音播放，"
    "避免使用 Markdown 格式、代码块或特殊符号。每次回答控制在 200 字以内。",
)

# ─── TTS (MiMo-V2.5-TTS) ────────────────────────────────────────────────────
TTS_BASE_URL = os.environ.get("TTS_BASE_URL", "https://api.xiaomimimo.com/v1")
TTS_API_KEY = os.environ.get("TTS_API_KEY", "")
TTS_MODEL = os.environ.get("TTS_MODEL", "mimo-v2.5-tts")
TTS_VOICE = os.environ.get("TTS_VOICE", "冰糖")
TTS_STYLE = os.environ.get("TTS_STYLE", "温柔亲切，语速适中，像在和朋友聊天一样自然。")
TTS_NATIVE_RATE = 24000  # Mimo TTS outputs 24kHz PCM16LE mono

# ─── Audio / flow control ────────────────────────────────────────────────────
DEVICE_SAMPLE_RATE = 16000      # device format: 16 kHz / 16-bit / mono
DEVICE_BYTES_PER_SEC = DEVICE_SAMPLE_RATE * 2
# MCU TTS ring buffer is 32 KB = 1.0 s. Pace the downlink so it never overflows:
# an initial burst pre-fills the buffer, then send at real-time rate.
DOWN_BURST_SECONDS = float(os.environ.get("DOWN_BURST_SECONDS", "0.6"))
# stop(QoS1) may overtake the last audio chunks (QoS0) — wait briefly for stragglers
STOP_GRACE_SECONDS = float(os.environ.get("STOP_GRACE_SECONDS", "0.3"))

# ─── Limits ──────────────────────────────────────────────────────────────────
MAX_SESSIONS = int(os.environ.get("MAX_SESSIONS", "5"))
SESSION_TIMEOUT = int(os.environ.get("SESSION_TIMEOUT", "90"))
MAX_PCM_SIZE = int(os.environ.get("MAX_PCM_SIZE", str(2 * 1024 * 1024)))  # 2MB
API_TIMEOUT = int(os.environ.get("API_TIMEOUT", "10"))

# ─── Logging ─────────────────────────────────────────────────────────────────
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

# ─── Validation ──────────────────────────────────────────────────────────────
_REQUIRED = {
    "MQTT_HOST": MQTT_HOST,
    "XUNFEI_APPID": XUNFEI_APPID,
    "LLM_API_KEY": LLM_API_KEY,
    "TTS_API_KEY": TTS_API_KEY,
}

_missing = [k for k, v in _REQUIRED.items() if not v]
if _missing:
    print(f"[config] Missing required env vars: {', '.join(_missing)}", file=sys.stderr)
    print("See README for reference.", file=sys.stderr)
    sys.exit(1)
