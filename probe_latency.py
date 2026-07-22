"""One-shot latency probe against the LLM/TTS gateway (run from dev machine).

Measures, with real API calls:
  1. GET /models        — what models the gateway offers
  2. LLM TTFT           — current model vs fast candidates (tiny max_tokens)
  3. TTS latency        — short clause vs full sentence (non-streaming)
  4. TTS stream=True    — does the gateway stream audio deltas?

Absolute numbers differ from the Aliyun host (different network path),
but relative comparisons hold. Cost: a few cent-level calls.
"""

import base64
import os
import time

import httpx

# minimal .env loader (dev machine may lack python-dotenv)
with open(os.path.join(os.path.dirname(__file__), ".env"), encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

LLM_BASE = os.environ["LLM_BASE_URL"].rstrip("/")
LLM_KEY = os.environ["LLM_API_KEY"]
TTS_BASE = os.environ["TTS_BASE_URL"].rstrip("/")
TTS_KEY = os.environ["TTS_API_KEY"]
TTS_MODEL = os.environ.get("TTS_MODEL", "mimo-v2.5-tts")
TTS_VOICE = os.environ.get("TTS_VOICE", "冰糖")

HDR_LLM = {"Authorization": f"Bearer {LLM_KEY}"}
HDR_TTS = {"Authorization": f"Bearer {TTS_KEY}"}


def probe_models(client: httpx.Client):
    print("=== 1. GET /models ===")
    try:
        r = client.get(f"{LLM_BASE}/models", headers=HDR_LLM, timeout=15)
        ids = [m.get("id") for m in r.json().get("data", [])]
        print(f"  {len(ids)} models: {ids}")
        return ids
    except Exception as e:
        print(f"  FAILED: {e}")
        return []


def probe_ttft(client: httpx.Client, model: str, label: str = ""):
    """Streaming chat completion; report time-to-headers and time-to-first-token."""
    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": "你是语音助手，用一句话简短回答。"},
            {"role": "user", "content": "青海湖的水质怎么样？"},
        ],
        "stream": True,
        "max_tokens": 24,
    }
    t0 = time.perf_counter()
    try:
        with client.stream("POST", f"{LLM_BASE}/chat/completions",
                           headers=HDR_LLM, json=body, timeout=30) as r:
            t_hdr = time.perf_counter() - t0
            first = None
            for line in r.iter_lines():
                if line.startswith("data:") and '"content"' in line:
                    first = time.perf_counter() - t0
                    break
            print(f"  {model:24s} {label:10s} headers={t_hdr:5.2f}s  first_token={first if first else -1:5.2f}s  status={r.status_code}")
            return first
    except Exception as e:
        print(f"  {model:24s} {label:10s} FAILED: {type(e).__name__}: {e}")
        return None


def probe_tts(client: httpx.Client, text: str, stream: bool):
    body = {
        "model": TTS_MODEL,
        "messages": [
            {"role": "user", "content": "温柔亲切，语速适中。"},
            {"role": "assistant", "content": text},
        ],
        "audio": {"format": "pcm16", "voice": TTS_VOICE},
        "stream": stream,
    }
    t0 = time.perf_counter()
    try:
        if stream:
            with client.stream("POST", f"{TTS_BASE}/chat/completions",
                               headers=HDR_TTS, json=body, timeout=30) as r:
                t_hdr = time.perf_counter() - t0
                t_first_audio = None
                n_audio_chunks = 0
                total_b64 = 0
                for line in r.iter_lines():
                    if line.startswith("data:") and ('"audio"' in line or '"data"' in line):
                        if t_first_audio is None:
                            t_first_audio = time.perf_counter() - t0
                        n_audio_chunks += 1
                        total_b64 += len(line)
                t_done = time.perf_counter() - t0
                print(f"  stream=True  '{text[:12]}...'({len(text)}字) headers={t_hdr:.2f}s "
                      f"first_audio={t_first_audio if t_first_audio else -1:.2f}s "
                      f"done={t_done:.2f}s chunks={n_audio_chunks} status={r.status_code}")
        else:
            r = client.post(f"{TTS_BASE}/chat/completions",
                            headers=HDR_TTS, json=body, timeout=30)
            t_done = time.perf_counter() - t0
            audio_b = 0
            try:
                data = r.json()["choices"][0]["message"].get("audio", {}).get("data", "")
                audio_b = len(base64.b64decode(data)) if data else 0
            except Exception:
                pass
            print(f"  stream=False '{text[:12]}...'({len(text)}字) total={t_done:.2f}s "
                  f"pcm24k={audio_b}B(~{audio_b/48000:.1f}s audio) status={r.status_code}")
    except Exception as e:
        print(f"  stream={stream} '{text[:12]}...' FAILED: {type(e).__name__}: {e}")


def main():
    with httpx.Client() as client:
        models = probe_models(client)

        print("=== 2. LLM TTFT (streaming, max_tokens=24; 每模型两次:冷/热连接) ===")
        cur = os.environ.get("LLM_MODEL", "mimo-v2.5-pro")
        candidates = [cur]
        for m in models:
            ml = m.lower()
            if m != cur and any(k in ml for k in
                                ("flash", "mini", "lite", "turbo", "instant", "air",
                                 "mimo-v2.5", "haiku", "4o-mini")):
                candidates.append(m)
        candidates = candidates[:6]
        for m in candidates:
            probe_ttft(client, m, "cold")
            probe_ttft(client, m, "warm")

        print("=== 3. TTS non-streaming: short clause vs full sentence ===")
        probe_tts(client, "青海湖是中国最大的内陆咸水湖，", stream=False)
        probe_tts(client, "青海湖是中国最大的内陆咸水湖，水质总体来说保持得还不错。", stream=False)

        print("=== 4. TTS stream=True support ===")
        probe_tts(client, "青海湖是中国最大的内陆咸水湖，", stream=True)


if __name__ == "__main__":
    main()
