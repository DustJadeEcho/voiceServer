"""ASR — iFlytek Speech-to-Text (WebSocket streaming API).

Uses 讯飞语音听写（流式版）WebAPI.
Auth: HMAC-SHA256 signed WebSocket URL.
Audio: 16kHz/16bit/mono PCM, sent as base64-encoded frames.
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import queue
import ssl
import threading
import time
from abc import ABC, abstractmethod
from urllib.parse import urlencode
from wsgiref.handlers import format_date_time

import websocket

import config

logger = logging.getLogger("asr")

# iFlytek frame status codes
FIRST_FRAME = 0
MIDDLE_FRAME = 1
LAST_FRAME = 2

# Frame size for 16kHz PCM (1280 bytes = 40ms of audio)
FRAME_SIZE = 1280
# Send pacing: iFlytek docs suggest 40ms/frame (real-time), but batch upload
# tolerates much faster — 40ms would add ~1.5s latency for a 3s clip.
# 10ms cuts that to ~0.4s; raise via env if the API starts complaining.
FRAME_INTERVAL = float(os.environ.get("ASR_FRAME_INTERVAL_MS", "10")) / 1000.0


class ASREngine(ABC):
    """Abstract ASR engine."""

    @abstractmethod
    async def recognize(self, pcm_bytes: bytes) -> str:
        """Transcribe 16kHz/16bit/mono PCM to text."""


class XunfeiStreamSession:
    """Live-feed iFlytek session: connect during recording, feed chunks as they
    arrive from the MCU, and get the final text ~0.3s after finish().

    Removes the batch path's whole-clip upload + recognition wait (~1.3s).
    All blocking work lives in two daemon threads; feed() never blocks the
    asyncio loop, finish() is blocking and must run in an executor.
    """

    _LAST = object()          # sentinel: end of audio

    def __init__(self, build_url, appid: str):
        self._build_url = build_url
        self._appid = appid
        self._queue: "queue.Queue[bytes | object]" = queue.Queue()
        self._results: list[str] = []
        self._error: str | None = None
        self._connected = threading.Event()
        self._done = threading.Event()
        self._ws: websocket.WebSocketApp | None = None
        self._started = False

    # ── lifecycle (called from asyncio thread) ───────────────────────────────

    def start(self) -> None:
        """Open the WebSocket and start the sender thread (non-blocking)."""
        if self._started:
            return
        self._started = True
        self._ws = websocket.WebSocketApp(
            self._build_url(),
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
            on_open=lambda ws: self._connected.set(),
        )
        threading.Thread(
            target=self._ws.run_forever,
            kwargs={"sslopt": {"cert_reqs": ssl.CERT_NONE}},
            daemon=True, name="asr-ws",
        ).start()
        threading.Thread(target=self._sender, daemon=True, name="asr-send").start()

    def feed(self, pcm: bytes) -> None:
        """Queue one uplink chunk for sending (non-blocking)."""
        if self._started and not self._done.is_set():
            self._queue.put(pcm)

    def finish(self, timeout: float = 5.0) -> str:
        """Send last frame, wait for the final result. BLOCKING — run in executor."""
        if not self._started:
            raise RuntimeError("stream not started")
        self._queue.put(self._LAST)
        if not self._done.wait(timeout=timeout):
            self._error = self._error or "final result timeout"
        try:
            self._ws.close()
        except Exception:
            pass
        if self._error:
            raise RuntimeError(f"iFlytek stream failed: {self._error}")
        return "".join(self._results).strip()

    def abort(self) -> None:
        """Tear down without waiting (session superseded / cleanup)."""
        self._done.set()
        self._queue.put(self._LAST)
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass

    # ── worker threads ───────────────────────────────────────────────────────

    def _sender(self) -> None:
        """Drain the feed queue into 1280-byte iFlytek frames."""
        if not self._connected.wait(timeout=5.0):
            self._error = self._error or "connect timeout"
            self._done.set()
            return
        first = True
        pending = b""
        try:
            while True:
                item = self._queue.get()
                if item is self._LAST:
                    break
                pending += item
                while len(pending) >= FRAME_SIZE:
                    self._send_frame(FIRST_FRAME if first else MIDDLE_FRAME,
                                     pending[:FRAME_SIZE])
                    first = False
                    pending = pending[FRAME_SIZE:]
                    if self._queue.qsize() > 1:      # 积压时稍作节流
                        time.sleep(FRAME_INTERVAL)
            if pending:
                self._send_frame(FIRST_FRAME if first else MIDDLE_FRAME, pending)
                first = False
            if first:                                # no audio at all
                self._error = self._error or "no audio fed"
                self._done.set()
            else:
                self._send_frame(LAST_FRAME, b"")
        except Exception as e:
            self._error = self._error or str(e)
            self._done.set()

    def _send_frame(self, status: int, chunk: bytes) -> None:
        audio_b64 = base64.b64encode(chunk).decode("utf-8")
        if status == FIRST_FRAME:
            frame = {
                "common": {"app_id": self._appid},
                "business": {
                    "domain": "iat",
                    "language": "zh_cn",
                    "accent": "mandarin",
                    "vinfo": 1,
                    "vad_eos": 10000,
                },
                "data": {
                    "status": FIRST_FRAME,
                    "format": "audio/L16;rate=16000",
                    "audio": audio_b64,
                    "encoding": "raw",
                },
            }
        else:
            frame = {
                "data": {
                    "status": status,
                    "format": "audio/L16;rate=16000",
                    "audio": audio_b64 if status != LAST_FRAME else "",
                    "encoding": "raw",
                },
            }
        self._ws.send(json.dumps(frame))

    # ── WebSocket callbacks (ws thread) ──────────────────────────────────────

    def _on_message(self, ws, message):
        try:
            resp = json.loads(message)
            code = resp.get("code", -1)
            if code != 0:
                self._error = f"iFlytek error {code}: {resp.get('message', '')}"
                self._done.set()
                return
            data = resp.get("data", {})
            for item in data.get("result", {}).get("ws", []):
                for cw in item.get("cw", []):
                    if cw.get("w"):
                        self._results.append(cw["w"])
            if data.get("status", 0) == LAST_FRAME:
                self._done.set()
        except Exception as e:
            self._error = str(e)
            self._done.set()

    def _on_error(self, ws, error):
        self._error = self._error or str(error)
        self._done.set()

    def _on_close(self, ws, close_status_code, close_msg):
        self._done.set()


class XunfeiASR(ASREngine):
    """iFlytek streaming ASR via WebSocket.

    Flows:
    1. Build signed WebSocket URL (HMAC-SHA256)
    2. Connect to wss://ws-api.xfyun.cn/v2/iat
    3. Send audio in frames (first → middle → last)
    4. Receive partial results until final result
    """

    def __init__(self):
        self._appid = config.XUNFEI_APPID
        self._api_key = config.XUNFEI_API_KEY
        self._api_secret = config.XUNFEI_API_SECRET
        self._host = "ws-api.xfyun.cn"
        self._path = "/v2/iat"
        self._url = f"wss://{self._host}{self._path}"

    async def recognize(self, pcm_bytes: bytes) -> str:
        if not self._appid:
            raise RuntimeError("XUNFEI_APPID not configured")

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._recognize_sync, pcm_bytes)

    def open_stream(self) -> XunfeiStreamSession:
        """Create a live-feed session (call .start() to connect)."""
        if not self._appid:
            raise RuntimeError("XUNFEI_APPID not configured")
        return XunfeiStreamSession(self._build_auth_url, self._appid)

    def _recognize_sync(self, pcm_bytes: bytes) -> str:
        """Synchronous recognition — runs in thread pool."""
        ws_url = self._build_auth_url()

        results = []
        error_msg = [None]
        done_event = threading.Event()

        def on_message(ws, message):
            try:
                resp = json.loads(message)
                code = resp.get("code", -1)
                if code != 0:
                    error_msg[0] = f"iFlytek error {code}: {resp.get('message', '')}"
                    done_event.set()
                    return

                data = resp.get("data", {})
                result = data.get("result", {})

                # Extract text from ws[].cw[].w
                ws_list = result.get("ws", [])
                for item in ws_list:
                    for cw in item.get("cw", []):
                        w = cw.get("w", "")
                        if w:
                            results.append(w)

                # Check if this is the final result
                status = data.get("status", 0)
                if status == LAST_FRAME:
                    done_event.set()

            except Exception as e:
                logger.error("iFlytek parse error: %s", e)
                error_msg[0] = str(e)
                done_event.set()

        def on_error(ws, error):
            logger.error("iFlytek WebSocket error: %s", error)
            error_msg[0] = str(error)
            done_event.set()

        def on_close(ws, close_status_code, close_msg):
            logger.debug("iFlytek WebSocket closed")
            done_event.set()

        def on_open(ws):
            try:
                self._send_audio_frames(ws, pcm_bytes)
            except Exception as e:
                logger.error("iFlytek send error: %s", e)
                error_msg[0] = str(e)
                done_event.set()

        ws = websocket.WebSocketApp(
            ws_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open,
        )

        # Run WebSocket in a thread with timeout
        ws_thread = threading.Thread(
            target=ws.run_forever,
            kwargs={"sslopt": {"cert_reqs": ssl.CERT_NONE}},
            daemon=True,
        )
        ws_thread.start()

        # Wait for completion with timeout
        try:
            if not done_event.wait(timeout=config.API_TIMEOUT + 5):
                raise RuntimeError("iFlytek ASR timed out")
        finally:
            ws.close()      # 成功也要主动关——不关会挂到讯飞侧 15s 超时并打错误日志

        if error_msg[0]:
            raise RuntimeError(f"iFlytek ASR failed: {error_msg[0]}")

        text = "".join(results).strip()
        logger.debug("iFlytek result: %s", text[:80])
        return text

    def _send_audio_frames(self, ws, pcm_bytes: bytes):
        """Send PCM audio as framed WebSocket messages."""
        chunks = [pcm_bytes[i:i + FRAME_SIZE]
                  for i in range(0, len(pcm_bytes), FRAME_SIZE)]

        if not chunks:
            # Send empty last frame
            ws.send(json.dumps({"data": {"status": LAST_FRAME}}))
            return

        for i, chunk in enumerate(chunks):
            if i == 0:
                status = FIRST_FRAME
            elif i == len(chunks) - 1:
                status = LAST_FRAME
            else:
                status = MIDDLE_FRAME

            frame = self._build_frame(status, chunk)
            ws.send(json.dumps(frame))

            if status == LAST_FRAME:
                time.sleep(0.2)          # 给服务端处理尾帧留量（结果靠 done_event 等）
            else:
                time.sleep(FRAME_INTERVAL)

    def _build_frame(self, status: int, audio_chunk: bytes) -> dict:
        """Build a WebSocket message frame."""
        audio_b64 = base64.b64encode(audio_chunk).decode("utf-8")

        if status == FIRST_FRAME:
            return {
                "common": {"app_id": self._appid},
                "business": {
                    "domain": "iat",
                    "language": "zh_cn",
                    "accent": "mandarin",
                    "vinfo": 1,
                    "vad_eos": 10000,
                },
                "data": {
                    "status": FIRST_FRAME,
                    "format": "audio/L16;rate=16000",
                    "audio": audio_b64,
                    "encoding": "raw",
                },
            }
        else:
            return {
                "data": {
                    "status": status,
                    "format": "audio/L16;rate=16000",
                    "audio": audio_b64 if status != LAST_FRAME else "",
                    "encoding": "raw",
                },
            }

    def _build_auth_url(self) -> str:
        """Build authenticated WebSocket URL with HMAC-SHA256 signature."""
        # RFC1123 GMT date. format_date_time takes a POSIX timestamp directly —
        # the old mktime(utcnow().timetuple()) treated a UTC tuple as local time
        # and only worked when the server TZ happened to be UTC.
        date = format_date_time(time.time())

        # Signature origin: host, date, request-line
        signature_origin = (
            f"host: {self._host}\n"
            f"date: {date}\n"
            f"GET {self._path} HTTP/1.1"
        )

        # HMAC-SHA256 signature
        signature_sha = hmac.new(
            self._api_secret.encode("utf-8"),
            signature_origin.encode("utf-8"),
            digestmod=hashlib.sha256,
        ).digest()
        signature = base64.b64encode(signature_sha).decode("utf-8")

        # Authorization header
        authorization_origin = (
            f'api_key="{self._api_key}", '
            f'algorithm="hmac-sha256", '
            f'headers="host date request-line", '
            f'signature="{signature}"'
        )
        authorization = base64.b64encode(
            authorization_origin.encode("utf-8")
        ).decode("utf-8")

        # Build URL
        params = {
            "authorization": authorization,
            "date": date,
            "host": self._host,
        }
        return f"{self._url}?{urlencode(params)}"


def create_asr_engine() -> ASREngine:
    """Factory — returns the configured ASR engine."""
    return XunfeiASR()
