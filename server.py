"""Voice Server — main entry point.

MQTT client + asyncio event loop + session management.
Runs as a systemd service on Ubuntu 24.04 with Python 3.12 and paho-mqtt 2.x.

Protocol (must match MCU application/samples/voiceTest/inc/mqtt_connect.h):
    audio packets : 5A5A envelope, session/seq uint32 LE, CRC16-CCITT — see audio.py
    control (up)  : {"event":"start","session":N,"max_duration":4} / {"event":"stop","session":N}
    control (down): {"event":"done","session":N} / {"event":"error","session":N,"reason":"..."}
"""

import asyncio
import json
import logging
import signal
import sys
import time

import paho.mqtt.client as mqtt

import config
from asr import create_asr_engine
from audio import decode_packet
from llm import LLMClient
from session import Session
from tts import TTSEngine

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("server")


# ─── Session Manager ────────────────────────────────────────────────────────
class SessionManager:
    """Manages active sessions with limits and timeouts."""

    def __init__(self, asr, llm, tts, publish_fn):
        self._sessions: dict[int, Session] = {}
        self._asr = asr
        self._llm = llm
        self._tts = tts
        self._publish = publish_fn

    @property
    def count(self) -> int:
        return len(self._sessions)

    def get(self, session_id: int) -> Session | None:
        return self._sessions.get(session_id)

    def create(self, session_id: int, max_duration: int = 60) -> Session | None:
        """Create a new session. Returns None if limit reached or duplicate."""
        if session_id in self._sessions:
            logger.warning("Duplicate session %s, ignoring start", session_id)
            return None

        if len(self._sessions) >= config.MAX_SESSIONS:
            logger.warning("Max sessions (%d) reached, rejecting %s",
                           config.MAX_SESSIONS, session_id)
            return None

        session = Session(
            session_id=session_id,
            publish_fn=self._publish,
            asr=self._asr,
            llm=self._llm,
            tts=self._tts,
            max_duration=max_duration,
        )
        self._sessions[session_id] = session
        logger.info("Session created: %s (%d/%d active)",
                    session_id, len(self._sessions), config.MAX_SESSIONS)
        return session

    def remove(self, session_id: int) -> None:
        session = self._sessions.pop(session_id, None)
        if session:
            session.cleanup()
            logger.info("Session removed: %s (%d active)", session_id, len(self._sessions))

    def all_ids(self) -> list[int]:
        return list(self._sessions.keys())

    def cleanup_stale(self) -> None:
        """Remove sessions that exceeded the global timeout."""
        stale = [sid for sid, s in self._sessions.items() if s.age > config.SESSION_TIMEOUT]
        for sid in stale:
            logger.warning("Session %s timed out (age=%.0fs), removing",
                           sid, self._sessions[sid].age)
            self.remove(sid)


# ─── Main Server ─────────────────────────────────────────────────────────────
class VoiceServer:
    """Main voice server — MQTT client + asyncio event loop."""

    def __init__(self):
        # paho-mqtt 2.x callback API
        self._mqtt = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id="voice-server",
            protocol=mqtt.MQTTv311,
        )

        # AI engines
        self._asr = create_asr_engine()
        self._llm = LLMClient()
        self._tts = TTSEngine()

        # Session manager
        self._sessions = SessionManager(
            asr=self._asr,
            llm=self._llm,
            tts=self._tts,
            publish_fn=self._async_publish,
        )

        self._loop: asyncio.AbstractEventLoop | None = None
        self._msg_queue: asyncio.Queue | None = None
        self._running = False

    # ── Entry ────────────────────────────────────────────────────────────────

    def start(self) -> None:
        """Start the server (blocks until shutdown)."""
        logger.info("Voice server starting...")
        logger.info("MQTT: %s:%d (TLS=%s)", config.MQTT_HOST, config.MQTT_PORT,
                    bool(config.MQTT_CA_CERT))
        logger.info("LLM: %s / %s", config.LLM_BASE_URL, config.LLM_MODEL)
        logger.info("TTS: %s / %s (voice=%s)",
                    config.TTS_BASE_URL, config.TTS_MODEL, config.TTS_VOICE)
        logger.info("Limits: max_sessions=%d, timeout=%ds, max_pcm=%dMB, burst=%.1fs",
                    config.MAX_SESSIONS, config.SESSION_TIMEOUT,
                    config.MAX_PCM_SIZE // (1024 * 1024), config.DOWN_BURST_SECONDS)
        asyncio.run(self._main())

    async def _main(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._msg_queue = asyncio.Queue()
        self._running = True

        for sig in (signal.SIGTERM, signal.SIGINT):
            self._loop.add_signal_handler(sig, self._signal_shutdown)

        self._setup_mqtt()
        try:
            self._mqtt.connect(config.MQTT_HOST, config.MQTT_PORT, keepalive=60)
        except Exception as e:
            logger.error("MQTT connect error: %s", e)
            return
        self._mqtt.loop_start()          # paho network thread

        cleanup_task = asyncio.create_task(self._periodic_cleanup())
        logger.info("Server ready, waiting for sessions...")

        try:
            while self._running:
                try:
                    topic, payload = await asyncio.wait_for(
                        self._msg_queue.get(), timeout=5.0)
                except asyncio.TimeoutError:
                    continue
                try:
                    await self._handle_message(topic, payload)
                except Exception as e:
                    logger.error("Message handling error: %s", e, exc_info=True)
        finally:
            cleanup_task.cancel()
            await self._shutdown()

    # ── MQTT plumbing ─────────────────────────────────────────────────────────

    def _setup_mqtt(self) -> None:
        """Configure MQTT client (auth, optional TLS, callbacks, reconnect)."""
        if config.MQTT_USERNAME:
            self._mqtt.username_pw_set(config.MQTT_USERNAME, config.MQTT_PASSWORD)

        if config.MQTT_CA_CERT:                      # TLS only when CA provided
            self._mqtt.tls_set(ca_certs=config.MQTT_CA_CERT)

        self._mqtt.reconnect_delay_set(min_delay=1, max_delay=30)
        self._mqtt.on_connect = self._on_connect
        self._mqtt.on_disconnect = self._on_disconnect
        self._mqtt.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            logger.info("MQTT connected")
            client.subscribe([
                (config.TOPIC_UP_AUDIO, 0),    # audio: QoS 0
                (config.TOPIC_UP_CONTROL, 1),  # control: QoS 1
            ])
            logger.info("Subscribed: %s , %s",
                        config.TOPIC_UP_AUDIO, config.TOPIC_UP_CONTROL)
        else:
            logger.error("MQTT connect failed: %s", reason_code)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        if reason_code != 0:
            logger.warning("MQTT unexpected disconnect (%s), auto-reconnecting",
                           reason_code)

    def _on_message(self, client, userdata, msg) -> None:
        """MQTT callback (network thread) — hand off to the asyncio loop."""
        try:
            self._loop.call_soon_threadsafe(
                self._msg_queue.put_nowait, (msg.topic, msg.payload))
        except Exception as e:
            logger.error("Failed to queue MQTT message: %s", e)

    async def _async_publish(self, topic: str, payload: bytes, qos: int) -> None:
        """Publish from asyncio context (paho publish is thread-safe and non-blocking)."""
        result = self._mqtt.publish(topic, payload, qos)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            logger.error("MQTT publish failed on %s: rc=%d", topic, result.rc)

    # ── Message routing ───────────────────────────────────────────────────────

    async def _handle_message(self, topic: str, payload: bytes) -> None:
        if topic == config.TOPIC_UP_CONTROL:
            await self._handle_control(payload)
        elif topic == config.TOPIC_UP_AUDIO:
            await self._handle_audio(payload)
        else:
            logger.debug("Unknown topic: %s", topic)

    async def _handle_control(self, payload: bytes) -> None:
        """Handle uplink control messages (start/stop)."""
        try:
            msg = json.loads(payload)
            event = msg.get("event")
            session_id = int(msg.get("session", 0))
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            logger.warning("Bad control message %r: %s", payload[:80], e)
            return

        if session_id == 0:
            logger.warning("Control message missing session")
            return

        if event == "start":
            max_duration = int(msg.get("max_duration", 60))
            # 单设备策略: 新 start 到来即作废所有旧会话——停止其残余下发
            # （设备端已换会话号，旧包只会被当 stale 丢弃，白耗带宽/串扰日志）
            for sid in self._sessions.all_ids():
                if sid != session_id:
                    logger.info("start %d supersedes session %d, cancelling",
                                session_id, sid)
                    self._sessions.remove(sid)
            if self._sessions.get(session_id):
                await self._send_error_to(session_id, "duplicate_session")
                return
            session = self._sessions.create(session_id, max_duration)
            if session:
                session.start()
            else:
                await self._send_error_to(session_id, "max_sessions_reached")

        elif event == "stop":
            session = self._sessions.get(session_id)
            if session:
                session._task = asyncio.create_task(self._process_session(session))
            else:
                logger.warning("Stop for unknown session %s", session_id)

        else:
            logger.warning("Unknown control event: %s", event)

    async def _process_session(self, session: Session) -> None:
        """Run the full pipeline for a session, then clean up."""
        try:
            await session.handle_stop()
        except Exception as e:
            logger.error("[%s] Pipeline exception: %s", session.session_id, e, exc_info=True)
            try:
                await session._send_error("internal_error")
            except Exception:
                pass
        finally:
            await asyncio.sleep(1)       # let the final control message flush
            self._sessions.remove(session.session_id)

    async def _handle_audio(self, payload: bytes) -> None:
        """Handle one uplink audio packet — decode once, route by session."""
        try:
            session_id, seq, pcm, crc_ok = decode_packet(payload)
        except ValueError as e:
            logger.warning("Bad audio packet: %s", e)
            return

        if not crc_ok:
            logger.warning("CRC mismatch on uplink seq=%d (accepting)", seq)

        session = self._sessions.get(session_id)
        if session:
            session.handle_audio_chunk(seq, pcm)
        else:
            logger.debug("Audio for unknown session %s (seq=%d), dropping",
                         session_id, seq)

    # ── Housekeeping ──────────────────────────────────────────────────────────

    async def _periodic_cleanup(self) -> None:
        while self._running:
            await asyncio.sleep(10)
            self._sessions.cleanup_stale()

    async def _send_error_to(self, session_id: int, reason: str) -> None:
        msg = json.dumps({"event": "error", "session": session_id, "reason": reason})
        await self._async_publish(config.TOPIC_DOWN_CONTROL, msg.encode(), 1)

    def _signal_shutdown(self) -> None:
        logger.info("Shutdown signal received")
        self._running = False

    async def _shutdown(self) -> None:
        logger.info("Shutting down...")
        self._running = False
        for sid in self._sessions.all_ids():
            self._sessions.remove(sid)
        self._mqtt.loop_stop()
        self._mqtt.disconnect()
        logger.info("Server stopped")


# ─── Entry Point ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    VoiceServer().start()
