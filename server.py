"""Voice Server — main entry point.

MQTT client + asyncio event loop + session management.
Designed to run as a systemd service on Ubuntu 20.04 with Python 3.8.10.
"""

import asyncio
import json
import logging
import signal
import sys
from typing import Dict, Optional

import paho.mqtt.client as mqtt

import config
from asr import create_asr_engine
from llm import LLMClient
from session import Session, State
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
        self._sessions: Dict[str, Session] = {}
        self._asr = asr
        self._llm = llm
        self._tts = tts
        self._publish = publish_fn

    @property
    def count(self) -> int:
        return len(self._sessions)

    def get(self, session_id: str) -> Optional[Session]:
        return self._sessions.get(session_id)

    def create(self, session_id: str, max_duration: int = 60) -> Optional[Session]:
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

    def remove(self, session_id: str):
        session = self._sessions.pop(session_id, None)
        if session:
            session.cleanup()
            logger.info("Session removed: %s (%d active)", session_id, len(self._sessions))

    def cleanup_stale(self):
        """Remove sessions that exceeded timeout."""
        now = time.monotonic()
        stale = []
        for sid, session in self._sessions.items():
            if session.age > config.SESSION_TIMEOUT:
                stale.append(sid)

        for sid in stale:
            logger.warning("Session %s timed out (age=%.0fs), removing", sid,
                           self._sessions[sid].age)
            self.remove(sid)


# ─── Main Server ─────────────────────────────────────────────────────────────
class VoiceServer:
    """Main voice server — MQTT client + asyncio event loop."""

    def __init__(self):
        self._loop = asyncio.new_event_loop()
        self._mqtt = mqtt.Client(
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

        # Asyncio queue for MQTT messages → event loop
        self._msg_queue: asyncio.Queue = None
        self._running = False
        self._cleanup_task: Optional[asyncio.Task] = None

    def start(self):
        """Start the server (blocks until shutdown)."""
        logger.info("Voice server starting...")
        logger.info("MQTT: %s:%d (TLS=%s)", config.MQTT_HOST, config.MQTT_PORT,
                     bool(config.MQTT_CA_CERT or config.MQTT_PORT == 8883))
        logger.info("LLM: %s / %s", config.LLM_BASE_URL, config.LLM_MODEL)
        logger.info("TTS: %s / %s (voice=%s)", config.TTS_BASE_URL, config.TTS_MODEL, config.TTS_VOICE)
        logger.info("Limits: max_sessions=%d, timeout=%ds, max_pcm=%dMB",
                     config.MAX_SESSIONS, config.SESSION_TIMEOUT,
                     config.MAX_PCM_SIZE // (1024 * 1024))

        asyncio.set_event_loop(self._loop)
        self._setup_mqtt()
        self._msg_queue = asyncio.Queue(loop=self._loop)

        # Signal handlers
        for sig in (signal.SIGTERM, signal.SIGINT):
            self._loop.add_signal_handler(sig, self._signal_shutdown)

        try:
            self._loop.run_until_complete(self._run())
        finally:
            self._loop.run_until_complete(self._shutdown())
            self._loop.close()

    def _setup_mqtt(self):
        """Configure MQTT client (TLS, auth, callbacks)."""
        # Auth
        if config.MQTT_USERNAME:
            self._mqtt.username_pw_set(config.MQTT_USERNAME, config.MQTT_PASSWORD)

        # TLS
        if config.MQTT_PORT == 8883 or config.MQTT_CA_CERT:
            import ssl
            tls_kwargs = {}
            if config.MQTT_CA_CERT:
                tls_kwargs["ca_certs"] = config.MQTT_CA_CERT
            else:
                # If no CA cert provided but using 8883, allow self-signed for development
                # or use system certs with relaxed verification if needed
                tls_kwargs["cert_reqs"] = ssl.CERT_NONE

            self._mqtt.tls_set(**tls_kwargs)
            if not config.MQTT_CA_CERT:
                self._mqtt.tls_insecure_set(True)

        # Callbacks
        self._mqtt.on_connect = self._on_connect
        self._mqtt.on_disconnect = self._on_disconnect
        self._mqtt.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info("MQTT connected")
            client.subscribe([
                (config.TOPIC_UP_AUDIO, 0),    # QoS 0
                (config.TOPIC_UP_CONTROL, 1),  # QoS 1
            ])
            logger.info("Subscribed to %s and %s", config.TOPIC_UP_AUDIO, config.TOPIC_UP_CONTROL)
        else:
            logger.error("MQTT connect failed: rc=%d", rc)

    def _on_disconnect(self, client, userdata, rc):
        if rc != 0:
            logger.warning("MQTT unexpected disconnect (rc=%d), will auto-reconnect", rc)

    def _on_message(self, client, userdata, msg):
        """MQTT callback — forward to asyncio queue (thread-safe)."""
        try:
            self._loop.call_soon_threadsafe(
                self._msg_queue.put_nowait,
                (msg.topic, msg.payload),
            )
        except Exception as e:
            logger.error("Failed to queue MQTT message: %s", e)

    async def _async_publish(self, topic: str, payload: bytes, qos: int):
        """Publish MQTT message from asyncio context."""
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, self._mqtt.publish, topic, payload, qos
        )
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            logger.error("MQTT publish failed on %s: rc=%d", topic, result.rc)

    async def _run(self):
        """Main event loop — process MQTT messages and periodic cleanup."""
        self._running = True
        self._cleanup_task = asyncio.ensure_future(self._periodic_cleanup())

        # Connect MQTT
        try:
            self._mqtt.connect(config.MQTT_HOST, config.MQTT_PORT, keepalive=60)
        except Exception as e:
            logger.error("MQTT connect error: %s", e)
            return

        self._mqtt.loop_start()

        logger.info("Server ready, waiting for connections...")

        while self._running:
            try:
                topic, payload = await asyncio.wait_for(
                    self._msg_queue.get(), timeout=5.0
                )
                await self._handle_message(topic, payload)
            except asyncio.TimeoutError:
                pass  # Periodic check
            except Exception as e:
                logger.error("Message handling error: %s", e, exc_info=True)

    async def _handle_message(self, topic: str, payload: bytes):
        """Route incoming MQTT messages to appropriate handlers."""
        if topic == config.TOPIC_UP_CONTROL:
            await self._handle_control(payload)
        elif topic == config.TOPIC_UP_AUDIO:
            await self._handle_audio(payload)
        else:
            logger.debug("Unknown topic: %s", topic)

    async def _handle_control(self, payload: bytes):
        """Handle uplink control messages (start/stop)."""
        try:
            msg = json.loads(payload)
        except json.JSONDecodeError as e:
            logger.warning("Bad control JSON: %s", e)
            return

        event = msg.get("event")
        session_id = msg.get("session_id", "")

        if not session_id:
            logger.warning("Control message missing session_id")
            return

        if event == "start":
            max_duration = msg.get("max_duration", 60)
            existing = self._sessions.get(session_id)
            if existing:
                # Duplicate start — send error, ignore
                await self._send_error_to(session_id, "duplicate_session")
                return

            session = self._sessions.create(session_id, max_duration)
            if session:
                session.start()
            else:
                # Creation failed (max sessions reached)
                await self._send_error_to(session_id, "max_sessions_reached")

        elif event == "stop":
            session = self._sessions.get(session_id)
            if session:
                # Run pipeline in background task
                session._task = asyncio.ensure_future(self._process_session(session))
            else:
                logger.warning("Stop for unknown session %s", session_id)

        else:
            logger.warning("Unknown control event: %s", event)

    async def _process_session(self, session: Session):
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
            # Delay cleanup slightly to allow final messages to be sent
            await asyncio.sleep(1)
            self._sessions.remove(session.session_id)

    async def _handle_audio(self, payload: bytes):
        """Handle uplink audio chunk — route to correct session."""
        from audio import decode_uplink
        try:
            session_id, seq, pcm = decode_uplink(payload)
        except ValueError as e:
            logger.warning("Bad audio packet: %s", e)
            return

        session = self._sessions.get(session_id)
        if session:
            session.handle_audio_chunk(payload)
        else:
            # No session yet — might be audio before start, or stale
            logger.debug("Audio for unknown session %s (seq=%d), dropping", session_id, seq)

    async def _periodic_cleanup(self):
        """Periodically check for stale sessions."""
        while self._running:
            await asyncio.sleep(10)
            self._sessions.cleanup_stale()

    async def _send_error_to(self, session_id: str, reason: str):
        """Send error control message for a session not yet in the manager."""
        msg = json.dumps({"event": "error", "session_id": session_id, "reason": reason})
        await self._async_publish(config.TOPIC_DOWN_CONTROL, msg.encode(), 1)

    def _signal_shutdown(self):
        """Handle SIGTERM/SIGINT."""
        logger.info("Shutdown signal received")
        self._running = False

    async def _shutdown(self):
        """Graceful shutdown."""
        logger.info("Shutting down...")
        self._running = False

        if self._cleanup_task:
            self._cleanup_task.cancel()

        # Clean up all sessions
        for sid in list(self._sessions._sessions.keys()):
            self._sessions.remove(sid)

        self._mqtt.loop_stop()
        self._mqtt.disconnect()
        logger.info("Server stopped")


# ─── Entry Point ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    server = VoiceServer()
    server.start()
