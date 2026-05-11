"""Audio utilities — packet encode/decode, WAV conversion, resampling."""

import struct
import numpy as np

# ─── Packet format (per Lele.md §6) ──────────────────────────────────────────
#  Up/Down link audio packet (4096 bytes):
#    [0:36]   session_id  (36 bytes, UTF-8 UUID, padded with \x00)
#    [36:40]  sequence    (4 bytes, uint32 big-endian)
#    [40:]    PCM payload (4056 bytes max)

SESSION_ID_LEN = 36
HEADER_LEN = SESSION_ID_LEN + 4  # 40 bytes
PACKET_SIZE = 4096
PCM_PAYLOAD_SIZE = PACKET_SIZE - HEADER_LEN  # 4056


def decode_uplink(data: bytes):
    """Decode an uplink/downlink audio packet.

    Returns:
        (session_id: str, seq: int, pcm: bytes)
    Raises:
        ValueError: if packet is too short
    """
    if len(data) < HEADER_LEN:
        raise ValueError(f"Packet too short: {len(data)} < {HEADER_LEN}")

    raw_id = data[:SESSION_ID_LEN]
    session_id = raw_id.rstrip(b"\x00").decode("utf-8")
    seq = struct.unpack("!I", data[SESSION_ID_LEN:HEADER_LEN])[0]
    pcm = data[HEADER_LEN:]
    return session_id, seq, pcm


def encode_downlink(session_id: str, seq: int, pcm: bytes) -> bytes:
    """Build a downlink audio packet.

    Args:
        session_id: UUID string (will be padded/truncated to 36 bytes)
        seq: sequence number (uint32)
        pcm: PCM payload bytes

    Returns:
        4096-byte packet (may be shorter for last chunk)
    """
    id_bytes = session_id.encode("utf-8")[:SESSION_ID_LEN].ljust(SESSION_ID_LEN, b"\x00")
    header = id_bytes + struct.pack("!I", seq)
    return header + pcm


def pcm_to_wav(pcm_bytes: bytes, sample_rate: int = 16000,
               channels: int = 1, bits: int = 16) -> bytes:
    """Wrap raw PCM in a WAV header (for ASR APIs that need WAV input)."""
    data_size = len(pcm_bytes)
    byte_rate = sample_rate * channels * (bits // 8)
    block_align = channels * (bits // 8)

    header = struct.pack(
        "<4sI4s"       # RIFF header
        "4sIHHIIHH"    # fmt chunk
        "4sI",         # data chunk header
        b"RIFF", 36 + data_size, b"WAVE",
        b"fmt ", 16, 1, channels, sample_rate, byte_rate, block_align, bits,
        b"data", data_size,
    )
    return header + pcm_bytes


def resample_pcm(pcm_bytes: bytes, from_rate: int, to_rate: int) -> bytes:
    """Resample 16-bit mono PCM using linear interpolation.

    Works on Python 3.8 with numpy only (no scipy/librosa).
    """
    if from_rate == to_rate:
        return pcm_bytes

    samples = np.frombuffer(pcm_bytes, dtype=np.int16).astype(np.float64)
    if len(samples) == 0:
        return pcm_bytes

    duration = len(samples) / from_rate
    n_out = int(duration * to_rate)
    if n_out == 0:
        return b""

    x_old = np.linspace(0, 1, len(samples), endpoint=False)
    x_new = np.linspace(0, 1, n_out, endpoint=False)
    resampled = np.interp(x_new, x_old, samples)
    return resampled.astype(np.int16).tobytes()


def chunk_pcm(pcm_bytes: bytes, chunk_size: int = PCM_PAYLOAD_SIZE) -> list:
    """Split PCM into chunks of `chunk_size` bytes (last chunk may be shorter)."""
    return [pcm_bytes[i:i + chunk_size] for i in range(0, len(pcm_bytes), chunk_size)]
