"""Audio utilities — unified packet codec, WAV wrap, resampling.

Packet format (must match MCU `mqtt_connect.h`, all little-endian):

    [0:2]    head   = 5A 5A
    [2:4]    type   = frame type (PCM = 1), uint16 LE
    [4:8]    len    = data length in bytes (incl. 8-byte session header), uint32 LE
    [8:12]   session= uint32 LE (device-generated, echoed back in downlink)
    [12:16]  seq    = uint32 LE (per-direction, starts at 0)
    [16:16+n]        PCM payload (16 kHz / 16-bit / mono / LE)
    [-4:-2]  crc16  = CRC16-CCITT(poly 0x1021, init 0xFFFF) over data, uint16 LE
    [-2:]    end    = 6B 6B
"""

import binascii
import struct

import numpy as np

PKT_HEAD = 0x5A
PKT_END = 0x6B
PKT_TYPE_PCM = 1
PKT_OVERHEAD = 12       # head(2) + type(2) + len(4) + crc(2) + end(2)
SESS_HDR = 8            # session(4) + seq(4) inside data
PCM_CHUNK = 4000        # downlink PCM bytes per packet (2000 samples = 125 ms)

_HDR = struct.Struct("<HHI")    # head-word, type, len
_SESS = struct.Struct("<II")    # session, seq


def crc16_ccitt(data: bytes) -> int:
    """CRC16-CCITT (poly 0x1021, init 0xFFFF, MSB-first) — matches MCU crc_x.c.

    binascii.crc_hqx implements exactly this polynomial at C speed.
    """
    return binascii.crc_hqx(data, 0xFFFF)


def decode_packet(raw: bytes) -> tuple[int, int, bytes, bool]:
    """Decode one audio packet.

    Returns:
        (session, seq, pcm, crc_ok)
    Raises:
        ValueError: on structural errors (bad head/end/length).
    """
    if len(raw) < PKT_OVERHEAD + SESS_HDR:
        raise ValueError(f"packet too short: {len(raw)}")
    if raw[0] != PKT_HEAD or raw[1] != PKT_HEAD:
        raise ValueError(f"bad head {raw[0]:02x}{raw[1]:02x}")

    _, ptype, dlen = _HDR.unpack_from(raw, 0)
    if ptype != PKT_TYPE_PCM:
        raise ValueError(f"unknown type {ptype}")
    if dlen < SESS_HDR or 8 + dlen + 4 > len(raw):
        raise ValueError(f"bad len {dlen} (raw {len(raw)})")

    data = raw[8:8 + dlen]
    crc_rx, = struct.unpack_from("<H", raw, 8 + dlen)
    end0, end1 = raw[8 + dlen + 2], raw[8 + dlen + 3]
    if end0 != PKT_END or end1 != PKT_END:
        raise ValueError(f"bad end mark {end0:02x}{end1:02x}")

    session, seq = _SESS.unpack_from(data, 0)
    pcm = data[SESS_HDR:]
    crc_ok = (crc_rx == crc16_ccitt(data))
    return session, seq, pcm, crc_ok


def encode_packet(session: int, seq: int, pcm: bytes) -> bytes:
    """Build one downlink audio packet (same format as uplink)."""
    data = _SESS.pack(session & 0xFFFFFFFF, seq & 0xFFFFFFFF) + pcm
    crc = crc16_ccitt(data)
    return (
        _HDR.pack(PKT_HEAD | (PKT_HEAD << 8), PKT_TYPE_PCM, len(data))
        + data
        + struct.pack("<H", crc)
        + bytes((PKT_END, PKT_END))
    )


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
    """Resample 16-bit mono PCM using linear interpolation (numpy only)."""
    if from_rate == to_rate:
        return pcm_bytes

    samples = np.frombuffer(pcm_bytes, dtype=np.int16).astype(np.float64)
    if len(samples) == 0:
        return pcm_bytes

    n_out = int(len(samples) / from_rate * to_rate)
    if n_out == 0:
        return b""

    x_old = np.linspace(0, 1, len(samples), endpoint=False)
    x_new = np.linspace(0, 1, n_out, endpoint=False)
    resampled = np.interp(x_new, x_old, samples)
    return np.clip(resampled, -32768, 32767).astype(np.int16).tobytes()


def chunk_pcm(pcm_bytes: bytes, chunk_size: int = PCM_CHUNK) -> list[bytes]:
    """Split PCM into chunks of `chunk_size` bytes (last chunk may be shorter).

    Keeps chunks sample-aligned (even byte counts).
    """
    if chunk_size % 2:
        chunk_size -= 1
    return [pcm_bytes[i:i + chunk_size] for i in range(0, len(pcm_bytes), chunk_size)]
