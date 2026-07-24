"""Sensors — boat water-quality / GPS frames from MQTT → cache + JSONL log + LLM context.

Frame source (must match MCU qhmu_water_ctrl / qhmu_water_boat dataExc.h):
    The ctrl board republishes the boat's raw SLE frames on MQTT as-is —
    payload is the packed 64-byte little-endian struct, no extra wrapping.

    BoatHullWaterCheckDataFrame (64B): <6H4I32sHH
        head length clientId serverId type | TempValue(u16) | PH ORP TDS Turb(u32) | ret[32] crc end
    BoatHullGpsDataFrame       (64B): <8H3i13B19sHH
        head length clientId serverId type utcYear bjYear speedX100 |
        lat lon alt(i32) | utc M/D/h/m/s bj M/D/h/m/s fixQuality numSat isValid | ret[19] crc end

    head=0x5A5A, end=0x6B6B; type: MEASURED_DATA=1, GPS_DATA=4 (commonPrj.h).
    Caution: the ctrl board routes *every non-GPS frame* to the water topic,
    so the type field must be checked, not assumed from the topic.

Scaling (verified against boat sources + web client):
    temp = raw × 0.0625 °C (DS18B20 register, int16)
    pH / ORP(mV, signed) / TDS(ppm) / turbidity(NTU) = raw / 100
    lat/lon = raw / 1e6 °,  alt = raw / 100 m,  speed = raw / 100 km/h

MCU publishes without retain, so a server restart would lose the last values —
each accepted frame is appended to a JSONL log and the newest line is read
back at startup to warm the cache.
"""

import json
import logging
import os
import struct
import time

import config
from audio import crc16_ccitt

logger = logging.getLogger("sensors")

FRAME_LEN = 64
FRAME_HEAD = 0x5A5A
FRAME_END = 0x6B6B
TYPE_MEASURED = 1        # TotalFrameDataType.MEASURED_DATA
TYPE_GPS = 4             # TotalFrameDataType.GPS_DATA

_WATER_FMT = struct.Struct("<6H4I32sHH")
_GPS_FMT = struct.Struct("<8H3i13B19sHH")
assert _WATER_FMT.size == FRAME_LEN and _GPS_FMT.size == FRAME_LEN

_EXPECTED_TYPE = {"water": TYPE_MEASURED, "gps": TYPE_GPS}


def _s16(v: int) -> int:
    """Reinterpret uint16 as int16 (DS18B20 negative temperatures)."""
    return v - 0x10000 if v >= 0x8000 else v


def _s32(v: int) -> int:
    """Reinterpret uint32 as int32 (ORP below reference voltage goes negative)."""
    return v - 0x1_0000_0000 if v >= 0x8000_0000 else v


def _parse_water(frame: bytes) -> dict:
    (_head, _length, _cid, _sid, _type, raw_temp,
     raw_ph, raw_orp, raw_tds, raw_turb, _ret, _crc, _end) = _WATER_FMT.unpack(frame)
    return {
        "temp_c": round(_s16(raw_temp) * 0.0625, 1),
        "ph": round(raw_ph / 100, 2),
        "orp_mv": round(_s32(raw_orp) / 100, 2),
        "tds_ppm": round(raw_tds / 100, 2),
        "turb_ntu": round(raw_turb / 100, 2),
    }


def _parse_gps(frame: bytes) -> dict:
    (_head, _length, _cid, _sid, _type, _utc_year, bj_year, speed_x100,
     lat, lon, alt,
     _um, _ud, _uh, _umin, _us,
     bj_mon, bj_day, bj_hour, bj_min, bj_sec,
     fix_quality, num_sat, is_valid, _ret, _crc, _end) = _GPS_FMT.unpack(frame)
    return {
        "lat": round(lat / 1e6, 6),
        "lon": round(lon / 1e6, 6),
        "alt_m": round(alt / 100, 1),
        "speed_kmh": round(speed_x100 / 100, 2),
        "fix_quality": fix_quality,
        "satellites": num_sat,
        "valid": bool(is_valid),
        "bj_time": f"{bj_year:04d}-{bj_mon:02d}-{bj_day:02d} "
                   f"{bj_hour:02d}:{bj_min:02d}:{bj_sec:02d}",
    }


def _validate(kind: str, frame: bytes) -> str | None:
    """Structural check. Returns a reason string to reject, None to accept."""
    if len(frame) != FRAME_LEN:
        return f"bad length {len(frame)}"
    head, = struct.unpack_from("<H", frame, 0)
    ftype, = struct.unpack_from("<H", frame, 8)
    end, = struct.unpack_from("<H", frame, FRAME_LEN - 2)
    if head != FRAME_HEAD:
        return f"bad head 0x{head:04X}"
    if end != FRAME_END:
        return f"bad end 0x{end:04X}"
    if ftype != _EXPECTED_TYPE[kind]:
        # ctrl 板把所有非 GPS 帧都发到 water 主题（连接应答等也会出现）——静默跳过
        return f"type {ftype} (expected {_EXPECTED_TYPE[kind]})"
    return None


class SensorStore:
    """Latest-value cache + JSONL persistence for water / GPS frames.

    handle_frame() is called on the asyncio loop; parsing is a struct.unpack
    and the log append is a one-line local write, so no executor needed.
    """

    def __init__(self):
        self._latest: dict[str, dict | None] = {"water": None, "gps": None}
        self._parsers = {"water": _parse_water, "gps": _parse_gps}
        self._log_paths = {
            kind: os.path.join(config.SENSOR_LOG_DIR, f"{kind}.jsonl")
            for kind in self._latest
        }
        os.makedirs(config.SENSOR_LOG_DIR, exist_ok=True)
        self._warm_start()

    # ── Ingest ───────────────────────────────────────────────────────────────

    def handle_frame(self, kind: str, payload: bytes) -> None:
        reason = _validate(kind, payload)
        if reason is not None:
            logger.debug("Dropping %s frame: %s", kind, reason)
            return

        crc_rx, = struct.unpack_from("<H", payload, FRAME_LEN - 4)
        if crc_rx != crc16_ccitt(payload[:FRAME_LEN - 4]):
            logger.warning("CRC mismatch on %s frame (accepting)", kind)

        try:
            data = self._parsers[kind](payload)
        except struct.error as e:
            logger.warning("Failed to parse %s frame: %s", kind, e)
            return

        record = {
            "ts": round(time.time(), 3),
            "time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "data": data,
        }
        self._latest[kind] = record
        self._append_log(kind, record)
        logger.debug("Sensor %s updated: %s", kind, data)

    def _append_log(self, kind: str, record: dict) -> None:
        try:
            with open(self._log_paths[kind], "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        except OSError as e:
            logger.error("Sensor log write failed (%s): %s", kind, e)

    def _warm_start(self) -> None:
        """Rebuild the cache from the newest log line (MCU publishes no retain)."""
        for kind, path in self._log_paths.items():
            record = _read_last_record(path)
            if record:
                self._latest[kind] = record
                logger.info("Sensor %s warmed from log (age %.0fs)",
                            kind, time.time() - record["ts"])

    # ── LLM context ──────────────────────────────────────────────────────────

    def prompt_context(self) -> str:
        """Compact Chinese context block for the system prompt.

        Always returns the block (with 暂无数据 placeholders before the first
        frame arrives) — the filler clip promises a data lookup, so the LLM
        must know it is a boat assistant even when the sensors are silent.
        """
        water = self._describe("water", self._water_text)
        gps = self._describe("gps", self._gps_text)
        lines = ["【船载实时数据】"]
        lines.append("水质：" + (water or "暂无数据（尚未收到传感器上报）"))
        lines.append("定位：" + (gps or "暂无数据（尚未收到定位上报）"))
        lines.append("回答涉及水质或位置时请依据以上数据；"
                     "数值请口语化播报，坐标不必逐位念完整小数。")
        return "\n".join(lines)

    def _describe(self, kind: str, to_text) -> str | None:
        record = self._latest[kind]
        if record is None:
            return None
        age = max(0.0, time.time() - record["ts"])
        text = f"（{_age_text(age)}更新）{to_text(record['data'])}"
        if age > config.SENSOR_STALE_SECONDS:
            text += "。注意：该数据已较久未更新，可能不是最新情况"
        return text

    @staticmethod
    def _water_text(d: dict) -> str:
        return (f"水温 {d['temp_c']}℃、pH {d['ph']}、ORP {d['orp_mv']}mV、"
                f"TDS {d['tds_ppm']}ppm、浊度 {d['turb_ntu']}NTU")

    @staticmethod
    def _gps_text(d: dict) -> str:
        if not d["valid"] or d["fix_quality"] == 0:
            return f"GPS 尚未定位成功（可见卫星 {d['satellites']} 颗）"
        ns = "北纬" if d["lat"] >= 0 else "南纬"
        ew = "东经" if d["lon"] >= 0 else "西经"
        return (f"{ns} {abs(d['lat'])} 度、{ew} {abs(d['lon'])} 度、"
                f"海拔 {d['alt_m']} 米、速度 {d['speed_kmh']} 公里每小时、"
                f"卫星 {d['satellites']} 颗")


def _age_text(age: float) -> str:
    if age < 60:
        return f"{int(age)}秒前"
    if age < 3600:
        return f"{int(age / 60)}分钟前"
    return f"{age / 3600:.1f}小时前"


def _read_last_record(path: str, tail_bytes: int = 8192) -> dict | None:
    """Read the newest valid JSONL record from the file tail."""
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            f.seek(max(0, f.tell() - tail_bytes))
            lines = f.read().decode("utf-8", errors="replace").splitlines()
    except OSError:
        return None
    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
            if isinstance(record, dict) and "ts" in record and "data" in record:
                return record
        except json.JSONDecodeError:
            continue
    return None
