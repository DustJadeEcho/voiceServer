# voiceServer — WM8978G + BearPi-Pico H3863 语音问答云端

MQTT 收 MCU 录音 → 讯飞 ASR → LLM（流式分句）→ MiMo TTS（逐句合成）→ 限速下发 PCM。
设计文档见 `Lele.md`；**协议与板端 `voiceTest/inc/mqtt_connect.h` 保持一致**。

## 协议速览（全小端）

- 音频包：`5A5A | type(LE16=1) | len(LE32) | session(LE32) | seq(LE32) | PCM | CRC16-CCITT(LE) | 6B6B`
  （CRC = poly 0x1021 / init 0xFFFF，覆盖 session+seq+PCM；下行每包 PCM 4000B = 125ms）
- 主题：
  - 上行音频 `/qhmu/lele/mcu/audio/pcm/up`（QoS0）
  - 上行控制 `/qhmu/lele/mcu/audio/wm8978/control`（QoS1）`{"event":"start","session":N,"max_duration":4}` / `{"event":"stop","session":N}`
  - 下行音频 `/qhmu/lele/mcu/audio/pcm/down`（QoS0）
  - 下行控制 `/qhmu/lele/mcu/audio/ubuntu/control`（QoS1）`{"event":"done","session":N}` / `{"event":"error","session":N,"reason":"..."}`
- 音频格式：16kHz / 16bit / mono / s16le

## 关键机制

- **下行限速**（session.py）：先突发 `DOWN_BURST_SECONDS`（默认 0.6s）预填 MCU 的 32KB(1s) 环形缓冲，之后按实时速率发——答案再长也不会溢出丢字。
- **TTS 流水线**：第 N 句在下发时，第 N+1 句已在后台合成。
- **stop 宽限**（`STOP_GRACE_SECONDS`=0.3s）：stop 走 QoS1 可能超车最后几片 QoS0 音频，组包前稍等。
- 容错矩阵按 Lele.md §8：能发多少发多少，结尾必有 done 或 error。

## 运行

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
# .env 填 MQTT/讯飞/LLM/TTS 密钥（权限 600）
python server.py
```

.env 必填：`MQTT_HOST MQTT_USERNAME MQTT_PASSWORD XUNFEI_APPID XUNFEI_API_KEY XUNFEI_API_SECRET LLM_API_KEY TTS_API_KEY`；
可选：`LLM_BASE_URL LLM_MODEL TTS_VOICE DOWN_BURST_SECONDS ASR_FRAME_INTERVAL_MS LOG_LEVEL` 等（见 config.py）。

## systemd 部署

`/etc/systemd/system/voiceserver.service`：

```ini
[Unit]
Description=Voice QA server (MQTT + ASR/LLM/TTS)
After=network-online.target emqx.service
Wants=network-online.target

[Service]
User=water
WorkingDirectory=/home/water/voiceServer
ExecStart=/home/water/voiceServer/.venv/bin/python server.py
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload && sudo systemctl enable --now voiceserver
journalctl -u voiceserver -f
```

## 联调提示

- 单测三个 API：`python test_apis.py`
- 抓上行流量：`mosquitto_sub -h 127.0.0.1 -u qhmu -P *** -t '/qhmu/lele/mcu/audio/pcm/up' --hex -v`
- 板端日志看 `[mqtt] ctrl >>` 与 `seq gap` / `crc mismatch` 告警可定位格式不一致。
