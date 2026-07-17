# voiceServer — WM8978G + BearPi-Pico H3863 语音问答云端

MQTT 收 MCU 录音 → 讯飞 ASR → LLM（流式分句）→ MiMo TTS（逐句流水线）→ 令牌桶限速下发 PCM。
**✅ 2026-07-17 与板端联调打通，连续多轮问答稳定（40s+ 长回答无丢包）。**
设计文档见 `Lele.md`；**协议与板端 `voiceTest/inc/mqtt_connect.h` 保持一致**
（板端全套硬件/协议/踩坑记录见 `d:\voiceTest\application\samples\voiceTest\README.md`）；
部署步骤详见 `DEPLOY.md`。

## 协议速览（全小端）

- 音频包：`5A5A | type(LE16=1) | len(LE32) | session(LE32) | seq(LE32) | PCM | CRC16-CCITT(LE) | 6B6B`
  （CRC = poly 0x1021 / init 0xFFFF，覆盖 session+seq+PCM；每包 PCM 4000B = 125ms，整包 4020B）
- 主题：
  - 上行音频 `/qhmu/lele/mcu/audio/pcm/up`（QoS0）
  - 上行控制 `/qhmu/lele/mcu/audio/wm8978/control`（QoS1）`{"event":"start","session":N,"max_duration":4}` / `{"event":"stop","session":N}`
  - 下行音频 `/qhmu/lele/mcu/audio/pcm/down`（QoS0）
  - 下行控制 `/qhmu/lele/mcu/audio/ubuntu/control`（QoS1）`{"event":"done","session":N}` / `{"event":"error","session":N,"reason":"..."}`
- 音频格式：16kHz / 16bit / mono / s16le；session 为 u32 数字（板端生成，服务器原样回填）

## 关键机制（均为实测教训的产物）

- **封顶令牌桶限速**（session.py）：桶容量 = `DOWN_BURST_SECONDS`（默认 0.6s），
  按实时速率补充。⚠️ 不能用"突发+经过时间×速率"的开放预算——网络卡顿期间预算照涨，
  恢复后 TCP 洪峰会灌爆 MCU 的 64KB(2s) 环形缓冲（实测卡 4s → 丢 0.5s 音频）。
- **TTS 逐句流水线**（queue maxsize=2 反压）：LLM 每出一句立刻排队合成，
  **首句合成完成即下发**（不等第二句出现）；同时最多 2 句在合成，兼顾网关限流。
- **stop 宽限**（`STOP_GRACE_SECONDS`=0.3s）：stop 走 QoS1 可能超车最后几片 QoS0 音频，组包前稍等。
- **单设备策略**：收到新 start 即作废所有旧会话——板端超时换会话后，
  旧回复不再继续下发（板端也会按 session 号丢弃 stale 包，双保险）。
- 容错矩阵按 Lele.md §8：能发多少发多少，结尾必有 done 或 error；
  TTS 单句失败跳过、连续 2 句失败中止。

## 延时账本（实测）

```text
首响应 10.6~12.7s = ASR 1.1s + LLM 首 token 4.2~5.5s + 首句 TTS ~1s + 网络 2~5s
优化杠杆排序: ① 换更快的 LLM 模型/网关（看日志 "LLM first token in X.XXs" 横评）
             ② 网络环境（手机热点抖动大，路由器好很多）
             ③ 流式 ASR（板端已分片上传，服务器改"边收边喂讯飞"可再省 ~2s，未做）
后续句子已流水线化，长回答播放无额外等待。
```

## 运行

```bash
python3.12 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
# .env 填 MQTT/讯飞/LLM/TTS 密钥（权限 600）
python server.py
```

.env 必填：`MQTT_HOST MQTT_USERNAME MQTT_PASSWORD XUNFEI_APPID XUNFEI_API_KEY XUNFEI_API_SECRET LLM_API_KEY TTS_API_KEY`；
可选：`LLM_BASE_URL LLM_MODEL LLM_MAX_TOKENS TTS_VOICE DOWN_BURST_SECONDS ASR_FRAME_INTERVAL_MS LOG_LEVEL` 等（见 config.py）。
⚠️ `DOWN_BURST_SECONDS` 勿超过 0.8——MCU 环形缓冲 64KB(2s)，突发+在途量必须留足余量。

## systemd 部署（定名 voice-server；本机环境: 用户 mcl，目录 ~/voiceServer）

⚠️ 注册前先停掉手动跑的 `python server.py`（Ctrl+C）——两个实例会重复订阅处理。
⚠️ 若之前装过旧名 `voiceserver`（无连字符），先清掉，避免双实例：

```bash
systemctl list-units --all 'voice*'        # 查重
sudo systemctl disable --now voiceserver 2>/dev/null
sudo rm -f /etc/systemd/system/voiceserver.service
sudo systemctl daemon-reload
```

安装（仓库里的 `voice-server.service` 即最终版，scp 上去后）：

```bash
sudo cp ~/voiceServer/voice-server.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now voice-server
systemctl status voice-server --no-pager   # 期望 Active: active (running)
```

单元文件要点（见 `voice-server.service`）：

- `WorkingDirectory=/home/mcl/voiceServer`——config.py 靠 `load_dotenv()` 从当前目录读 `.env`；
- `ExecStart` 用 venv python 的**绝对路径**；
- `Restart=always` + `RestartSec=3`，另有 60s/10 次崩溃循环保护
  （broker 未起时的初始连接失败靠 `After=emqx.service` 的开机顺序规避；
  运行期 broker 重启由 paho 自动重连兜底，进程不退出）。

常用命令：

```bash
journalctl -u voice-server -f          # 实时日志（等价于原来前台跑的输出）
journalctl -u voice-server -n 100      # 最近 100 行
sudo systemctl restart voice-server    # 改完代码/.env 后重启
sudo systemctl disable --now voice-server   # 停用并取消自启
```

排障速查：

- `status` 显示 `code=exited, status=203/EXEC` → ExecStart 路径不对（venv python 不存在）；
- `status=1/FAILURE` 且日志见 `Missing required env vars` → `.env` 不在 WorkingDirectory 或缺键；
- 反复重启后 `start-limit-hit` → 60s/10 次保护触发，修好根因后 `sudo systemctl restart voice-server`；
- 起来了但板子没反应 → `journalctl -u voice-server -n 50` 看是否 `MQTT connected`+`Subscribed`。

## 联调提示（含踩过的坑）

- 单测三个 API：`python test_apis.py`
- 抓上行流量：`mosquitto_sub -h 127.0.0.1 -u qhmu -P *** -t '/qhmu/lele/mcu/audio/pcm/up' --hex -v`
- 板端日志看 `[mqtt] ctrl >>/<<`、`audio << pkt#N ... ring=水位 drop=丢弃数`、
  `seq gap` / `crc mismatch` 告警可定位格式不一致。
- **跨机日志钟差 ~8-9s**：板端串口时间戳（PC 时钟）与服务器时钟不能直接对表，
  只能各自机内比较；判定"谁慢了"要靠同机的请求-响应对（如 PUBLISH→PUBACK）。
- 板端 paho 的逐包 trace 会灌爆 115200 串口并拖慢收包线程——板端已用
  `MQTTClient_setTraceLevel(TRACE_ERROR)` 压掉；若看到板端收包整体滞后先查这个。
- `drop` 持续增长 = 下发超过板端消化能力：检查 `DOWN_BURST_SECONDS` 是否被调大、
  或是否绕过了令牌桶直接下发。
- 会话日志形态（健康样例）：`Session created: N` → `Stop received, 24 chunks` →
  `ASR (1.1s): 问题文本` → `LLM first token in X.XXs` → `TTS #k ...` → `done: M packets`。
