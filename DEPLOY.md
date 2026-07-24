# 部署指南 —— Ubuntu 24.04 / Python 3.12

目标服务器: 39.105.17.91（EMQX 已在 1883 端口运行）。

## 1. 上传代码

本机（Windows，任选其一）:

```powershell
# 方式 A: scp 整个目录（排除测试 pcm 大文件）
scp -r D:\voiceServer ubuntu@39.105.17.91:/opt/voiceServer

# 方式 B: 服务器上 git clone（若已推到仓库）
```

⚠️ `.env` 含密钥——确认它一并上传，且**不要**提交到公开仓库。

## 2. 服务器上安装

```bash
cd /opt/voiceServer
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## 3. 核对 .env

必填: `MQTT_HOST`(127.0.0.1，本机 broker)、`MQTT_PORT`(1883)、`MQTT_USERNAME/PASSWORD`、
`XUNFEI_APPID/API_KEY/API_SECRET`、`LLM_BASE_URL/API_KEY/MODEL`、`TTS_BASE_URL/API_KEY/VOICE`。
可调: `DOWN_BURST_SECONDS`(默认 1.0，勿超过 1.5——MCU 环形缓冲 64KB=2s)、
`LLM_MAX_TOKENS`(默认 300)、`TTS_STREAM`(默认 1=流式合成；网关流式坏了设 0 回退整段合成)、
`STOP_GRACE_SECONDS`(默认 0.3，仅批量 ASR 回退路径使用)。

## 4. 先手动跑通

```bash
cd /opt/voiceServer
.venv/bin/python test_apis.py          # 单测三个云 API（看 ASR/LLM/TTS 各自耗时）
.venv/bin/python test_stream_e2e.py    # 流式全链路: 流式ASR→分句→流式TTS首块计时
.venv/bin/python probe_latency.py      # 网关体检: 模型列表/TTFT/TTS流式支持（延时变差时先跑它）
.venv/bin/python gen_filler.py         # 生成垫场语音 filler.pcm（一次即可；换文案/音色后重跑）
.venv/bin/python server.py             # 前台跑，板子开机做一轮问答，看日志
```

期望日志顺序:
`MQTT connected` → `Session created: <N>` → `Stop received, 24 chunks` → `Sending filler clip`
→ `ASR (stream, 0.3s): 你说的话` → `LLM first token in X.XXs` → `TTS: 首句…` → `Session done`。

## 4.5 船载传感器数据（水质/GPS → LLM 提示词）

服务器额外订阅 `/qhmu/lele/mcu/water` 和 `/qhmu/lele/mcu/gps`（ctrl 板原样转发的
64 字节小端帧，见 `sensors.py` 头注释）。最新值进内存缓存并追加到 `data/*.jsonl`；
LLM 每次请求把缓存注入 system prompt（超过 `SENSOR_STALE_SECONDS`=300s 标注过期）。
MCU 发布不带 retain，重启后从日志尾行回填缓存，无需等下一次上报。
注意 ctrl 板把**所有非 GPS 帧**都发往 water 主题（含连接应答等），服务器按帧内
type 字段过滤，日志出现 `Dropping water frame: type N` 属正常。

## 4.6 垫场语音（感知延迟优化）

收到 stop 的瞬间即下发预合成的 `filler.pcm`（"好的，我先查询水质和定位数据…"），
真实回答排在其后无缝衔接——用户 1 秒内听到回应，而不是静默等 4~8s。
`gen_filler.py` 用与回答相同的音色合成（本地 Windows 网络下整段合成慢，
需 `API_TIMEOUT=90` 环境变量；服务器上默认值即可）。文件缺失仅告警不影响运行；
`FILLER_ENABLED=0` 可临时关闭。文案改短 = 好情况下更早听到真实回答。

## 5. 注册 systemd 服务（开机自启+崩溃自拉，定名 voice-server）

⚠️ 先停掉手动跑的 `python server.py`（Ctrl+C），否则两个实例重复订阅。
⚠️ 若装过旧名 `voiceserver`（无连字符）先清掉：`sudo systemctl disable --now voiceserver;
sudo rm -f /etc/systemd/system/voiceserver.service; sudo systemctl daemon-reload`。

仓库里的 `voice-server.service` 已按本机环境写好（用户 mcl、目录 ~/voiceServer），直接安装：

```bash
sudo cp ~/voiceServer/voice-server.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now voice-server
systemctl status voice-server --no-pager    # 期望 Active: active (running)
journalctl -u voice-server -f               # 实时日志
```

## 6. 常见问题

| 现象 | 排查 |
|---|---|
| `MQTT connect failed` | EMQX 是否运行 `systemctl status emqx`；用户名密码与 MCU 侧一致 |
| 板子收不到音频 | EMQX Dashboard 看两个客户端是否都在线；主题拼写（前导 `/` 必须一致） |
| `LLM first token in >8s` | 上游网关慢/抖动——跑 `probe_latency.py` 对比；必要时换 `LLM_BASE_URL`（实测本网关只有 MiMo 系，pro 已是最快，换模型无益） |
| 日志出现 `TTS stream failed pre-audio` | 网关流式挂了，已自动回退整段合成（变慢但能用）；持续如此设 `TTS_STREAM=0` 并跑 probe 确认 |
| 日志出现 `ASR stream failed ... falling back to batch` | 讯飞 WS 连不上/断线，自动回退批量识别（多 ~1.3s）；看服务器到 ws-api.xfyun.cn 的连通性 |
| TTS 偶发 503 | 网关限流——流水线已限并发为"一步预取"，若仍频繁可加重试或换 TTS_BASE_URL |
| MCU 播放断续 | `DOWN_BURST_SECONDS` 过大溢出环形缓冲（调回 1.0）或 WiFi 拥塞 |

## 7. 延时账本（2026-07-22 流式化改造后）

一轮问答（从板端 stop 到听到声音）= **ASR 收尾 ~0.3s + LLM 首 token 1.5~6s(网关抖动) +
首单元出句 ~0.5s + TTS 流式首块 ~1.5s + 下行传输 <0.5s ≈ 4~8s**（改造前 12~14s）。

已做的流水线化（都在 server 代码里，无需配置）:
① **流式 TTS**（最大杠杆，省 3~5s）: 边合成边下发，首块 ~1.5s 到（整段合成实测 15字/5s、28字/9s）；
② **流式 ASR**（省 ~1.3s）: 录音期间边收边喂讯飞，stop 即拿最终文本（`ASR (stream, 0.3s)`）；
③ **首单元逗号切分**（省 ~0.5s）: 第一个 TTS 单元在第一个逗号处就开始合成，后续仍按整句保韵律。

**剩余瓶颈 = LLM 首 token**（实测同一网关 TTFT 在 1.5~10s 间抖动，模型已是最快的 mimo-v2.5-pro）。
要稳定进 5s 内，唯一有效手段是换更快的 LLM 网关/服务商（`LLM_BASE_URL`+`LLM_API_KEY`），
换之前先用 `probe_latency.py` 实测候选网关的 TTFT。
