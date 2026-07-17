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
可调: `DOWN_BURST_SECONDS`(默认 0.6，勿超过 0.8——MCU 环形缓冲只有 1s)、
`LLM_MAX_TOKENS`(默认 300)、`ASR_FRAME_INTERVAL_MS`(默认 10)。

## 4. 先手动跑通

```bash
cd /opt/voiceServer
.venv/bin/python test_apis.py     # 单测三个云 API（看 ASR/LLM/TTS 各自耗时）
.venv/bin/python server.py        # 前台跑，板子开机做一轮问答，看日志
```

期望日志顺序:
`MQTT connected` → `Session created: <N>` → `stop: 24 chunks` → `ASR (2.x s): 你说的话`
→ `LLM first token in X.XXs` → `TTS #0 ...` → `done: M packets`。

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
| `LLM first token in >8s` | 上游网关慢——换 `LLM_BASE_URL`/`LLM_MODEL`（这是首响应延时的最大变量） |
| TTS 偶发 503 | 网关限流——流水线已限并发为"一步预取"，若仍频繁可加重试或换 TTS_BASE_URL |
| MCU 播放断续 | `DOWN_BURST_SECONDS` 过大溢出环形缓冲（调回 0.6）或 WiFi 拥塞 |

## 7. 延时账本（哪里还能压）

一轮问答 = 录音(用户说话时长) + ASR(~2s) + **LLM 首 token(1~12s，取决于网关/模型)** +
TTS 首句(~2.5s) + 下行传输(<0.5s)。后续句子已流水线化，边播边合成，无额外等待。
压延时优先级: ① 换快的 LLM 网关/模型（看 `LLM first token` 日志数值）；
② TTS 若支持流式改流式；③ ASR 改边录边传的流式识别（板端已按分片上传，服务器可改为
收到分片即喂讯飞，stop 时立即拿结果，可再省 ~2s——留作下一步优化）。
