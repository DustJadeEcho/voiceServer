# voiceServer

面向 WM8978G + BearPi-Pico H3863 设备的 MQTT 语音问答服务。设备端采集
16 kHz PCM 并通过 MQTT 上传；服务端在录音期间进行讯飞流式 ASR，在收到
`stop` 后结合船载水质/GPS 数据调用 OpenAI 兼容 LLM，再将 MiMo TTS 的音频以
16 kHz PCM 分包、限速下发。

当前入口为 [`server.py`](server.py)。音频封包必须与设备端
`voiceTest/inc/mqtt_connect.h` 保持一致；详细设计记录见
[`Lele.md`](Lele.md)。

## 功能概览

- 录音期间建立并持续喂送讯飞 WebSocket ASR，正常情况下 `stop` 后只需等待最终识别结果；流式 ASR 建连或收尾失败时，自动以已缓存的 PCM 回退到批量 ASR。
- LLM 使用 OpenAI 兼容的流式接口。首个可播放单元可在逗号、分号、冒号等位置提前切出，后续以完整句为主；小数点和数字后的英文句点不会被错误切句。
- TTS 支持 SSE 流式音频：接收 24 kHz PCM16LE 后持续重采样到设备所需的 16 kHz PCM16LE，再按顺序下发。流式 TTS 在首个音频块前失败时会自动尝试一次非流式合成。
- 可选垫场音 `filler.pcm`：收到 `stop` 即开始播放，真实回答严格排在其后，降低用户的静默等待感。
- 订阅水质和 GPS 原始帧，将最新数据写入 `data/*.jsonl` 并注入每次 LLM 请求的 system prompt；重启后可从日志恢复缓存。
- 下行采用容量受限的令牌桶，避免网络恢复后的突发流量压满 MCU 约 64 KB 的播放环形缓冲。
- MQTT 断线由 paho 自动重连；会话、PCM 大小、处理时长均有限制，并向设备返回明确的 `done` 或 `error` 控制消息。

## 数据流

```text
按键 start / PCM / stop
        │
        ▼
  MQTT Broker ──► server.py ──► 流式 ASR ──► 流式 LLM ──► 流式 TTS
        ▲              │                                      │
        │              ├── 水质 / GPS 缓存 ──► LLM 上下文      ▼
        │              └── filler.pcm ──► 受限令牌桶 ──► PCM / done / error
        │
      MCU 设备
```

一次正常会话的实际顺序如下：

1. MCU 发布 `start`。服务端创建会话并异步建立 ASR WebSocket。
2. MCU 以音频包上传 PCM。服务端校验封包结构，将 PCM 按 `seq` 缓存，同时非阻塞地送入流式 ASR。
3. MCU 发布 `stop`。若启用垫场音，服务端立即开始下发垫场音；随后完成流式 ASR，失败时才等待短暂宽限并走批量 ASR。
4. 识别文本连同最新水质/GPS 上下文进入 LLM 流。每个可播放单元进入 TTS，TTS 音频按生成顺序、经过限速后发给 MCU。
5. 所有音频发完后发送 `{"event":"done"}`；发生不可恢复错误时发送 `{"event":"error", "reason":"..."}`。

> 当前是单设备策略：新的 `start` 会取消所有其他会话，防止设备已切换 session 后旧回答继续占用带宽。`MAX_SESSIONS` 仍保留为保护上限。

## MQTT 协议

所有整数均为小端序。设备音频格式固定为 **16 kHz / 16-bit / mono / signed PCM little-endian**。

### 音频封包

| 字段 | 字节数 | 说明 |
| --- | ---: | --- |
| `head` | 2 | 固定 `5A 5A` |
| `type` | 2 | `uint16 LE`，PCM 固定为 `1` |
| `len` | 4 | `uint32 LE`，`session + seq + PCM` 的长度，即 `8 + PCM 字节数` |
| `session` | 4 | `uint32 LE`，由设备生成，服务端下行原样回填 |
| `seq` | 4 | `uint32 LE`，每个方向各自计数；下行从 `0` 开始，垫场音和回答共用连续序号 |
| `PCM` | 可变 | 16 kHz / 16-bit / mono / s16le 数据 |
| `crc16` | 2 | `uint16 LE`，CRC16-CCITT，`poly=0x1021`、`init=0xFFFF`，覆盖 `session + seq + PCM` |
| `end` | 2 | 固定 `6B 6B` |

下行通常每包放入 `4000` B PCM，即 `125 ms` 音频；完整包为 `4020` B，最后一包可以更短。

结构错误（包头、包尾、类型、长度）会直接丢弃。CRC 不匹配会记录告警但仍继续处理，因此设备端仍应保证 CRC 正确。上行包会按 `seq` 排序拼接；检测到序号缺口时会写入告警日志。

### 主题与 QoS

| 方向 | 主题 | QoS | 载荷 |
| --- | --- | ---: | --- |
| 上行 | `/qhmu/lele/mcu/audio/pcm/up` | 0 | 上述 PCM 封包 |
| 上行 | `/qhmu/lele/mcu/audio/wm8978/control` | 1 | `start` / `stop` JSON |
| 下行 | `/qhmu/lele/mcu/audio/pcm/down` | 0 | 上述 PCM 封包 |
| 下行 | `/qhmu/lele/mcu/audio/ubuntu/control` | 1 | `done` / `error` JSON |
| 上行 | `/qhmu/lele/mcu/water` | 0 | 水质原始 64 B 帧 |
| 上行 | `/qhmu/lele/mcu/gps` | 0 | GPS 原始 64 B 帧 |

控制消息示例：

```json
{"event":"start","session":12345,"max_duration":4}
{"event":"stop","session":12345}
{"event":"done","session":12345}
{"event":"error","session":12345,"reason":"asr_failed"}
```

`session` 应为非零 `uint32`。`start.max_duration` 缺省值为 `60`；当前实现将其传入会话并记录日志，但不以该字段强制截断录音，录音时长仍应由设备端控制。服务端的全局会话超时由 `SESSION_TIMEOUT` 控制。

`stop` 使用 QoS 1，而音频使用 QoS 0，网络中二者可能乱序。流式 ASR 路径已经按收到顺序喂入 PCM；只有批量 ASR 回退路径会额外等待 `STOP_GRACE_SECONDS`，以接收可能晚到的音频包。

### 控制错误码

| `reason` | 触发条件 |
| --- | --- |
| `duplicate_session` | 收到已存在 session 的第二个 `start` |
| `max_sessions_reached` | 创建会话时达到 `MAX_SESSIONS` 上限 |
| `no_audio` | 批量 ASR 回退时没有可用 PCM |
| `asr_failed` | 流式 ASR 失败后，批量 ASR 也失败 |
| `empty_speech` | ASR 返回空文本 |
| `tts_failed` | 连续两段 TTS 失败，终止本次会话 |
| `llm_failed` | LLM 无内容且兜底语音也无法生成，或 LLM 在产出音频前失败 |
| `llm_stream_interrupted` | LLM 已产出部分音频后流中断；设备可能已收到前半段回答 |
| `timeout` | 会话处理超过 `SESSION_TIMEOUT` |
| `internal_error` | 未分类的服务端异常 |

未知 session 的 `stop`、格式错误的控制消息和结构错误的音频包仅记录日志，不会回发控制消息。重复 `stop` 在已有处理任务时会被忽略。

## 流式与容错策略

### 延迟路径

服务端不把固定秒数作为 SLA，实际时延主要受 LLM 首 token、云端网络和 TTS 网关影响。可用日志中的 `LLM first token in X.XXs` 以及 `probe_latency.py` 比较不同网关或模型。

- **流式 ASR**：`start` 时先连讯飞，录音片段到达即排队发送；`stop` 时只等待最终识别结果。连接或流式收尾异常时，使用已缓存 PCM 走批量识别。
- **首单元提前切分**：首个 LLM 播放单元可在足够长的逗号、分号、冒号等处切出，让 TTS 更早启动；后续单元按句末切分。数字小数点不会被视为断句符。
- **流式 TTS**：默认 `TTS_STREAM=1`，从 SSE 中逐块读取 base64 音频并连续重采样 `24 kHz -> 16 kHz`。当网关无法在首块音频前完成流式输出时，自动改用该句的非流式 TTS。
- **垫场音**：`filler.pcm` 存在且启用时，`stop` 后立即下发。真实回答会等待垫场音全部发布后再发，以保持 MCU 播放顺序。
- **LLM 空流**：若一次 LLM 流没有形成任何音频，服务端重试一次；连续两次空流会合成固定兜底句“抱歉，我刚才没有组织好语言，请再问我一次吧。”。
- **部分结果优先**：单句 TTS 失败会跳过该句；连续两句失败才终止。LLM 在已经产出部分内容后中断时，已下发的音频会保留，随后发送 `llm_stream_interrupted`。

### 下行节流

`session.py` 用容量封顶的令牌桶限制下行速率：桶容量为
`DOWN_BURST_SECONDS * 32000` 字节，初始允许一个小突发，之后按 32,000 B/s 的实时音频速率补充。令牌数量始终被封顶，因此网络卡顿期间不会积累可在恢复后一次性灌入 MCU 的“补偿额度”。

默认 `DOWN_BURST_SECONDS=1.0`。代码不会对该值施加硬上限；调整时应观察设备端环形缓冲水位和 `drop` 计数，不能只依据服务端吞吐量判断。设备播放缓冲约为 64 KB（约 2 秒），突发量还需给在途 MQTT/TCP 数据留出余量。

## 船载传感器上下文

`sensors.py` 订阅控制板原样转发的 64 B 小端帧，校验包头、包尾与帧内 `type` 后解析。水质和 GPS 的 CRC 不匹配同样会告警后继续接受。

| 数据 | 解析后的主要字段 | 处理方式 |
| --- | --- | --- |
| 水质 | 水温、pH、ORP（mV）、TDS（ppm）、浊度（NTU） | 温度按 `raw * 0.0625`，其余测量值按 `raw / 100` 缩放 |
| GPS | 纬度、经度、海拔、速度、定位质量、卫星数、北京时间 | 经纬度按 `raw / 1e6`，海拔和速度按 `raw / 100` 缩放 |

每个合法帧都会更新内存缓存并追加到 `SENSOR_LOG_DIR`（默认项目根目录下的 `data/`）中的 `water.jsonl` 或 `gps.jsonl`。MQTT 上报未使用 retain，服务启动时会从每个 JSONL 的最后一条有效记录恢复缓存。超过 `SENSOR_STALE_SECONDS`（默认 `300` 秒）的数据仍会提供给 LLM，但 prompt 会明确标注可能已过期。

控制板会把所有非 GPS 帧发往 water 主题，因此 water 主题中出现 `Dropping water frame: type N` 的调试日志通常表示帧类型过滤，未必是故障。

## 环境要求

- Python **3.10+**，推荐与部署单元和 `requirements.txt` 一致的 Python 3.12。
- 可访问的 MQTT Broker；默认协议为 TCP `1883`，提供 `MQTT_CA_CERT` 后才启用 TLS。
- 讯飞语音听写（WebSocket）凭据。
- 支持 OpenAI Chat Completions 流式接口的 LLM 网关。
- 支持 MiMo 音频字段及 SSE 音频 delta 的 OpenAI 兼容 TTS 网关。

> 根目录 `.python-version` 当前为 `3.12.13`，请按上面的版本要求创建虚拟环境。

## 安装与启动

项目没有提交可直接复制的 `.env.example`；新环境请在项目根目录创建 `.env`。`.env` 已被 `.gitignore` 忽略，切勿提交密钥。

```bash
cd /path/to/voiceServer
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt
chmod 600 .env
.venv/bin/python server.py
```

最小可用 `.env` 示例如下。MQTT 匿名访问时可省略用户名和密码；若启用 TLS，还应把端口改为 Broker 的 TLS 端口并填写 CA 文件的绝对路径。

```dotenv
# MQTT
MQTT_HOST=127.0.0.1
MQTT_PORT=1883
# MQTT_USERNAME=...
# MQTT_PASSWORD=...
# MQTT_CA_CERT=/absolute/path/to/ca.pem

# iFlytek ASR
XUNFEI_APPID=...
XUNFEI_API_KEY=...
XUNFEI_API_SECRET=...

# OpenAI-compatible LLM
LLM_BASE_URL=https://your-llm-gateway/v1
LLM_API_KEY=...
LLM_MODEL=...

# MiMo-compatible TTS
TTS_BASE_URL=https://your-tts-gateway/v1
TTS_API_KEY=...
TTS_MODEL=mimo-v2.5-tts
TTS_VOICE=冰糖
```

启动时 `config.py` 会检查 `MQTT_HOST`、`XUNFEI_APPID`、`LLM_API_KEY` 和 `TTS_API_KEY` 是否有值；其中 `MQTT_HOST` 即使未设置也会使用默认值 `127.0.0.1`。`XUNFEI_API_KEY` 与 `XUNFEI_API_SECRET` 虽未在启动校验中列出，但缺失时 ASR 鉴权会失败，因此正常部署也必须填写。

### 配置项

以下默认值直接来自 `config.py` 和 `asr.py`：

| 分类 | 变量 | 默认值 | 说明 |
| --- | --- | --- | --- |
| MQTT | `MQTT_HOST` / `MQTT_PORT` | `127.0.0.1` / `1883` | Broker 地址与端口 |
| MQTT | `MQTT_USERNAME` / `MQTT_PASSWORD` | 空 | 可选认证信息 |
| MQTT | `MQTT_CA_CERT` | 空 | 非空时调用 paho TLS；文件须可读 |
| ASR | `ASR_FRAME_INTERVAL_MS` | `10` | 讯飞音频帧的发送节奏；单位毫秒 |
| LLM | `LLM_BASE_URL` | `https://api.openai.com/v1` | OpenAI 兼容接口地址 |
| LLM | `LLM_MODEL` | `gpt-4o-mini` | 聊天模型名称 |
| LLM | `LLM_MAX_TOKENS` | `1000` | 最大 token 数；推理模型的 reasoning token 也会占用此值 |
| LLM | `LLM_SYSTEM_PROMPT` | 内置简洁中文语音助手提示词 | 每次请求会在其后追加传感器上下文 |
| TTS | `TTS_BASE_URL` | `https://api.xiaomimimo.com/v1` | TTS OpenAI 兼容接口地址 |
| TTS | `TTS_MODEL` | `mimo-v2.5-tts` | TTS 模型名称 |
| TTS | `TTS_VOICE` | `冰糖` | 音色名称 |
| TTS | `TTS_STYLE` | 内置“温柔亲切...”风格 | TTS 的风格提示词 |
| TTS | `TTS_STREAM` | `1` | `0`、`false` 或 `no` 时禁用 SSE 流式 TTS |
| 垫场音 | `FILLER_PCM_PATH` | `<项目目录>/filler.pcm` | 预合成的 16 kHz PCM 文件路径 |
| 垫场音 | `FILLER_ENABLED` | `1` | `0`、`false` 或 `no` 时关闭垫场音 |
| 传感器 | `SENSOR_LOG_DIR` | `<项目目录>/data` | JSONL 持久化目录 |
| 传感器 | `SENSOR_STALE_SECONDS` | `300` | 数据被标记为过期前的秒数 |
| 流控 | `DOWN_BURST_SECONDS` | `1.0` | 下行令牌桶初始突发时长 |
| 流控 | `STOP_GRACE_SECONDS` | `0.3` | 仅批量 ASR 回退前等待迟到 QoS 0 音频的秒数 |
| 限制 | `MAX_SESSIONS` | `5` | 会话管理器保护上限 |
| 限制 | `SESSION_TIMEOUT` | `90` | 会话处理超时与陈旧会话清理阈值，单位秒 |
| 限制 | `MAX_PCM_SIZE` | `2097152` | 单会话缓存 PCM 最大字节数（2 MiB） |
| 限制 | `API_TIMEOUT` | `10` | LLM、TTS 和批量 ASR 的 API 超时基准，单位秒 |
| 日志 | `LOG_LEVEL` | `INFO` | Python logging 级别 |

外部凭据变量为 `XUNFEI_APPID`、`XUNFEI_API_KEY`、`XUNFEI_API_SECRET`、`LLM_API_KEY` 和 `TTS_API_KEY`。`LLM_BASE_URL`、`TTS_BASE_URL` 与模型名可使用默认值，也可替换为兼容网关。

## 垫场音

仓库包含可直接使用的 `filler.pcm`。需要更换文案或音色时，使用与正式回答相同的 TTS 配置重新生成：

```bash
.venv/bin/python gen_filler.py
.venv/bin/python gen_filler.py "好的，我正在查询，请稍等。"
```

脚本会写入 `FILLER_PCM_PATH` 指向的 PCM 文件，并额外生成同名 `.wav` 方便试听。服务只在启动时读取垫场音，因此重生成后需要重启 `server.py` 或 `voice-server` 服务。文件缺失、为空或 `FILLER_ENABLED=0` 时功能自动关闭，主语音链路仍可运行。

## 测试与诊断

这些脚本会访问真实云服务，可能产生 API 消耗；运行前确认 `.env` 和 `test_input.pcm` 已准备好。

```bash
# 批量 ASR -> LLM 分句 -> 非流式逐句 TTS，输出 test_output*.pcm
.venv/bin/python test_apis.py

# 流式 ASR -> LLM 分句 -> 流式 TTS，并检查流式重采样边界连续性
.venv/bin/python test_stream_e2e.py

# 探测网关模型列表、LLM 首 token 和 TTS 流式支持情况
.venv/bin/python probe_latency.py
```

`test_apis.py` 会覆盖 `test_output.pcm` 并写入 `test_output_00.pcm` 等分句音频。`test_stream_e2e.py` 没有连接 MQTT Broker，但会调用真实的 ASR、LLM 和 TTS 服务；它首先执行本地重采样断言，然后测试云端流式路径。

健康会话通常可在日志中看到类似顺序：

```text
MQTT connected
Session created: <session>
Recording started
Stop received, ...
Sending filler clip
ASR (stream, ...): <识别文本>
LLM first token in X.XXs
TTS: <首个播放单元>
Session done
```

## systemd 部署

仓库中的 [`voice-server.service`](voice-server.service) 是一个路径示例，默认写死了用户 `mcl` 和目录 `/home/mcl/voiceServer`。安装前必须将以下三项改为实际值：

- `User=`：运行服务的普通用户；
- `WorkingDirectory=`：项目绝对路径；
- `ExecStart=`：该项目虚拟环境中 Python 的绝对路径。

`.env` 应放在项目根目录，并与 `WorkingDirectory` 对应。确认不再有手动运行的 `server.py` 实例后执行：

```bash
sudo cp voice-server.service /etc/systemd/system/voice-server.service
sudo systemctl daemon-reload
sudo systemctl enable --now voice-server
systemctl status voice-server --no-pager
journalctl -u voice-server -f
```

服务单元使用 `Restart=always` 和 `RestartSec=3`。Broker 运行期断开时，paho 会以 1 到 30 秒退避自动重连；首次连接失败时应先检查 Broker、网络和 `.env`，再查看 systemd 日志。

常用操作：

```bash
sudo systemctl restart voice-server      # 修改代码、.env 或 filler.pcm 后
sudo systemctl stop voice-server
sudo systemctl disable --now voice-server
journalctl -u voice-server -n 100 --no-pager
```

## 排障速查

| 现象 | 优先检查项 |
| --- | --- |
| 启动即退出并显示 `Missing required env vars` | `.env` 是否在项目根目录；`MQTT_HOST`、`XUNFEI_APPID`、`LLM_API_KEY`、`TTS_API_KEY` 是否非空 |
| ASR 阶段鉴权或连接失败 | 讯飞 `APPID/API_KEY/API_SECRET`、服务器到 `ws-api.xfyun.cn` 的网络、系统时间是否正确 |
| 日志显示 `ASR stream failed ... falling back to batch` | 流式 ASR 建连或收尾异常；批量回退会继续尝试，重点检查讯飞网络与凭据 |
| `LLM first token in ...` 很慢 | 主要是上游 LLM 网关或模型延迟；运行 `probe_latency.py` 对比候选网关/模型 |
| `TTS stream failed pre-audio` | 该句会自动回退非流式 TTS；持续发生时检查网关 SSE 支持，或设置 `TTS_STREAM=0` |
| 板端没有收到音频 | 确认两个 MQTT 客户端都在线、主题前导 `/` 一致、下行订阅和 session 号一致 |
| 板端 `drop` 持续增长或播放断续 | 从默认 `DOWN_BURST_SECONDS=1.0` 开始，避免增大突发；同时检查 Wi-Fi 与 Broker 链路拥塞 |
| 水质/GPS 回答显示暂无或过期 | 检查对应主题是否有 64 B 原始帧、帧内 type 是否正确、`data/*.jsonl` 是否可写，以及 `SENSOR_STALE_SECONDS` 设置 |

## 项目结构

| 文件 | 作用 |
| --- | --- |
| `server.py` | MQTT 客户端、消息路由、会话管理与进程生命周期 |
| `session.py` | 单次会话状态机、ASR/LLM/TTS 流水线、垫场音与下行限速 |
| `asr.py` | 讯飞 WebSocket 流式/批量 ASR |
| `llm.py` | OpenAI 兼容 LLM 流与播放单元切分 |
| `tts.py` | MiMo TTS 的 SSE 流式与非流式回退、24 kHz 到 16 kHz 重采样 |
| `audio.py` | PCM 封包、CRC、WAV 包装和重采样工具 |
| `sensors.py` | 水质/GPS 帧解析、缓存、JSONL 持久化与 LLM 上下文 |
| `config.py` | 所有环境变量默认值与启动校验 |
| `gen_filler.py` | 生成垫场 PCM/WAV |
| `test_apis.py` | 云 API 批量链路测试 |
| `test_stream_e2e.py` | 云端流式链路和流式重采样测试 |
| `probe_latency.py` | LLM/TTS 网关延迟与能力探测 |
| `voice-server.service` | systemd 服务单元模板 |
