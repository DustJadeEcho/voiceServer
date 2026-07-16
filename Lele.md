# 语音交互系统设计与开发记录

## 1. 硬件架构

| 组件 | 说明 |
|------|------|
| WM8978G | 音频编解码芯片 |
| BearPi-Pico H3863(WS63) | 海思开发板主控 |
| 1 个 Key | Key1 开始录音 / Key1 停止录音 |

**通信：** MQTT（BearPi-Pico H3863 ↔ 云服务器）

**服务器：** 阿里云（Ubuntu 24.04.4 LTS / 2 核 2.5GHz / 1612MB RAM）

## 2. 数据流

```
┌─────────────────────────────────────────────┐
│  BearPi-Pico H3863 端                       │
│                                             │
│  麦克风 → WM8978G → 录音 → S-PCM           │
│                              ↓              │
│                         MQTT 上行(QoS 0)    │
└─────────────────────────────────────────────┘
                     ↓
┌─────────────────────────────────────────────┐
│  云服务器                                    │
│                                             │
│  S-PCM → ASR（语音识别）                     │
│              ↓                              │
│         LLM（大模型问答，流式输出）            │
│              ↓ 分句                          │
│         TTS（分段语音合成）→ D-PCM           │
│                              ↓              │
│                         MQTT 下行(QoS 0)    │
└─────────────────────────────────────────────┘
                     ↓
┌─────────────────────────────────────────────────┐
│  BearPi-Pico H3863 端                           │
│                                                 │
│  D-PCM → WM8978G → 喇叭播放（好像现在暂时在用耳机）│
└─────────────────────────────────────────────────┘
```

## 3. 关键设计决策

- **S-PCM**: 语音模块录制的原始音频数据（16kHz/16bit/mono）
- **D-PCM**: 语音合成后的音频数据（统一为 16kHz/16bit/mono，与 S-PCM 一致。若 TTS API 输出格式不同，服务器端需做重采样和格式转换后再分片下发）
- 中间通过自建服务器处理，调用云端 AI 平台 API
- **边录边传**：设备按 Key1 后立即开始录音并同步通过 MQTT 发送分片，按 Key2 时数据基本已传完
- **LLM 流式输出**：边生成边送给 TTS，每凑够一句（句号/问号/感叹号）就送 TTS 合成
- **TTS 分段合成**：合成完一片就发一片给设备，用户不必等全部合成完
- 音频分片大小 4KB，带 sequence number 防止丢片/乱序
- 控制消息 QoS 1 保证可靠，音频数据 QoS 0 优先低延迟
- MQTT 开启 TLS 加密，使用用户名/密码认证
- 服务器限制并发会话数（最多 5 个），每个会话超时 90 秒自动清理
- 云服务器后台持续运行

## 4. 技术选型

| 项目 | 选择 | 原因 |
|------|------|------|
| 服务器语言 | Python 3.12.13 | 已部署在服务器，MQTT 库成熟、AI API SDK 完善、音频处理方便 |
| 音频格式 | 16kHz/16bit/单声道 | WM8978G 常用配置 |
| MQTT Broker | EMQX 5.8.9 | 已在服务器部署，性能好、功能丰富 |
| ASR 平台 | 讯飞 | 需满足兼容性要求 |
| LLM 平台 | OpenAI 兼容 API | 支持 GPT / DeepSeek / 通义千问等多种模型，流式输出 |
| TTS 平台 | Xiaomi Mimo V2.5 TTS | 目前限时免费、支持输出 PCM 音频、中文效果好，需准备备选方案 |

### ASR 平台选型约束

| 条件 | 要求 |
|------|------|
| 输入格式 | 支持 16kHz/16bit/mono PCM 或 WAV |
| SDK 兼容 | Python 3.12.13 |
| 响应延迟 | < 3 秒 |
| 语言 | 中文 |
| 费用 | 免费或有免费额度 |

## 5. MQTT Topic 设计

| Topic | 方向 | QoS | 说明 |
|-------|------|-----|------|
| `/qhmu/lele/mcu/audio/pcm/up` | 设备→服务器 | 0 | 上行音频 PCM 分片 |
| `/qhmu/lele/mcu/audio/wm8978/control` | 设备→服务器 | 1 | 上行控制信号（start/stop） |
| `/qhmu/lele/mcu/audio/pcm/down` | 服务器→设备 | 0 | 下行合成音频分片 |
| `/qhmu/lele/mcu/audio/ubuntu/control` | 服务器→设备 | 1 | 下行控制信号（done/error） |

## 6. 上下行协议

### 上行（设备→服务器）

**控制消息** (`/qhmu/lele/mcu/audio/wm8978/control`)：
```json
{"event": "start", "session_id": "uuid-xxx", "max_duration": 60}
{"event": "stop",  "session_id": "uuid-xxx"}
```
- `max_duration`：最大录音时长（秒），超时服务器自动结束会话，默认 60 秒

**音频数据** (`/qhmu/lele/mcu/audio/pcm/up`)：

| 偏移 | 长度 | 说明 |
|------|------|------|
| 0-35 | 36 字节 | session_id（UTF-8 编码的 UUID 字符串） |
| 36-39 | 4 字节 | sequence number（uint32，大端序，从 0 开始递增） |
| 40+ | 剩余 | 原始 PCM 数据（16kHz/16bit/mono） |

- 每片总大小 4KB（4096 字节），其中 PCM 数据 4056 字节（约 126.75ms 音频）
- 最后一片可能不满 4KB

### 下行（服务器→设备）

**音频数据** (`/qhmu/lele/mcu/audio/pcm/down`)：

| 偏移 | 长度 | 说明 |
|------|------|------|
| 0-35 | 36 字节 | session_id |
| 36-39 | 4 字节 | sequence number（uint32，大端序，从 0 开始递增） |
| 40+ | 剩余 | 合成 PCM 数据 |

- 格式与上行一致

**控制消息** (`/qhmu/lele/mcu/audio/ubuntu/control`)：
```json
{"event": "done",  "session_id": "uuid-xxx"}
{"event": "error", "session_id": "uuid-xxx", "reason": "错误描述"}
```

## 7. 交互时序

```
设备 Key1 按下
  → publish voice/up/control {"event":"start","session_id":"xxx","max_duration":60}
  → 开始录音，同时开始发分片 [session_id + seq + PCM]  (QoS 0, 边录边传)

设备录音中，持续发送分片...

设备 Key1 再次按下
  → publish voice/up/control {"event":"stop","session_id":"xxx"}

  ┌──────────────── 服务器处理 ────────────────────────────────┐
  │  1. 按 sequence number 排序拼接 PCM（检测丢片）              │
  │  2. ASR 语音识别                                           │
  │  3. LLM 流式输出，每遇句号/问号/感叹号分句                    │
  │  4. 每句话送 TTS 合成，合成完一片立刻发送                      │
  └────────────────────────────────────────────────────────────┘

  → publish voice/down/audio [session_id + seq + PCM]  (QoS 0, 分片发送)
  → publish voice/down/control {"event":"done","session_id":"xxx"}

设备收到分片 → 按 seq 排序 → WM8978G → 喇叭/耳机播放
```

### 延迟预估（优化后）

| 阶段 | 耗时 | 说明 |
|------|------|------|
| 录音 + 传输 | ~0s | 边录边传，停止时已传完 |
| ASR 识别 | ~1-2s | |
| LLM 首句生成 | ~1-2s | 流式输出 |
| TTS 合成首句 | ~0.5-1s | |
| **首字延迟** | **~3-5s** | 用户按停止到听到第一个字 |

## 8. 容错设计

### 异常输入处理

| 异常场景 | 处理方式 |
|----------|----------|
| 只发 audio 没发 start | 丢弃音频，等待 start |
| 连续发两次 start | 忽略第二个，回复 error |
| start 和 stop 的 session_id 不匹配 | 忽略 stop，等待匹配的 stop 或超时 |
| 音频 sequence 不连续 | 检测丢片，可选补零或报错 |
| 超时未收到 stop | 90 秒后自动清理会话，回复 error |
| 设备断线重连 | 旧 session 超时清理，新连接开始新会话 |

### 服务器资源保护

| 措施 | 说明 |
|------|------|
| 并发会话限制 | 最多 5 个同时进行的会话 |
| 单会话内存上限 | 拼接后 PCM 不超过 2MB（约 60 秒） |
| 超时清理 | 每个会话 90 秒超时 |
| API 调用超时 | ASR/LLM/TTS 各自设置 10 秒超时 |

### 流式调用中途失败处理

LLM 和 TTS 均为流式调用，中途可能因网络、超时或 API 错误而中断：

| 失败场景 | 处理方式 |
|----------|----------|
| LLM 流式输出中途超时/断开 | 将已生成并分句的部分正常送 TTS 合成下发，然后发送 `{"event":"error","reason":"llm_stream_interrupted"}`，结束本次会话 |
| LLM 首次调用即失败（无任何输出） | 直接发送 `{"event":"error","reason":"llm_failed"}`，不发送任何音频 |
| TTS 某段合成失败 | 记录日志，跳过该段，继续处理 LLM 后续输出的下一句。若连续 2 段失败则放弃并发送 error |
| TTS 首段即失败 | 直接发送 `{"event":"error","reason":"tts_failed"}`，结束本次会话 |

**核心原则**：能发多少发多少，尽量让用户听到部分回答，而非直接报错丢弃全部。

### 会话超时与 TTS 阶段一致性

会话超时（90 秒）可能在 TTS 流式下发过程中触发，导致设备收到部分音频但没有 done/结束信号：

| 状态 | 超时处理 |
|------|----------|
| ASR/LLM 阶段超时 | 直接发送 `{"event":"error","reason":"timeout"}`，清理会话 |
| TTS 正在下发中超时 | **不立即断开**，将超时延长至当前 TTS 段发完，发完后发送 `{"event":"error","reason":"timeout_partial"}` 通知设备数据不完整，然后清理会话 |
| TTS 已全部合成完、正在分片下发中超时 | 同上，尽量发完已有数据，再发 error |

**设备端对应策略**：设备在开始播放后应设置一个播放超时（如 120 秒），若超时未收到 done 也未收到 error，自行停止播放并恢复空闲状态。

### 设备端保护

| 措施 | 说明 |
|------|------|
| 录音超时 | 设备端最大录音 60 秒，自动停止 |
| MQTT 断线重连 | 自动重连，重连后开始新会话 |
| 播放缓冲 | 收到足够分片后再开始播放，避免断续 |

## 9. 安全设计

| 措施 | 说明 |
|------|------|
| MQTT TLS | EMQX 开启 TLS，加密传输 |
| MQTT 认证 | 设备使用用户名/密码连接 |
| API Key 存储 | 环境变量，不硬编码 |
| 频率限制 | 单设备录音间隔不少于 5 秒 |

## 10. 服务端架构

```
voiceServer/
├── server.py          # 主入口，MQTT 客户端 + 会话管理
├── session.py         # 会话类，管理单次交互的生命周期
├── asr.py             # ASR 调用封装
├── llm.py             # LLM 流式调用封装
├── tts.py             # TTS 调用封装
├── audio.py           # PCM 分片/拼接/格式处理
├── config.py          # 配置（MQTT 地址、API Key、超时参数等）
├── requirements.txt   # 锁定版本的依赖
```

**技术栈**：
- `paho-mqtt`：MQTT 客户端
- `asyncio`：异步处理，避免阻塞
- `openai`：LLM 调用（OpenAI 兼容 API）
如果有更好的你可以替换
## 11. 系统服务部署

将 voiceServer 注册为 systemd 服务，实现开机自启、崩溃自愈、资源动态分配。


### 环境变量文件

```bash
# /home/water/voiceServer/.env（权限 600）
ASR_API_KEY=your_asr_key
LLM_API_KEY=your_llm_key
TTS_API_KEY=your_tts_key
MQTT_PASSWORD=your_mqtt_password
```
