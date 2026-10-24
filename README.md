# free-channel-hub

免费网页版大模型 **多渠道聚合网关**：自制网页适配器 + 仿 wb2api 语义的熔断（Circuit Breaker）与负载均衡层，对外暴露统一 OpenAI 兼容 API。

## 组成

| 服务 | 端口 | 说明 |
|---|---|---|
| `ark-free-api` | 8110 | 火山方舟体验页适配器（Playwright UI 驱动；免登录有 10 次/日额度，风控或超额时扫码登录，`GET /login.png` 取二维码） |
| `qianwen-free-api` | 8109 | 阿里千问 try-ai 免登录适配器（匿名直聊，Qwen3.8-Max；流式 DOM 增量抓取） |
| `hub` | 8111 | **熔断 + 负载均衡层**：按模型路由到渠道池，加权轮询、故障自动切换、会话粘滞 |

另有第三方 `yuanbao-free-api`（腾讯元宝，DeepSeek 满血/混元）可并列接入，见部署文档。

## 熔断语义（仿 wb2api）

- 连续失败 **3 次** → 渠道 OPEN，冷却 **30 分钟**起步、每次翻倍、上限 **6 小时**
- 冷却结束 → HALF_OPEN，放一个探测请求：成功关断、失败重开
- 请求失败转移：上游连接失败/超时/5xx/流首字节前失败 → **自动切下一渠道**；已向客户端发出字节后不再转移（避免内容重复）
- 会话粘滞：按首条用户消息指纹绑定渠道 30 分钟
- 渠道状态面板：`GET /admin/channels`

## 快速开始

```bash
docker compose up -d --build

# 模型清单
curl http://127.0.0.1:8111/v1/models -H "Authorization: Bearer sk-hub-local-2026"

# 对话（free-chat 是聚合别名：千问/元宝/死渠道轮询，自动跳过熔断渠道）
curl http://127.0.0.1:8111/v1/chat/completions \
  -H "Authorization: Bearer sk-hub-local-2026" -H "Content-Type: application/json" \
  -d '{"model":"free-chat","messages":[{"role":"user","content":"hi"}],"stream":true}'

# 渠道熔断状态
curl http://127.0.0.1:8111/admin/channels -H "Authorization: Bearer sk-hub-local-2026"
```

## 配置

渠道表通过 `CHANNELS_JSON` 环境变量注入（见 docker-compose.yml）：

```json
[{"name":"qianwen-web","base_url":"http://qianwen-free-api:8000",
  "api_key":"sk-qianwen-local-2026","models":["qwen3.8-max","free-chat"],"weight":1}]
```

每渠道可调：`weight`（轮询权重）、`first_byte_timeout_s`（首字节超时，UI 驱动通道建议 ≥60）、`max_in_flight`（并发上限）。

## 工程笔记（踩坑实录）

- **输入注入必须用 `keyboard.insert_text`**：逐字符 `type` 对长提示词是不可接受的（8ms/字符）
- **流式抓取勿用 prompt 文本 `lastIndexOf` 定位**：DOM 把 `\n\n` 渲染成 `\n`，多轮 prompt 必挂；改用结构锚点（模型徽标 `a[href*="/models/"]`）+ 空白不敏感前缀剥离
- **过渡文案要过滤**："模型配置加载中…"等会被误当正文增量；正文整体替换时全量补发（宁重复不丢内容）
- **异步看门狗必须持有引用**：裸 `asyncio.create_task` 可能被 GC 静默回收
- **`FastAPI(lifespan=...)` 别忘了挂参**：定义了不挂，启动钩子静默不执行
- **元宝类登录态有间歇刷新竞态**：偶发"无法获取请求头"，重试即愈，勿当故障重建
- 免责：本项目仅供学习研究，网页逆向通道违反各平台 ToS 且易碎，请勿用于生产

## License

MIT
