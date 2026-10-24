"""free-channel-hub v2：多渠道熔断 + 负载均衡 + 透明透传网关（Agent 可用）

v2 相对 v1 的核心修复（v1 对 Agent 不可用）：
  1. 全量透传请求体：v1 只转发 model/messages/stream，把 tools、temperature、
     reasoning_effort、response_format 全丢了 -> Agent 的 tool_calls 永远拿不到。
     v2 原样转发整个 JSON 对象，仅在需要时改写 model 字段。
  2. 模型通配符路由：渠道可用 glob（如 "qwen*"、"*flash*"）声明模型，
     未精确命中时按 glob 匹配；请求模型名 -> 上游真实模型名的别名映射。
  3. 非流式不再丢弃增量：v1 非流式只累加 delta.content，丢了
     tool_calls / reasoning_content；v2 直接转发上游非流式 JSON。
  4. 错误块规范化：上游报错时产出合法 SSE 错误帧，客户端可解析。
  5. 渠道健康探测后台任务 + 管理端点，便于观察。

配置来源（优先级从高到低）：
  CHANNELS_JSON  ->  /app/channels.json  ->  内置默认值
"""

import asyncio
import fnmatch
import hashlib
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.security.http import HTTPAuthorizationCredentials, HTTPBearer

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("hub")

HUB_API_KEYS = set(filter(None, os.getenv("HUB_API_KEYS", "sk-hub-local-2026").split(",")))
STICKY_TTL = float(os.getenv("STICKY_TTL", "1800"))
FIRST_BYTE_TIMEOUT = float(os.getenv("FIRST_BYTE_TIMEOUT", "60"))
PROBE_INTERVAL = float(os.getenv("PROBE_INTERVAL", "300"))

# ---------------- 渠道表 ----------------
# 说明：
#   name        渠道唯一名
#   base_url    上游 OpenAI 兼容地址（不带 /v1/chat/completions）
#   api_key     上游 key
#   models      该渠道可服务的模型；支持 glob（"qwen*"）；可用 "alias=real" 改写上游模型名
#   weight      权重，默认 1
#   env_key     可选：只有当该环境变量非空时此渠道才启用（官方 API key 场景）
DEFAULT_CHANNELS = [
    {"name": "qianwen-web", "base_url": "http://qianwen-free-api:8000",
     "api_key": "sk-qianwen-local-2026",
     "models": ["qwen3.8-max", "qwen3.8-plus", "qwen*"], "weight": 1},
    {"name": "yuanbao-web", "base_url": "http://yuanbao-free-api:8000",
     "api_key": "sk-yuanbao-local-2026",
     "models": ["hunyuan", "hunyuan-t1", "deepseek-v3", "deepseek-r1",
                "hunyuan*", "deepseek*"], "weight": 1},
    {"name": "ark-web", "base_url": "http://ark-free-api:8000",
     "api_key": "sk-ark-local-2026",
     "models": ["deepseek-v4.1-flash", "deepseek*", "doubao*"], "weight": 1},
]


def load_channels() -> List[dict]:
    raw = os.getenv("CHANNELS_JSON")
    if not raw and os.path.exists("/app/channels.json"):
        raw = open("/app/channels.json", encoding="utf-8").read()
    if not raw:
        raw = json.dumps(DEFAULT_CHANNELS)
    return json.loads(raw)


# ---------------- 熔断器 ----------------

@dataclass
class Breaker:
    threshold: int = int(os.getenv("BREAKER_THRESHOLD", "3"))
    cooldown_s: float = float(os.getenv("BREAKER_COOLDOWN_S", "300"))
    cooldown_max_s: float = float(os.getenv("BREAKER_COOLDOWN_MAX_S", "3600"))
    fails: int = 0
    trips: int = 0
    open_until: float = 0.0
    state: str = "closed"      # closed / open / half_open

    def allow(self, now: float) -> bool:
        if self.state == "open":
            if now >= self.open_until:
                self.state = "half_open"
                return True
            return False
        return True

    def record_success(self):
        self.fails = 0
        self.trips = 0
        self.state = "closed"

    def record_fail(self, now: float):
        self.fails += 1
        if self.state == "half_open" or self.fails >= self.threshold:
            cd = min(self.cooldown_s * (2 ** self.trips), self.cooldown_max_s)
            self.trips += 1
            self.open_until = now + cd
            self.state = "open"
            logger.warning(f"[Breaker] OPEN cooldown={cd/60:.0f}m trips={self.trips}")


@dataclass
class Channel:
    name: str
    base_url: str
    api_key: str
    models: List[str]
    weight: int = 1
    first_byte_timeout_s: float = FIRST_BYTE_TIMEOUT
    max_in_flight: int = 2
    breaker: Breaker = field(default_factory=Breaker)
    in_flight: int = 0
    rr_cursor: float = 0.0
    ok_count: int = 0
    fail_count: int = 0
    last_error: str = ""
    # 并发闸门：单会话型上游（网页适配器只有一个浏览器实例）必须串行，
    # 否则并发会因登录态/会话竞争直接 500。信号量让多余请求排队而不是打爆上游。
    sem: Any = field(default=None, repr=False)

    def __post_init__(self):
        if self.sem is None:
            self.sem = asyncio.Semaphore(max(1, self.max_in_flight))

    def match(self, model: str) -> Optional[str]:
        """返回上游真实模型名；不服务该模型返回 None。
        支持 'alias=real' 形式改写模型名。"""
        best = None
        for m in self.models:
            if "=" in m:
                pat, real = m.split("=", 1)
            else:
                pat, real = m, None
            if model == pat.split("=")[0] or fnmatch.fnmatch(model, pat):
                # 精确匹配优先于 glob
                score = 2 if pat == model else 1
                if best is None or score > best[0]:
                    best = (score, real)
        if best is None:
            return None
        return best[1] or model


class Hub:
    def __init__(self):
        raw = load_channels()
        self.channels: List[Channel] = []
        for c in raw:
            env_key = c.pop("env_key", None)
            if env_key:
                val = os.getenv(env_key, "").strip()
                if not val:
                    logger.info(f"[Skip] 渠道 {c.get('name')} 未配置 {env_key}，已跳过")
                    continue
                # 环境变量的值直接作为上游 api_key
                c["api_key"] = val
            allowed = set(Channel.__dataclass_fields__)
            self.channels.append(Channel(**{k: v for k, v in c.items() if k in allowed}))
        self.sticky: Dict[str, tuple] = {}

    def candidates(self, model: str) -> List[tuple]:
        """返回 [(channel, upstream_model)]"""
        out = []
        for c in self.channels:
            real = c.match(model)
            if real is not None:
                out.append((c, real))
        return out

    def pick_order(self, cands: List[tuple], prefer: Optional[str] = None) -> List[tuple]:
        now = time.time()
        avail = [t for t in cands
                 if t[0].breaker.allow(now) and t[0].in_flight < t[0].max_in_flight]
        if not avail:
            avail = [t for t in cands if t[0].breaker.allow(now)] or list(cands)
        if prefer:
            avail.sort(key=lambda t: 0 if t[0].name == prefer else 1)
        for t in avail:
            t[0].rr_cursor += t[0].weight
        return sorted(avail, key=lambda t: -t[0].rr_cursor)

    def touch_sticky(self, fp: str) -> Optional[str]:
        hit = self.sticky.get(fp)
        if hit and hit[1] > time.time():
            return hit[0]
        return None

    def set_sticky(self, fp: str, name: str):
        self.sticky[fp] = (name, time.time() + STICKY_TTL)


hub = Hub()
bearer = HTTPBearer(auto_error=False)


def check_key(auth: HTTPAuthorizationCredentials = Depends(bearer)):
    if not auth or auth.credentials not in HUB_API_KEYS:
        raise HTTPException(status_code=403, detail="invalid api_key")


app = FastAPI(title="free-channel-hub", version="2.0.0")


@asynccontextmanager
async def lifespan(_: FastAPI):
    logger.info("channels: " + ", ".join(
        f"{c.name}({','.join(c.models)})" for c in hub.channels) or "EMPTY")
    if PROBE_INTERVAL > 0:
        asyncio.create_task(probe_loop())
    yield


@app.get("/healthz")
def healthz():
    return {"ok": True, "channels": len(hub.channels),
            "models": sorted({m for c in hub.channels for m in c.models})}


@app.get("/v1/models")
def models():
    ids = sorted({m for c in hub.channels for m in c.models
                  if "*" not in m and "=" not in m})
    return {"data": [{"id": m, "object": "model", "owned_by": "free-channel-hub"}
                     for m in ids], "object": "list"}


@app.get("/admin/channels")
def admin_channels():
    now = time.time()
    return [{"name": c.name, "state": c.breaker.state,
             "fails": c.breaker.fails, "trips": c.breaker.trips,
             "open_until": round(c.breaker.open_until - now, 1) if c.breaker.state == "open" else 0,
             "in_flight": c.in_flight, "ok": c.ok_count, "fail": c.fail_count,
             "models": c.models, "last_error": c.last_error[:200]}
            for c in hub.channels]


@app.post("/admin/reset-breaker/{name}")
def reset_breaker_named(name: str, _=Depends(check_key)):
    """按渠道名解除熔断。

    场景：网页渠道登录态恢复后不想等冷却（默认 5 分钟起、逐次翻倍）。
    name 传 "*" 重置全部。
    """
    known = {c.name for c in hub.channels}
    if name not in known and name != "*":
        raise HTTPException(status_code=404,
                            detail=f"unknown channel {name}; known: {sorted(known)}")
    hit = []
    for c in hub.channels:
        if name != "*" and c.name != name:
            continue
        c.breaker.record_success()
        c.fail_count = 0
        hit.append(c.name)
    return {"reset": hit, "channels": [c.name for c in hub.channels]}


async def probe_loop():
    """后台健康探测：把已熔断渠道的失败状态暴露出来，并做被动恢复。"""
    while True:
        try:
            async with httpx.AsyncClient(timeout=10) as cli:
                for c in hub.channels:
                    try:
                        r = await cli.get(f"{c.base_url}/v1/models",
                                          headers={"Authorization": f"Bearer {c.api_key}"})
                        if r.status_code < 500:
                            c.last_error = ""
                    except Exception as e:
                        c.last_error = f"{type(e).__name__}: {e}"
        except Exception:
            pass
        await asyncio.sleep(PROBE_INTERVAL)


def _err_sse(cid: str, model: str, text: str) -> str:
    return ("data: " + json.dumps({"id": cid, "object": "chat.completion.chunk",
                                   "created": int(time.time()), "model": model,
                                   "choices": [{"index": 0, "delta": {"content": text},
                                                "finish_reason": None}]},
                                  ensure_ascii=False) + "\n\n")


def _sse_to_completion(raw: str, cid: str, model: str) -> dict:
    """把上游误返的 SSE 文本聚合成标准非流式 chat.completion。
    累加 delta.content；若出现 tool_calls 也按 OpenAI 语义合并。"""
    content = ""
    reasoning = ""
    tool_calls: Dict[int, dict] = {}
    finish = "stop"
    for line in raw.splitlines():
        line = line.strip()
        if not line.startswith("data:"):
            continue
        data = line[5:].strip()
        if data == "[DONE]":
            break
        try:
            d = json.loads(data)
        except Exception:
            continue
        for ch in d.get("choices") or []:
            delta = ch.get("delta") or {}
            if delta.get("content"):
                content += delta["content"]
            if delta.get("reasoning_content"):
                reasoning += delta["reasoning_content"]
            for i, tc in enumerate(delta.get("tool_calls") or []):
                slot = tool_calls.setdefault(tc.get("index", i),
                                             {"id": "", "type": "function",
                                              "function": {"name": "", "arguments": ""}})
                if tc.get("id"):
                    slot["id"] = tc["id"]
                fn = tc.get("function") or {}
                if fn.get("name"):
                    slot["function"]["name"] += fn["name"]
                if fn.get("arguments"):
                    slot["function"]["arguments"] += fn["arguments"]
            if ch.get("finish_reason"):
                finish = ch["finish_reason"]

    msg: Dict[str, Any] = {"role": "assistant", "content": content}
    if reasoning:
        msg["reasoning_content"] = reasoning
    if tool_calls:
        msg["tool_calls"] = [tool_calls[k] for k in sorted(tool_calls)]
        finish = "tool_calls" if finish == "stop" else finish
    return {"id": cid, "object": "chat.completion", "created": int(time.time()),
            "model": model,
            "choices": [{"index": 0, "message": msg, "finish_reason": finish}],
            "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}}


def _rewrite_model(line: str, model: str) -> str:
    """把 SSE 行的 model 字段改写为调用方请求的模型名。
    解析失败时原样返回，绝不影响内容转发。"""
    try:
        d = json.loads(line[5:].strip())
        if "model" in d:
            d["model"] = model
        return "data: " + json.dumps(d, ensure_ascii=False)
    except Exception:
        return line


async def _try_stream(ch: Channel, payload: dict, cid: str, model: str):
    """单渠道流式转发。首字节前失败 -> raise（可切换渠道）；
    已发出内容后失败 -> 补错误块结束（不切换，避免内容重复）。"""
    emitted = False
    # 并发闸门：超出 max_in_flight 的请求在此排队，
    # 避免并发打爆单会话型上游（网页适配器）
    async with ch.sem:
        try:
            ch.in_flight += 1
            async with httpx.AsyncClient(timeout=httpx.Timeout(
                    ch.first_byte_timeout_s, read=600, write=60, pool=60)) as client:
                async with client.stream("POST", f"{ch.base_url}/v1/chat/completions",
                                         headers={"Authorization": f"Bearer {ch.api_key}"},
                                         json=payload) as r:
                    if r.status_code >= 500 or r.status_code in (401, 403, 404):
                        body = (await r.aread()).decode("utf-8", "ignore")[:300]
                        raise RuntimeError(f"upstream {r.status_code}: {body}")
                    async for line in r.aiter_lines():
                        if not line.startswith("data:"):
                            continue
                        if line.strip() == "data: [DONE]":
                            yield line + "\n\n"
                            ch.breaker.record_success()
                            ch.ok_count += 1
                            return
                        if '"choices"' in line:
                            emitted = True
                            # 回写调用方请求的模型名：上游可能返回自己的内部 ID
                            # （如元宝把 model 设成会话 ID），会导致客户端模型名漂移
                            yield _rewrite_model(line, model) + "\n\n"
                    if emitted:
                        ch.breaker.record_success()
                        ch.ok_count += 1
                        yield "data: [DONE]\n\n"
                    else:
                        raise RuntimeError("upstream closed without content")
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout,
                httpx.RemoteProtocolError, RuntimeError) as e:
            ch.breaker.record_fail(time.time())
            ch.fail_count += 1
            ch.last_error = f"{type(e).__name__}: {e}"
            if emitted:
                yield _err_sse(cid, model, f"\n[hub] 渠道 {ch.name} 中途失败: {e}")
                yield "data: [DONE]\n\n"
            else:
                raise
        finally:
            ch.in_flight -= 1


@app.post("/v1/chat/completions")
async def chat(request: Request, _=Depends(check_key)):
    body = await request.json()
    model = body.get("model")
    if not model:
        raise HTTPException(status_code=400, detail="missing model")

    cands = hub.candidates(model)
    if not cands:
        raise HTTPException(status_code=404,
                            detail=f"no channel for model {model}; "
                                   f"available: {sorted({m for c in hub.channels for m in c.models})}")

    # 会话粘滞：按首条消息指纹
    msgs = body.get("messages") or []
    first = ""
    if msgs and isinstance(msgs[0], dict):
        c = msgs[0].get("content")
        first = c if isinstance(c, str) else json.dumps(c, ensure_ascii=False)
    fp = hashlib.sha1(first.encode()).hexdigest()[:12]
    prefer = hub.touch_sticky(fp)
    order = hub.pick_order(cands, prefer=prefer)
    cid = f"hub-{int(time.time()*1000)}"

    attempts: List[str] = []

    async def sse():
        last_err = None
        for ch, real_model in order:
            attempts.append(ch.name)
            logger.info(f"[Route] {model} -> {ch.name} (upstream={real_model})")
            # 透传整个请求体，仅改写 model 字段
            payload = dict(body)
            payload["model"] = real_model
            payload["stream"] = True
            try:
                got = False
                async for chunk in _try_stream(ch, payload, cid, model):
                    got = True
                    yield chunk
                if got:
                    hub.set_sticky(fp, ch.name)
                    return
            except Exception as e:
                last_err = f"{ch.name}: {e}"
                logger.warning(f"[Failover] {last_err}")
        yield _err_sse(cid, model,
                       f"\n[hub] 所有渠道不可用 attempts={attempts} last={last_err}")
        yield "data: [DONE]\n\n"

    if body.get("stream"):
        return StreamingResponse(sse(), media_type="text/event-stream",
                                 headers={"X-Hub-Attempts": ",".join(attempts)})

    # 非流式：优先直接转发上游的非流式响应（保留 tool_calls 等结构）
    for ch, real_model in order:
        attempts.append(ch.name)
        payload = dict(body)
        payload["model"] = real_model
        payload["stream"] = False
        try:
            # 并发闸门：单会话型上游必须串行
            async with ch.sem:
                ch.in_flight += 1
                try:
                    async with httpx.AsyncClient(timeout=httpx.Timeout(
                            ch.first_byte_timeout_s, read=600, write=60,
                            pool=60)) as client:
                        r = await client.post(f"{ch.base_url}/v1/chat/completions",
                                              headers={"Authorization": f"Bearer {ch.api_key}"},
                                              json=payload)
                        if r.status_code >= 500 or r.status_code in (401, 403, 404):
                            raise RuntimeError(
                                f"upstream {r.status_code}: {r.text[:300]}")
                        ch.breaker.record_success()
                        ch.ok_count += 1
                        hub.set_sticky(fp, ch.name)
                        raw = r.text
                        # 兼容性归一化：有些上游（如元宝网页版）无视 stream:false，
                        # 仍然返回 SSE 文本。此时解析出正文再包成标准 JSON，
                        # 否则调用方会收到一串 "data: {...}" 原始文本。
                        if raw.lstrip().startswith("data:"):
                            return JSONResponse(
                                _sse_to_completion(raw, cid, model))
                        try:
                            data = r.json()
                            data["model"] = model   # 回写请求方用的模型名
                            return JSONResponse(data)
                        except Exception:
                            return JSONResponse({
                                "id": cid, "object": "chat.completion",
                                "created": int(time.time()), "model": model,
                                "choices": [{"index": 0, "message":
                                             {"role": "assistant", "content": raw},
                                             "finish_reason": "stop"}],
                                "usage": {"prompt_tokens": 0, "completion_tokens": 0,
                                          "total_tokens": 0}})
                finally:
                    ch.in_flight -= 1
        except Exception as e:
            ch.breaker.record_fail(time.time())
            ch.fail_count += 1
            ch.last_error = f"{type(e).__name__}: {e}"
            logger.warning(f"[Failover] {ch.name}: {e}")

    raise HTTPException(status_code=502, detail=f"all channels failed: {attempts}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000)
