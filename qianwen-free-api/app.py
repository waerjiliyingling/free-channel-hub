"""千问 try-ai（platform.qianwenai.com）免登录网页转 API

原理：驱动真实 Chromium 打开试用页（匿名可用，默认 Qwen3.8-Max），
每次请求新开一轮对话：注入消息 -> 发送 -> 轮询 DOM 流式抓取回复增量。
反爬（bxUmid/baxia）由真实页面环境自行处理。

性能设计：
- 输入用 keyboard.insert_text 整段注入（逐字符 type 对长提示词是不可接受的）
- 回复流式产出：轮询"正文区"文本增量，边生成边 yield（首字延迟≈模型首包）
"""

import asyncio
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, List, Optional

from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import StreamingResponse
from fastapi.security.http import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("qianwen-free-api")

API_KEYS = set(filter(None, os.getenv("API_KEYS", "sk-qianwen-local-2026").split(",")))
PAGE_URL = "https://platform.qianwenai.com/try-ai/?scene=chat"
SEND_TIMEOUT_MS = 240_000
POLL_MS = 250

bearer = HTTPBearer(auto_error=False)


class Message(BaseModel):
    role: str = "user"
    content: str = ""


class ChatCompletionRequest(BaseModel):
    model: str = "qwen3.8-max"
    messages: List[Message]
    stream: Optional[bool] = False
    # Agent 场景下客户端会带这些字段；本适配器仅透传文本，但要原样接收不报错
    tools: Optional[list] = None
    tool_choice: Optional[Any] = None
    temperature: Optional[float] = None
    top_p: Optional[float] = None
    max_tokens: Optional[int] = None
    reasoning_effort: Optional[Any] = None
    response_format: Optional[Any] = None


class BrowserManager:
    def __init__(self):
        self._pw = None
        self._ctx = None
        self.page = None
        self._lock = asyncio.Lock()
        self._chat_lock = asyncio.Lock()

    async def ensure(self):
        async with self._lock:
            if self.page is None:
                self._pw = await async_playwright().start()
                self._ctx = await self._pw.chromium.launch_persistent_context(
                    user_data_dir="/app/browser-profile",
                    headless=os.getenv("HEADLESS", "1") == "1",
                    args=["--disable-blink-features=AutomationControlled"],
                    viewport={"width": 1440, "height": 900},
                    locale="zh-CN",
                )
                self.page = self._ctx.pages[0] if self._ctx.pages else await self._ctx.new_page()
                await self.page.goto(PAGE_URL, timeout=90_000, wait_until="domcontentloaded")
                await self.page.wait_for_selector('[contenteditable="true"]', timeout=60_000)
                await self.page.wait_for_timeout(4000)
                logger.info("[Browser] 页面就绪")
        return self.page


manager = BrowserManager()


def parse_messages(messages: List[Message]) -> str:
    """多轮历史折叠为单条提示词（每轮新会话，无服务端上下文）"""
    parts = []
    for m in messages:
        if m.role == "system":
            parts.append(f"[系统指令] {m.content}")
        elif m.role == "assistant":
            parts.append(f"[你的上一轮回答] {m.content}")
        else:
            parts.append(m.content)
    return "\n\n".join(parts)


# 流式抓取：以"模型徽标链接"为结构锚点定位最后一条助手消息容器。
# 关键设计：不依赖 prompt 前缀匹配。
#   prompt 可能极长（Agent 的 tools JSON、多轮历史折叠），
#   而 DOM 会把 \n\n 渲染成 \n、并做空白归一，前缀匹配在多轮长 prompt 下必挂。
#   改为：取徽标所在消息容器 -> 剥掉徽标自身 -> 剥掉页面噪声行 -> 剩下的就是回复。
STREAM_EXTRACT_JS = """(prompt) => {
    const cut = (s) => {
        for (const mk of ['\\n输入消息', '\\n输入你的内容', '\\n首包延迟', '\\n深度思考',
                          '\\n联网搜索', '\\n登录可领取', '\\n请输入提示词后再发送']) {
            const idx = s.indexOf(mk);
            if (idx > 0) s = s.slice(0, idx);
        }
        return s;
    };
    const clean = (s) => {
        const lines = s.split('\\n').filter(l => l.trim() !== '');
        while (lines.length && /^qwen[\\w.\\-]*$/i.test(lines[0].trim())) lines.shift();
        const out = [];
        for (const l of lines) {
            const t = l.trim();
            // 统计行 / 过渡态 / 免责声明 一律截断
            if (/^(首包延迟|总耗时|输入|输出)[:：]/.test(t)) break;
            if (/^(内容生成中|思考中|模型配置加载中)[.。…]*$/.test(t)) break;
            if (/AI 回答可能存在不准确/.test(t)) break;
            out.push(l);
        }
        // 过滤纯符号噪声行（残留的 "+"、分隔线等），避免首块出现垃圾字符
        const body = out.filter(l => !/^[+*\-_=~|·—]+$/.test(l.trim()));
        return body.join('\\n').trim();
    };
    const links = Array.from(document.querySelectorAll('a[href*="/models/"]'));
    if (!links.length) return null;
    // 最后一条助手消息的徽标（页面是追加式渲染，最后一个就是当前回复）
    const badge = links[links.length - 1];
    const badgeText = (badge.innerText || '').trim();
    let node = badge;
    for (let i = 0; i < 8; i++) {
        const t = (node.innerText || '');
        if (t.replace(badgeText, '').trim().length > 5 && !/输入消息/.test(t)) break;
        if (!node.parentElement || node.parentElement.tagName === 'BODY') break;
        node = node.parentElement;
    }
    let text = cut(node.innerText || '');
    // 只保留徽标之后的文本：徽标出现在助手消息开头，其后即回复正文
    const bi = text.indexOf(badgeText);
    if (bi >= 0) text = text.slice(bi + badgeText.length);
    return clean(text);
}"""

IS_DONE_JS = ("() => Array.from(document.querySelectorAll('*')).some("
              "el => el.children.length === 0 && /总耗时：/.test(el.textContent || ''))")


# 过渡态文案：出现时视为"还没有正文"，不作增量
NOISE_TEXTS = {"模型配置加载中...", "模型配置加载中…", "内容生成中...", "内容生成中…", "思考中...", "思考中…"}


async def _wait_editor(page, timeout_ms: int = 60_000):
    """等待输入框就绪。SPA 页面 goto 后编辑器是异步挂载的，
    单次 wait_for_selector 容易超时，这里做带重载的多轮等待。"""
    deadline = time.time() + timeout_ms / 1000
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        try:
            await page.wait_for_selector('[contenteditable="true"]',
                                         state="visible", timeout=20_000)
            await page.wait_for_timeout(1500)
            if await page.locator('[contenteditable="true"]').count():
                return True
        except Exception:
            pass
        if time.time() >= deadline:
            break
        # 还在会话页就先尝试点返回；都不行才整页重载
        try:
            back = page.get_by_role("button", name="Back to scene home")
            if await back.count():
                await back.click(timeout=2500)
                await page.wait_for_timeout(2500)
                continue
        except Exception:
            pass
        logger.warning(f"[Browser] 编辑器未就绪，第 {attempt} 次重载页面")
        try:
            await page.goto(PAGE_URL, timeout=90_000, wait_until="domcontentloaded")
        except Exception as e:
            logger.warning(f"[Browser] goto 失败: {e}")
        await page.wait_for_timeout(3000)
    return False


async def run_chat(prompt: str) -> AsyncGenerator[str, None]:
    """执行一轮对话，流式 yield 回复增量"""
    page = await manager.ensure()
    async with manager._chat_lock:
        t0 = time.time()
        # 回到"新对话"：上一轮结束后停在会话页，优先点返回按钮（零重载）；
        # 无论成败都交给 _wait_editor 做带重载的多轮等待，避免单次超时直接抛出
        if getattr(manager, "_in_session", False):
            manager._in_session = False
            try:
                back = page.get_by_role("button", name="Back to scene home")
                if await back.count():
                    await back.click(timeout=2500)
            except Exception as e:
                logger.warning(f"[Browser] 返回首页失败，改为重载: {e}")

        # 输入框不在（或刚返回首页还没渲染）-> 带重试的等待，内部含整页重载
        if not await page.locator('[contenteditable="true"]').count():
            if not await _wait_editor(page):
                raise RuntimeError("输入框始终未就绪（页面结构可能已变更或网络异常）")
        t_load = time.time() - t0

        editor = page.locator('[contenteditable="true"]').first
        try:
            await editor.click(timeout=10_000)
        except Exception:
            # 弹层/推广条可能遮挡：截图留证后强制点击
            try:
                await page.screenshot(path="/app/debug-click.png")
            except Exception:
                pass
            await editor.click(force=True, timeout=10_000)
        await page.keyboard.insert_text(prompt)
        await page.wait_for_timeout(400)

        send = page.get_by_role("button", name="发送消息")
        for _ in range(6):
            if await send.is_enabled():
                break
            await page.wait_for_timeout(300)
        if not await send.is_enabled():
            raise RuntimeError("发送按钮不可用（输入未生效）")
        await send.click()
        manager._in_session = True  # 已进入会话页；无论本轮成败，下轮都先点返回回新对话
        t_sent = time.time()

        last = ""
        first_chunk_at = None
        done = False
        deadline = time.time() + SEND_TIMEOUT_MS / 1000
        while time.time() < deadline:
            cur = await page.evaluate(STREAM_EXTRACT_JS, prompt)
            if cur in NOISE_TEXTS:
                cur = ""
            if cur and cur.startswith(last) and len(cur) > len(last):
                if first_chunk_at is None:
                    first_chunk_at = time.time() - t_sent
                yield cur[len(last):]
                last = cur
            elif cur and not cur.startswith(last):
                # 正文被整体替换（如未知加载态→真实回复）：全量补发，宁重复不丢内容
                if first_chunk_at is None:
                    first_chunk_at = time.time() - t_sent
                yield cur
                last = cur
            if await page.evaluate(IS_DONE_JS):
                await page.wait_for_timeout(600)
                fin = await page.evaluate(STREAM_EXTRACT_JS, prompt)
                if fin in NOISE_TEXTS:
                    fin = ""
                if fin and fin.startswith(last) and len(fin) > len(last):
                    if first_chunk_at is None:
                        first_chunk_at = time.time() - t_sent
                    yield fin[len(last):]
                    last = fin
                elif fin and not fin.startswith(last):
                    yield fin
                    last = fin
                done = True
                break
            await page.wait_for_timeout(POLL_MS)

        if not done:
            # 区分"真的超时"和"上游明确报错"：后者是额度/登录态问题，
            # 给出可操作的提示，而不是一句难懂的 TimeoutError
            tail = await page.evaluate(
                "() => (document.body.innerText || '').slice(-600)")
            if "访客体验次数" in tail or "前往登录" in tail:
                raise RuntimeError(
                    "千问免登录访客额度已用尽（页面提示『访客体验次数仅剩 N 次』）。"
                    "该渠道为匿名试用，有次数上限，请改用官方 API 渠道"
                    "（ModelScope / DashScope 等）或登录后重试。")
            if "SSE_TRANSPORT" in tail or "网络连接中断" in tail:
                raise RuntimeError(
                    "千问网页版 SSE 连接被服务端切断（错误码 SSE_TRANSPORT），"
                    "通常是匿名试用被限流或额度耗尽。请稍后重试或改用官方 API 渠道。")
            raise TimeoutError("等待回复超时")
        if not last:
            sample = await page.evaluate(
                "() => (document.body.innerText || '').slice(-400)")
            logger.error(f"[Chat] 空回复诊断，页面尾部: {sample!r}")
            raise RuntimeError("回复内容为空")
        logger.info(f"[Chat] 装载 {t_load:.1f}s | 首字 {first_chunk_at}s | "
                    f"全程 {time.time() - t0:.1f}s | 长度 {len(last)}")


@asynccontextmanager
async def lifespan(_: FastAPI):
    await manager.ensure()
    # 启动即预检 JS 常量：写错（如把 '\\n' 写成 '\n' 破坏字符串字面量）
    # 只会在运行时炸，这里提前暴露，避免线上才报 SyntaxError
    try:
        await manager.page.evaluate(STREAM_EXTRACT_JS, "__preflight__")
        logger.info("[Preflight] STREAM_EXTRACT_JS 语法 OK")
    except Exception as e:
        logger.error(f"[Preflight] STREAM_EXTRACT_JS 语法错误: {e}")
    yield


app = FastAPI(title="qianwen-free-api", version="0.2.0", lifespan=lifespan)


def check_key(auth: HTTPAuthorizationCredentials = Depends(bearer)):
    if not auth or auth.credentials not in API_KEYS:
        raise HTTPException(status_code=403, detail="invalid api_key")


@app.get("/healthz")
def healthz():
    return {"ok": True, "service": "qianwen-free-api",
            "page_ready": manager.page is not None,
            "note": "免登录渠道有访客额度上限，见 /debug/quota"}


@app.get("/debug/quota")
async def debug_quota(_=Depends(check_key)):
    """读取页面上的访客剩余体验次数，用于提前发现额度耗尽。"""
    try:
        page = await manager.ensure()
        tail = await page.evaluate(
            "() => (document.body.innerText || '').slice(-600)")
        quota = None
        for line in tail.split("\n"):
            if "访客体验次数" in line:
                quota = line.strip()
                break
        return {"quota_line": quota,
                "exhausted": bool(quota and "仅剩 0 次" in quota),
                "needs_login": "前往登录" in tail}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


@app.get("/v1/models")
def models():
    return {"data": [{"id": "qwen3.8-max", "object": "model", "owned_by": "qianwen-free-api"},
                     {"id": "qwen3.8-plus", "object": "model", "owned_by": "qianwen-free-api"}],
            "object": "list"}


@app.get("/debug/dom")
async def debug_dom(_=Depends(check_key)):
    """诊断：返回当前页面 URL、输入框数量、页面尾部文本。
    网页改版导致选择器失效时，用它看真实结构。"""
    try:
        page = await manager.ensure()
    except Exception as e:
        return {"error": f"browser init failed: {e}"}
    try:
        return {
            "url": page.url,
            "editor_count": await page.locator('[contenteditable="true"]').count(),
            "in_session": getattr(manager, "_in_session", False),
            "back_button_count": await page.get_by_role(
                "button", name="Back to scene home").count(),
            "tail": (await page.evaluate(
                "() => (document.body.innerText || '').slice(-500)")),
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


def _sse(cid: str, created: int, model: str, delta: dict = None, finish: str = None) -> str:
    choice = {"index": 0, "delta": delta or {}, "finish_reason": finish}
    return "data: " + json.dumps({"id": cid, "object": "chat.completion.chunk",
                                  "created": created, "model": model,
                                  "choices": [choice]}, ensure_ascii=False) + "\n\n"


@app.post("/v1/chat/completions")
async def chat(req: ChatCompletionRequest, _=Depends(check_key)):
    prompt = parse_messages(req.messages)
    if not prompt.strip():
        raise HTTPException(status_code=400, detail="empty messages")

    if req.stream:
        cid = f"qw-{int(time.time())}"
        created = int(time.time())

        async def sse():
            yield _sse(cid, created, req.model, {"role": "assistant"})
            try:
                async for delta in run_chat(prompt):
                    yield _sse(cid, created, req.model, {"content": delta})
            except Exception as e:
                logger.error(f"stream failed: {e}")
                yield _sse(cid, created, req.model, {"content": f"\n[适配器错误] {e}"})
            yield _sse(cid, created, req.model, None, "stop")
            yield "data: [DONE]\n\n"

        return StreamingResponse(sse(), media_type="text/event-stream")

    try:
        parts = []
        async for delta in run_chat(prompt):
            parts.append(delta)
        text = "".join(parts)
    except Exception as e:
        logger.error(f"chat failed: {e}")
        raise HTTPException(status_code=502, detail=str(e))
    now = int(time.time())
    return {"id": f"qw-{now}", "object": "chat.completion", "created": now,
            "model": req.model,
            "choices": [{"index": 0,
                         "message": {"role": "assistant", "content": text},
                         "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
