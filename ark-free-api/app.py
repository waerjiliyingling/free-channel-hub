"""火山方舟体验页（ark.volcengine.com/experience/gen_chat）转 OpenAI 兼容 API

模式：
- ARK_API_KEY 模式（可选）：直接转发官方 /api/v3/chat/completions，最稳
- 网页模式（默认）：Playwright 驱动体验页。体验页聊天**强制登录**，
  首次启动截图登录二维码到 /login.png（GET /login.png 获取，微信扫码），
  登录态持久化在 /app/browser-profile，之后免登录。

流式：轮询聊天 iframe 正文增量（文本稳定 2s 视为生成完成）。
"""

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator, List, Optional

import httpx
from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.security.http import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("ark-free-api")

API_KEYS = set(filter(None, os.getenv("API_KEYS", "sk-ark-local-2026").split(",")))
ARK_API_KEY = os.getenv("ARK_API_KEY", "")          # 官方 API key（可选兜底通道）
ARK_API_BASE = os.getenv("ARK_API_BASE", "https://ark.cn-beijing.volces.com/api/v3")
REGION = os.getenv("ARK_REGION", "cn-beijing")
# 逻辑模型名 -> 方舟模型ID（体验页 URL 参数）
ARK_MODELS = json.loads(os.getenv("ARK_MODELS", json.dumps({
    "deepseek-v4.1-flash": "deepseek-v4-1-flash-261017",
})))
DEFAULT_MODEL = os.getenv("ARK_DEFAULT_MODEL", "deepseek-v4.1-flash")
SEND_TIMEOUT_S = 240
STABLE_MS = 2000          # 正文稳定时长视为生成完成
POLL_MS = 250

bearer = HTTPBearer(auto_error=False)


class Message(BaseModel):
    role: str = "user"
    content: str = ""


class ChatCompletionRequest(BaseModel):
    model: str = DEFAULT_MODEL
    messages: List[Message]
    stream: Optional[bool] = False


def page_url(model_id: str) -> str:
    return f"https://ark.volcengine.com/region:{REGION}/experience/gen_chat?model={model_id}"


class BrowserManager:
    def __init__(self):
        self._pw = None
        self._ctx = None
        self.page = None
        self.logged_in = False
        self._lock = asyncio.Lock()
        self._chat_lock = asyncio.Lock()
        self._watchdog_task = None

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
                await self.page.goto(page_url(ARK_MODELS[DEFAULT_MODEL]),
                                     timeout=90_000, wait_until="domcontentloaded")
                await self.page.wait_for_timeout(4000)
                # 显式持有引用，防止任务被 GC
                self._watchdog_task = asyncio.create_task(self._login_watchdog())
        return self.page

    async def _login_watchdog(self):
        """未登录时循环截图二维码供扫码；登录成功后置位"""
        logger.info("[Login] watchdog 启动")
        while True:
            try:
                need = await self._needs_login()
                if need:
                    self.logged_in = False
                    Path("/app/login.png").write_bytes(await self.page.screenshot())
                    logger.info("[Login] 需要登录，二维码已更新（GET /login.png）")
                else:
                    if not self.logged_in:
                        logger.info("[Login] 登录成功")
                    self.logged_in = True
            except Exception as e:
                logger.warning(f"[Login] watchdog: {e}")
            await asyncio.sleep(20)

    async def _needs_login(self) -> bool:
        for frame in [self.page, *self.page.frames]:
            try:
                txt = await frame.evaluate("document.body ? document.body.innerText.slice(0, 2000) : ''")
                if txt and ("手机号" in txt and "获取验证码" in txt or "微信扫码" in txt):
                    return True
            except Exception:
                continue
        return False

    async def find_input(self):
        """跨 frame 找聊天输入框（textarea 或 contenteditable）"""
        for frame in [self.page, *self.page.frames]:
            for sel in ("textarea", '[contenteditable="true"]', "[contenteditable]"):
                loc = frame.locator(sel).first
                try:
                    if await loc.count() and await loc.is_visible():
                        return frame, loc
                except Exception:
                    continue
        return None, None


FIND_PROMPT_FRAME_JS = """(prompt) => {
    const t = document.body ? document.body.innerText : '';
    return t.indexOf(prompt) !== -1;
}"""

EXTRACT_JS = """(prompt) => {
    const body = document.body.innerText || '';
    const i = body.lastIndexOf(prompt);
    if (i === -1) return null;
    let rest = body.slice(i + prompt.length);
    const markers = ['Shift + Enter', '发送', '重新生成', '复制', '点赞', '点踩', '分享',
                     '在线体验', '模型体验', '温度', '最大输出', '推理', '在线咨询', '客服', '满意度反馈'];
    for (const mk of markers) {
        const idx = rest.indexOf('\\n' + mk);
        if (idx > 0) rest = rest.slice(0, idx);
        const idx2 = rest.indexOf('\\n' + mk + '\\n');
        if (idx2 > 0) rest = rest.slice(0, idx2);
    }
    const lines = rest.split('\\n').filter(l => l.trim() !== '');
    while (lines.length && /volcengine|ark/i.test(lines[0]) && lines[0].length < 30) lines.shift();
    return lines.join('\\n').trim();
}"""


async def run_chat(prompt: str, model: str) -> AsyncGenerator[str, None]:
    model_id = ARK_MODELS.get(model) or ARK_MODELS[DEFAULT_MODEL]
    mgr = browser_manager
    page = await mgr.ensure()
    if not mgr.logged_in:
        raise HTTPException(status_code=503, detail="ark 需要登录：GET /login.png 扫码")
    async with mgr._chat_lock:
        t0 = time.time()
        await page.goto(page_url(model_id), timeout=90_000, wait_until="domcontentloaded")
        await page.wait_for_timeout(2500)

        frame, editor = await mgr.find_input()
        if editor is None:
            raise RuntimeError("找不到聊天输入框（页面结构变化或登录失效）")
        await editor.click()
        await frame.keyboard.insert_text(prompt) if hasattr(frame, "keyboard") else await page.keyboard.insert_text(prompt)
        await page.wait_for_timeout(400)
        await page.keyboard.press("Enter")
        await page.wait_for_timeout(800)

        # 定位包含正文的 frame
        target = None
        for _ in range(40):
            for fr in [page, *page.frames]:
                try:
                    if await fr.evaluate(FIND_PROMPT_FRAME_JS, prompt):
                        target = fr
                        break
                except Exception:
                    continue
            if target:
                break
            await page.wait_for_timeout(500)
        if target is None:
            raise RuntimeError("未定位到聊天正文区")

        last = ""
        stable_since = None
        first_chunk_at = None
        deadline = time.time() + SEND_TIMEOUT_S
        while time.time() < deadline:
            try:
                cur = await target.evaluate(EXTRACT_JS, prompt)
            except Exception:
                cur = None
            if cur and cur.startswith(last) and len(cur) > len(last):
                if first_chunk_at is None:
                    first_chunk_at = time.time() - t0
                yield cur[len(last):]
                last = cur
                stable_since = time.time()
            elif cur and not cur.startswith(last):
                yield cur
                last = cur
                stable_since = time.time()
            if last and stable_since and (time.time() - stable_since) * 1000 >= STABLE_MS:
                break
            await page.wait_for_timeout(POLL_MS)
        if not last:
            raise RuntimeError("回复内容为空")
        logger.info(f"[Chat] 首字 {first_chunk_at}s | 全程 {time.time() - t0:.1f}s | 长度 {len(last)}")


browser_manager = BrowserManager()


@asynccontextmanager
async def lifespan(_: FastAPI):
    if not ARK_API_KEY:
        await browser_manager.ensure()
    yield


app = FastAPI(title="ark-free-api", version="0.1.0", lifespan=lifespan)


def check_key(auth: HTTPAuthorizationCredentials = Depends(bearer)):
    if not auth or auth.credentials not in API_KEYS:
        raise HTTPException(status_code=403, detail="invalid api_key")


@app.get("/healthz")
async def healthz():
    return {"ok": True, "logged_in": browser_manager.logged_in,
            "mode": "api-key" if ARK_API_KEY else "web"}


@app.get("/login.png")
async def login_png():
    p = Path("/app/login.png")
    if not p.exists():
        raise HTTPException(status_code=404, detail="无登录截图（可能已登录）")
    return FileResponse(p, media_type="image/png")


@app.get("/v1/models")
def models():
    data = [{"id": m, "object": "model", "owned_by": "ark-free-api"} for m in ARK_MODELS]
    return {"data": data, "object": "list"}


def _sse(cid: str, created: int, model: str, delta: dict = None, finish: str = None) -> str:
    choice = {"index": 0, "delta": delta or {}, "finish_reason": finish}
    return "data: " + json.dumps({"id": cid, "object": "chat.completion.chunk",
                                  "created": created, "model": model,
                                  "choices": [choice]}, ensure_ascii=False) + "\n\n"


@app.post("/v1/chat/completions")
async def chat(req: ChatCompletionRequest, _=Depends(check_key)):
    prompt = "\n\n".join(
        f"[系统指令] {m.content}" if m.role == "system"
        else f"[你的上一轮回答] {m.content}" if m.role == "assistant"
        else m.content
        for m in req.messages)
    if not prompt.strip():
        raise HTTPException(status_code=400, detail="empty messages")

    # 官方 API key 模式：直接转发（流式透传）
    if ARK_API_KEY:
        headers = {"Authorization": f"Bearer {ARK_API_KEY}"}
        payload = {"model": ARK_MODELS.get(req.model, ARK_MODELS[DEFAULT_MODEL]),
                   "messages": [m.model_dump() for m in req.messages],
                   "stream": bool(req.stream)}
        if req.stream:
            async def proxy():
                async with httpx.AsyncClient(timeout=180) as client:
                    async with client.stream("POST", f"{ARK_API_BASE}/chat/completions",
                                             headers=headers, json=payload) as r:
                        async for line in r.aiter_lines():
                            yield line + "\n\n" if line.startswith("data:") else ""
            return StreamingResponse(proxy(), media_type="text/event-stream")
        async with httpx.AsyncClient(timeout=180) as client:
            r = await client.post(f"{ARK_API_BASE}/chat/completions", headers=headers, json=payload)
            return r.json()

    if req.stream:
        cid = f"ark-{int(time.time())}"
        created = int(time.time())

        async def sse():
            yield _sse(cid, created, req.model, {"role": "assistant"})
            try:
                async for delta in run_chat(prompt, req.model):
                    yield _sse(cid, created, req.model, {"content": delta})
            except HTTPException as e:
                yield _sse(cid, created, req.model, {"content": f"\n[适配器错误] {e.detail}"})
            except Exception as e:
                yield _sse(cid, created, req.model, {"content": f"\n[适配器错误] {e}"})
            yield _sse(cid, created, req.model, None, "stop")
            yield "data: [DONE]\n\n"

        return StreamingResponse(sse(), media_type="text/event-stream")

    try:
        parts = []
        async for delta in run_chat(prompt, req.model):
            parts.append(delta)
        text = "".join(parts)
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))
    now = int(time.time())
    return {"id": f"ark-{now}", "object": "chat.completion", "created": now,
            "model": req.model,
            "choices": [{"index": 0, "message": {"role": "assistant", "content": text},
                         "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000)
