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

# 图标行无文字属性，靠结构定位：'其他登录方式'标签下方图标按 x 排序，微信是第 2 个（邮箱、微信、飞书…）
CLICK_WX_ICON_JS = """() => {
    const label = Array.from(document.querySelectorAll('*')).find(
        el => el.childElementCount === 0 && /其他登录方式/.test(el.textContent || ''));
    if (!label) return 'no-label';
    const labelR = label.getBoundingClientRect();
    let container = label.parentElement;
    for (let up = 0; up < 3 && container; up++) {
        const icons = Array.from(container.querySelectorAll('[class*="icon" i], svg, img'))
            .map(el => ({el, r: el.getBoundingClientRect()}))
            .filter(x => x.r.width > 8 && x.r.height > 8 && x.r.y >= labelR.y)
            .sort((a, b) => a.r.x - b.r.x);
        if (icons.length >= 3) { icons[1].el.click(); return 'clicked:' + icons.length; }
        container = container.parentElement;
    }
    return 'not-found';
}"""


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
                    args=["--disable-blink-features=AutomationControlled",
                          "--no-sandbox", "--disable-dev-shm-usage"],
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

    async def _find_qr_page(self):
        """微信登录常弹独立窗口；在所有页面里找含扫码提示的那个"""
        try:
            for p in self._ctx.pages:
                try:
                    txt = await p.evaluate("document.body ? document.body.innerText.slice(0, 800) : ''")
                    if txt and ("扫码" in txt or "二维码" in txt or "QR" in txt):
                        return p
                except Exception:
                    continue
        except Exception:
            pass
        return None

    async def _switch_to_wechat_qr(self):
        """登录弹窗在 shadow DOM 里，原生 JS 查不到——必须用 Playwright 选择器（可穿透 shadow）。
        定位'其他登录方式'文字正下方的图标行，按视觉 x 序点第 2 个（邮箱、微信、飞书…）。"""
        try:
            for frame in [self.page, *self.page.frames]:
                try:
                    label = frame.get_by_text("其他登录方式")
                    if not await label.count():
                        continue
                    lbb = await label.first.bounding_box()
                    icons = frame.locator(
                        '[class*="icon" i]:below(:text("其他登录方式")),'
                        'svg:below(:text("其他登录方式")),'
                        'img:below(:text("其他登录方式"))')
                    n = await icons.count()
                    if n < 3:
                        continue
                    # 只留弹窗内（x 在标签右侧区域）、与标签同一横带（下方100px）的图标
                    boxed = []
                    for i in range(n):
                        bb = await icons.nth(i).bounding_box()
                        if (bb and bb["width"] > 8 and lbb
                                and 0 <= bb["y"] - lbb["y"] <= 100
                                and bb["x"] > lbb["x"] - 40
                                and bb["x"] < lbb["x"] + 400):
                            boxed.append((bb["x"], i))
                    boxed.sort()
                    logger.info(f"[Login] 弹窗内图标 {len(boxed)} 个: {[(round(x), i) for x, i in boxed[:8]]}")
                    # 图标顺序未知（实测第2个开的是抖音 OAuth）——逐个试错：
                    # 点开弹窗 URL 含 weixin/wechat 即命中，否则关掉试下一个
                    for _, idx in boxed[:5]:
                        before = set()
                        for pp in self._ctx.pages:
                            before.add(id(pp))
                        try:
                            await icons.nth(idx).click(timeout=2500)
                        except Exception:
                            try:
                                await icons.nth(idx).click(force=True, timeout=2500)
                            except Exception:
                                continue
                        await self.page.wait_for_timeout(2500)
                        hit = False
                        for p in self._ctx.pages:
                            try:
                                url = p.url
                            except Exception:
                                continue
                            if id(p) in before or not url:
                                continue
                            if "weixin" in url.lower() or "wechat" in url.lower():
                                logger.info(f"[Login] 微信 OAuth 窗口已打开: {url[:90]}")
                                hit = True
                                break
                            logger.info(f"[Login] 跳过非微信窗口: {url[:90]}")
                            try:
                                await p.close()
                            except Exception:
                                pass
                        if hit:
                            return True
                    return False
                except Exception as e:
                    logger.info(f"[Login] wx-icon pierce err: {str(e)[:100]}")
                    continue
        except Exception:
            pass
        return False

    async def _probe_login(self):
        """主动打开登录流：点页面'登录'入口→点微信图标→截码窗。
        返回 ("logged_in"|"need_login", qr_page or None)。"""
        try:
            # 首选：体验页特有的登录弹窗——点发送触发，自带"微信扫码登录"页签
            try:
                eframe, editor = await self.find_input()
                if editor is not None:
                    await editor.click(timeout=3000)
                    await self.page.keyboard.insert_text("登录探测")
                    await self.page.wait_for_timeout(400)
                    send = None
                    for frame in [self.page, *self.page.frames]:
                        for name in ("发送", "发送消息"):
                            loc = frame.get_by_role("button", name=name).first
                            try:
                                if await loc.count() and await loc.is_enabled():
                                    send = loc
                                    break
                            except Exception:
                                continue
                        if send:
                            break
                    if send is not None:
                        await send.click(timeout=3000)
                        await self.page.wait_for_timeout(2500)
                # 点"微信扫码"页签（Playwright 可穿透 shadow DOM）
                for frame in [self.page, *self.page.frames]:
                    try:
                        tab = frame.get_by_text("微信扫码").first
                        if await tab.count():
                            await tab.click(timeout=3000)
                            await self.page.wait_for_timeout(2500)
                            logger.info("[Login] 已点'微信扫码'页签")
                            return ("need_login", self.page)
                    except Exception:
                        continue
            except Exception as e:
                logger.info(f"[Login] send-probe err: {str(e)[:100]}")

            # 兜底：右上角"登录"按钮 → 其他登录方式图标
            for frame in [self.page, *self.page.frames]:
                for sel in ['button:has-text("登录")', 'a:has-text("登录")']:
                    try:
                        loc = frame.locator(sel).first
                        if await loc.count() and await loc.is_visible():
                            await loc.click(timeout=3000)
                            await self.page.wait_for_timeout(2000)
                            await self._switch_to_wechat_qr()
                            qr = await self._find_qr_page()
                            return ("need_login", qr)
                    except Exception:
                        continue
            # 没有独立"登录"按钮 → 已登录
            return ("logged_in", None)
        except Exception as e:
            logger.warning(f"[Login] probe: {e}")
            return ("unknown", None)

    async def _login_watchdog(self):
        """维护登录态并持续产出微信扫码截图"""
        logger.info("[Login] watchdog 启动")
        while True:
            try:
                state, qr = await self._probe_login()
                if state == "logged_in":
                    if not self.logged_in:
                        logger.info("[Login] 登录成功")
                    self.logged_in = True
                else:
                    self.logged_in = False
                    target = qr or self.page
                    Path("/app/login.png").write_bytes(await target.screenshot())
                    logger.info(f"[Login] 未登录，扫码截图已更新（state={state}）")
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


class CookiePayload(BaseModel):
    cookie: str


@app.post("/login/cookies")
async def inject_cookies(payload: CookiePayload):
    """接收浏览器整串 Cookie 头，注入容器 profile（用户在自己浏览器登录后搬迁登录态）"""
    raw = (payload.cookie or "").strip()
    if not raw or "=" not in raw:
        raise HTTPException(status_code=400, detail="cookie 格式不对，应为 a=1; b=2 形式")
    mgr = browser_manager
    await mgr.ensure()
    pairs = []
    for part in raw.split(";"):
        part = part.strip()
        if "=" in part:
            k, _, v = part.partition("=")
            if k.strip():
                pairs.append((k.strip(), v.strip()))
    if not pairs:
        raise HTTPException(status_code=400, detail="未解析出任何 cookie 键值对")
    cookies = []
    for domain in (".volcengine.com",):
        for k, v in pairs:
            cookies.append({"name": k, "value": v, "domain": domain, "path": "/"})
    try:
        await mgr._ctx.add_cookies(cookies)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"注入失败: {e}")
    # 刷新页面让登录态生效
    try:
        await mgr.page.goto(page_url(ARK_MODELS[DEFAULT_MODEL]),
                            timeout=90_000, wait_until="domcontentloaded")
        await mgr.page.wait_for_timeout(3000)
    except Exception:
        pass
    return {"ok": True, "injected": len(pairs),
            "logged_in": mgr.logged_in, "hint": "看 healthz 的 logged_in，或直接发一条对话验证"}


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
