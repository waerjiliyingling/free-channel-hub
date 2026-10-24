"""free-channel-hub 端到端回归测试

用法： python verify.py [base_url] [api_key]
默认： http://127.0.0.1:8111  sk-hub-local-2026

覆盖：
  1. /healthz
  2. /v1/models
  3. 非流式对话（含 SSE 误返的归一化）
  4. 流式对话（含 model 字段回写）
  5. tools 透传（请求体不被裁剪）
  6. 未知模型 -> 404 且错误信息含可用模型
  7. 错误 api_key -> 403
  8. /admin/channels
"""

import json
import sys
import urllib.error
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8111"
KEY = sys.argv[2] if len(sys.argv) > 2 else "sk-hub-local-2026"

MODEL = "hunyuan"          # 元宝网页渠道，当前可用
passed = failed = 0


def call(path, payload=None, timeout=200, key=KEY, method=None):
    url = f"{BASE}{path}"
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {key}")
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "ignore")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "ignore")
    except Exception as e:
        return 0, f"{type(e).__name__}: {e}"


def check(name, cond, detail=""):
    global passed, failed
    if cond:
        passed += 1
        print(f"  [PASS] {name}")
    else:
        failed += 1
        print(f"  [FAIL] {name} {detail}")


def stream_chat(model, prompt):
    """读 SSE 流，返回 (块数, 累计文本, model 字段集合)"""
    payload = {"model": model,
               "messages": [{"role": "user", "content": prompt}],
               "stream": True}
    url = f"{BASE}/v1/chat/completions"
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), method="POST")
    req.add_header("Authorization", f"Bearer {KEY}")
    req.add_header("Content-Type", "application/json")
    n, text, models = 0, "", set()
    with urllib.request.urlopen(req, timeout=200) as r:
        for raw in r:
            line = raw.decode("utf-8", "ignore").strip()
            if line.startswith("data:") and line != "data: [DONE]":
                n += 1
                try:
                    d = json.loads(line[5:])
                    text += (d["choices"][0].get("delta") or {}).get("content") or ""
                    models.add(d.get("model"))
                except Exception:
                    pass
    return n, text, models


print(f"\n=== free-channel-hub 回归测试  {BASE} ===\n")

print("[1] /healthz")
st, body = call("/healthz", timeout=20)
check("返回 200", st == 200, f"got {st}")
try:
    check("channels > 0", json.loads(body)["channels"] > 0)
except Exception as e:
    check("JSON 可解析", False, str(e))

print("\n[2] /v1/models")
st, body = call("/v1/models", timeout=20)
check("返回 200", st == 200, f"got {st}")
try:
    ids = [m["id"] for m in json.loads(body)["data"]]
    check("模型列表非空", len(ids) > 0)
    print(f"       可用: {', '.join(ids)}")
except Exception as e:
    check("JSON 可解析", False, str(e))

print("\n[3] 非流式对话")
st, body = call("/v1/chat/completions",
                {"model": MODEL,
                 "messages": [{"role": "user", "content": "回复OK两个字"}],
                 "stream": False})
check("返回 200", st == 200, f"got {st}: {body[:200]}")
try:
    d = json.loads(body)
    content = d["choices"][0]["message"]["content"]
    check("正文非空", bool(content.strip()), repr(content[:100]))
    check("未混入 SSE 原始文本", not content.lstrip().startswith("data:"))
    check("model 字段回写", d["model"] == MODEL, f"got {d['model']}")
except Exception as e:
    check("JSON 可解析", False, str(e))

print("\n[4] 流式对话")
try:
    n, text, models = stream_chat(MODEL, "从1数到3")
    check("收到数据块", n > 0)
    check("累计文本非空", bool(text.strip()), repr(text[:100]))
    check("model 字段统一", models == {MODEL}, f"got {models}")
except Exception as e:
    check("流式可用", False, f"{type(e).__name__}: {e}")

print("\n[5] tools 透传")
tools_payload = {
    "model": MODEL,
    "messages": [{"role": "user", "content": "北京天气如何？"}],
    "stream": False,
    "tools": [{"type": "function", "function": {
        "name": "get_weather", "description": "查询城市天气",
        "parameters": {"type": "object",
                       "properties": {"city": {"type": "string"}},
                       "required": ["city"]}}}],
    "tool_choice": "auto",
    "temperature": 0.7,
    "max_tokens": 512,
}
st, body = call("/v1/chat/completions", tools_payload)
check("带 tools 不报参数错误", st in (200, 502), f"got {st}: {body[:200]}")
if st == 200:
    check("返回 200", True)
    print("       注：网页渠道只回纯文本，无 tool_calls 属预期")

print("\n[6] 未知模型 -> 404")
st, body = call("/v1/chat/completions",
                {"model": "definitely-not-a-model-xyz",
                 "messages": [{"role": "user", "content": "hi"}], "stream": False},
                timeout=30)
check("返回 404", st == 404, f"got {st}")
check("错误信息含可用模型", "available" in body.lower() or "channel" in body.lower(),
      body[:150])

print("\n[7] 错误 api_key -> 403")
st, body = call("/v1/chat/completions",
                {"model": MODEL,
                 "messages": [{"role": "user", "content": "hi"}], "stream": False},
                key="sk-wrong-key", timeout=30)
check("返回 403", st == 403, f"got {st}")

print("\n[8] /admin/channels")
st, body = call("/admin/channels", timeout=20)
check("返回 200", st == 200, f"got {st}")
try:
    for c in json.loads(body):
        print(f"       {c['name']:<16} {c['state']:<9} ok={c['ok']} fail={c['fail']}")
except Exception as e:
    check("JSON 可解析", False, str(e))

print(f"\n=== 结果: {passed} 通过, {failed} 失败 ===\n")
sys.exit(1 if failed else 0)
