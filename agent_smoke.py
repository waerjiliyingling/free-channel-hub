"""模拟 Agent 的典型调用序列，验证 free-channel-hub 可作为 Agent 后端。

覆盖 Agent 真实会用到的形态：
  1. 带 system prompt 的多轮对话
  2. 带 tools 的请求（含 tool_choice / temperature / max_tokens）
  3. 把工具结果回传给模型（tool role 消息）
  4. 流式 + 增量累积
  5. 并发请求（验证渠道并发上限与粘滞不会串话）

用法： python agent_smoke.py [base_url] [api_key]
"""

import json
import sys
import urllib.error
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8111"
KEY = sys.argv[2] if len(sys.argv) > 2 else "sk-hub-local-2026"
MODEL = "hunyuan"          # 当前可用的网页渠道

passed = failed = 0


def chat(payload, timeout=200):
    req = urllib.request.Request(
        f"{BASE}/v1/chat/completions",
        data=json.dumps(payload).encode(), method="POST")
    req.add_header("Authorization", f"Bearer {KEY}")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode())
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
        print(f"  [FAIL] {name}  {detail}")


print(f"\n=== Agent 冒烟测试  {BASE}  模型 {MODEL} ===\n")

print("[1] system prompt + 多轮上下文")
st, d = chat({"model": MODEL, "stream": False, "messages": [
    {"role": "system", "content": "你是一个简洁的助手，回答不超过20字。"},
    {"role": "user", "content": "记住数字 42"},
    {"role": "assistant", "content": "好的，我记住了 42。"},
    {"role": "user", "content": "刚才的数字是多少？"}]})
check("返回 200", st == 200, f"got {st}: {str(d)[:150]}")
if st == 200:
    txt = d["choices"][0]["message"]["content"]
    check("上下文被传递（提到42）", "42" in txt, repr(txt[:120]))

print("\n[2] tools + 完整 Agent 参数")
st, d = chat({"model": MODEL, "stream": False, "messages": [
    {"role": "user", "content": "北京天气如何？"}],
    "tools": [{"type": "function", "function": {
        "name": "get_weather", "description": "查询城市天气",
        "parameters": {"type": "object",
                       "properties": {"city": {"type": "string"}},
                       "required": ["city"]}}}],
    "tool_choice": "auto", "temperature": 0.3, "max_tokens": 512,
    "top_p": 0.9, "presence_penalty": 0.1})
check("带全套参数不报错", st == 200, f"got {st}: {str(d)[:150]}")

print("\n[3] tool 角色消息回传（Agent 第二步）")
st, d = chat({"model": MODEL, "stream": False, "messages": [
    {"role": "user", "content": "北京天气如何？"},
    {"role": "assistant", "content": "", "tool_calls": [
        {"id": "call_1", "type": "function",
         "function": {"name": "get_weather", "arguments": '{"city":"北京"}'}}]},
    {"role": "tool", "tool_call_id": "call_1", "content": "晴，25℃，微风"}]})
check("tool 消息被接受", st == 200, f"got {st}: {str(d)[:150]}")
if st == 200:
    txt = d["choices"][0]["message"]["content"]
    check("基于工具结果作答", bool(txt.strip()), repr(txt[:120]))
    print(f"       回复: {txt[:100]}")

print("\n[4] 流式增量累积")
payload = {"model": MODEL, "stream": True,
           "messages": [{"role": "user", "content": "从1数到3"}]}
req = urllib.request.Request(f"{BASE}/v1/chat/completions",
                             data=json.dumps(payload).encode(), method="POST")
req.add_header("Authorization", f"Bearer {KEY}")
req.add_header("Content-Type", "application/json")
try:
    with urllib.request.urlopen(req, timeout=200) as r:
        n, text = 0, ""
        for raw in r:
            line = raw.decode("utf-8", "ignore").strip()
            if line.startswith("data:") and line != "data: [DONE]":
                n += 1
                try:
                    text += (json.loads(line[5:])["choices"][0]
                             .get("delta", {}).get("content") or "")
                except Exception:
                    pass
    check("收到多个数据块", n > 1, f"got {n}")
    check("增量可累积", bool(text.strip()), repr(text[:100]))
except Exception as e:
    check("流式可用", False, f"{type(e).__name__}: {e}")

print("\n[5] 并发 3 请求（验证不会互相污染）")
import concurrent.futures


def one(i):
    st, d = chat({"model": MODEL, "stream": False,
                  "messages": [{"role": "user", "content": f"只回复数字{i}"}]})
    if st != 200:
        return i, False, f"HTTP {st}"
    txt = d["choices"][0]["message"]["content"]
    return i, str(i) in txt, txt[:60]


with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
    for i, ok, detail in ex.map(one, [7, 8, 9]):
        check(f"并发请求 {i} 内容正确", ok, str(detail))

print(f"\n=== 结果: {passed} 通过, {failed} 失败 ===\n")
sys.exit(1 if failed else 0)
