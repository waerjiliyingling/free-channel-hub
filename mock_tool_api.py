"""本地 mock 上游：模拟一个支持 tool_calls 的官方 API 渠道。

用途：验证 free-channel-hub 能否正确透传并聚合 tool_calls。
因为本机没有可用的官方 API key，用一个假上游来验证 hub 的
协议处理逻辑（这才是 hub 的职责）。

用法： python mock_tool_api.py [port]
"""

import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

TOOL_CALL_CHUNKS = [
    {"id": "chatcmpl-mock1", "object": "chat.completion.chunk", "created": 1700000000,
     "model": "mock-official", "choices": [{"index": 0, "delta": {"role": "assistant"},
                                            "finish_reason": None}]},
    {"id": "chatcmpl-mock1", "object": "chat.completion.chunk", "created": 1700000000,
     "model": "mock-official", "choices": [{"index": 0, "delta": {"tool_calls": [
        {"index": 0, "id": "call_abc123", "type": "function",
         "function": {"name": "get_", "arguments": ""}}]}, "finish_reason": None}]},
    {"id": "chatcmpl-mock1", "object": "chat.completion.chunk", "created": 1700000000,
     "model": "mock-official", "choices": [{"index": 0, "delta": {"tool_calls": [
        {"index": 0, "function": {"name": "weather", "arguments": "{\"ci"}}]},
        "finish_reason": None}]},
    {"id": "chatcmpl-mock1", "object": "chat.completion.chunk", "created": 1700000000,
     "model": "mock-official", "choices": [{"index": 0, "delta": {"tool_calls": [
        {"index": 0, "function": {"name": "", "arguments": "ty\":\"北京\"}"}}]},
        "finish_reason": None}]},
    {"id": "chatcmpl-mock1", "object": "chat.completion.chunk", "created": 1700000000,
     "model": "mock-official", "choices": [{"index": 0, "delta": {},
                                            "finish_reason": "tool_calls"}]},
]


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/v1/models":
            return self._json({"object": "list", "data": [
                {"id": "mock-official", "object": "model", "owned_by": "mock"}]})
        self._json({"error": "not found"}, 404)

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        req = json.loads(self.rfile.read(n) or b"{}")

        if self.path != "/v1/chat/completions":
            return self._json({"error": "not found"}, 404)

        # 校验 hub 是否把 tools / temperature 原样透传过来
        if "tools" not in req:
            return self._json({"error": "hub dropped tools field"}, 400)
        if req["tools"][0]["function"]["name"] != "get_weather":
            return self._json({"error": "tools mangled by hub"}, 400)
        if req.get("temperature") != 0.3:
            return self._json({"error": "hub dropped temperature"}, 400)

        if req.get("stream"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            for c in TOOL_CALL_CHUNKS:
                self.wfile.write(
                    f"data: {json.dumps(c, ensure_ascii=False)}\n\n".encode())
            self.wfile.write(b"data: [DONE]\n\n")
            return

        # 非流式：故意返回 SSE 文本，验证 hub 的归一化能力
        sse = "".join(f"data: {json.dumps(c, ensure_ascii=False)}\n\n"
                      for c in TOOL_CALL_CHUNKS) + "data: [DONE]\n\n"
        body = sse.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    print(f"mock upstream listening on 0.0.0.0:{port}", flush=True)
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()
