"""构建前校验：把 app.py 里的 JS 常量抽出来，交给 Node 做真实语法检查。

背景：STREAM_EXTRACT_JS 是 Python 三引号里的 JS。在里面写 '\n' 会变成
真实换行符，嵌进 JS 字符串字面量后造成
SyntaxError: Invalid or unexpected token（只在容器运行时才暴露）。
本脚本在构建前跑，把这类错误拦在本地。

用法： python check_js.py
依赖：Node.js（本机 24.x 可用）
"""

import re
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).parent
APP = HERE / "app.py"

BLOCK_RE = re.compile(r'^([A-Z_][A-Z0-9_]*)\s*=\s*"""(.*?)"""', re.S | re.M)


def looks_like_js(code: str) -> bool:
    return ("=>" in code or "function" in code
            or "document." in code or "Array.from" in code)


def main() -> int:
    src = APP.read_text(encoding="utf-8")
    blocks = BLOCK_RE.findall(src)
    if not blocks:
        print("未找到三引号常量块")
        return 1

    failures = 0
    for name, code in blocks:
        if not looks_like_js(code):
            continue
        # 包成箭头函数交给 node --check 验证语法
        wrapped = f"(() => {{\n{code}\n}})"
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False,
                                         encoding="utf-8") as f:
            f.write(wrapped)
            path = f.name
        r = subprocess.run(["node", "--check", path],
                           capture_output=True, text=True)
        if r.returncode != 0:
            print(f"[FAIL] {name} JS 语法错误：")
            print(r.stderr.strip())
            failures += 1
        else:
            print(f"[OK]   {name} 语法通过（{len(code)} 字符）")
        Path(path).unlink(missing_ok=True)

    if failures:
        print(f"\n共 {failures} 个 JS 块有语法错误，请修复后再构建")
        return 1
    print("\nJS 常量语法检查全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
