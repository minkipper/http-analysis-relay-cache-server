#!/usr/bin/env python3
"""
Demo origin server for testing the caching proxy (plain HTTP, local only).

Serves one page with twelve 4K photos (demo/images, credits in demo/CREDITS.md)
and delays every response to imitate a slow, far-away website, so the proxy
cache effect is visible: first load is slow, reload through the proxy cache is instant.

Run:  python3 demo/demo_server.py          (http://127.0.0.1:9000)
"""
import argparse
import html
import http.server
import pathlib
import re
import time

DEMO_DIR = pathlib.Path(__file__).resolve().parent
IMAGE_DIR = DEMO_DIR / "images"


def load_credits():
    """Read `file -> "author, license"` from the table rows in CREDITS.md."""
    credits = {}
    for line in (DEMO_DIR / "CREDITS.md").read_text(encoding="utf-8").splitlines():
        m = re.match(r"\| `([^`]+)` \| .* \| (.*) \| (.*) \|$", line)
        if m:
            credits[m.group(1)] = f"{m.group(2)}, {m.group(3)}"
    return credits


PAGE = """<!doctype html>
<html lang="ko"><head><meta charset="utf-8"><title>Proxy Cache Demo</title>
<style>
 body {{ font-family: sans-serif; margin: 24px; background: #f4f4f6; color: #222; }}
 #time {{ font-size: 40px; font-weight: bold; margin: 8px 0 16px; }}
 button, a.btn {{ font-size: 16px; padding: 8px 14px; margin-right: 8px; }}
 .grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 12px; margin-top: 16px; }}
 figure {{ margin: 0; }}
 figure img {{ width: 100%; border-radius: 6px; background: #ddd; aspect-ratio: 3 / 2; object-fit: cover; }}
 figcaption {{ font-size: 11px; color: #666; margin-top: 2px; }}
</style></head>
<body>
<h1>HTTP 프록시 캐시 테스트 페이지</h1>
<p>4K 사진 {count}장 (총 약 {total_mb:.1f} MB). 원 서버는 요청마다 {delay}초씩 일부러 늦게 응답합니다.</p>
<div id="time">로딩 중...</div>
<button onclick="location.reload()">다시 불러오기</button>
<a class="btn" href="http://game.com/">차단 사이트 열기 (game.com)</a>
<div class="grid">{figures}</div>
<p style="font-size:12px;color:#666">사진: Wikimedia Commons Featured pictures (작가·라이선스는 각 사진 아래 및 demo/CREDITS.md)</p>
<script>
 window.addEventListener("load", () => {{
   const s = (performance.now() / 1000).toFixed(2);
   document.getElementById("time").textContent = "이번 로딩: " + s + "초";
 }});
</script>
</body></html>
"""


def main():
    parser = argparse.ArgumentParser(description="Slow demo origin server for the caching proxy")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--delay", type=float, default=1.0, help="seconds to wait before each response")
    args = parser.parse_args()

    images = {f"/img/{p.name}": p.read_bytes() for p in sorted(IMAGE_DIR.glob("*.jpg"))}
    credits = load_credits()
    total_mb = sum(len(b) for b in images.values()) / 1024 / 1024
    figures = "".join(
        f'<figure><img src="{path}" alt="{html.escape(path[5:])}">'
        f"<figcaption>{html.escape(credits.get(path[5:], ''))}</figcaption></figure>"
        for path in images)
    page = PAGE.format(count=len(images), total_mb=total_mb, delay=args.delay, figures=figures).encode("utf-8")

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            time.sleep(args.delay)  # imitate a slow, distant origin server
            path = self.path.split("?", 1)[0]
            if path == "/":
                body, ctype = page, "text/html; charset=utf-8"
            elif path in images:
                body, ctype = images[path], "image/jpeg"
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            # Browser must re-ask every time (max-age=0), shared caches like our proxy may keep it (s-maxage).
            self.send_header("Cache-Control", "public, max-age=0, s-maxage=3600")
            self.end_headers()
            self.wfile.write(body)

    server = http.server.ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"Demo page: http://127.0.0.1:{args.port}/  ({len(images)} photos, {total_mb:.1f} MB, delay {args.delay}s)")
    print("Run the proxy with a cache larger than the photos, e.g. --max-bytes 100000000")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down")


if __name__ == "__main__":
    main()
