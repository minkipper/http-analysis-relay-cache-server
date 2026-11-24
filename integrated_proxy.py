#!/usr/bin/env python3
"""
Minimal entry point connecting the 4 GitHub role modules without extra features:
  - Role A (proxy.py): recv_request, build_upstream_request, send_error
  - Role B (http_parser.py): parse_http_request, BLACKLIST
  - Role C (lru_cache.py): LRUCache, is_cacheable, key_from_url
  - Role D (role_d_concurrency.py): MultiThreadedProxyServer, forward_to_origin
"""

import argparse
import json
import os
import shutil
import subprocess
import tempfile
import threading
import time

import http_parser
import lru_cache
import proxy
import role_d_concurrency

DEFAULT_START_URL = "http://example.com"


def find_chrome_executable():
    """Return the installed Google Chrome / Chromium executable path, or None."""
    candidates = [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        os.path.join(os.environ.get("LOCALAPPDATA", ""), r"Google\Chrome\Application\chrome.exe"),
    ]
    for path in candidates:
        if path and os.path.isfile(path):
            return path

    for name in ("chrome", "google-chrome", "chromium", "chromium-browser"):
        found = shutil.which(name)
        if found:
            return found
    return None


def build_chrome_command(
    chrome_path: str,
    proxy_host: str,
    proxy_port: int,
    start_url: str,
    profile_dir: str,
) -> list:
    """Build Chrome CLI arguments forcing HTTP traffic through the local proxy."""
    return [
        chrome_path,
        f"--user-data-dir={profile_dir}",
        f"--proxy-server=http={proxy_host}:{proxy_port}",
        "--disable-features=HttpsUpgrades,HttpsFirstBalancedModeAutoEnable,HttpsFirstModeV2ForEngagedSites,HttpsFirstModeV2ForTypicallySecureUsers",
        "--no-first-run",
        "--no-default-browser-check",
        start_url,
    ]


def launch_chrome(
    proxy_host: str,
    proxy_port: int,
    start_url: str = DEFAULT_START_URL,
):
    """Launch an isolated Chrome window configured to use the proxy server."""
    chrome_path = find_chrome_executable()
    if not chrome_path:
        return None

    profile_dir = os.path.join(tempfile.gettempdir(), f"http_proxy_chrome_{proxy_port}")
    os.makedirs(profile_dir, exist_ok=True)
    cmd = build_chrome_command(chrome_path, proxy_host, proxy_port, start_url, profile_dir)
    subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return chrome_path


def build_integrated_server(
    host: str = proxy.LISTEN_HOST,
    port: int = proxy.LISTEN_PORT,
    max_bytes: int = 10 * 1024 * 1024,
    max_item_bytes: int = None,
    max_threads: int = 64,
    single_threaded: bool = False,
    disable_cache: bool = False,
) -> role_d_concurrency.MultiThreadedProxyServer:
    """Create a MultiThreadedProxyServer explicitly wired to the 4 teammate modules."""
    if not role_d_concurrency._HAS_ROLE_A_PROXY:
        raise RuntimeError("역할 A 모듈(proxy.py)이 연결되지 않았습니다.")

    cache_engine = lru_cache.LRUCache(
        max_bytes=max_bytes,
        max_item_bytes=max_item_bytes,
    )
    return role_d_concurrency.MultiThreadedProxyServer(
        host=host,
        port=port,
        cache_max_bytes=max_bytes,
        cache_max_item_bytes=max_item_bytes,
        max_threads=max_threads,
        single_threaded=single_threaded,
        disable_cache=disable_cache,
        parser_fn=http_parser.parse_http_request,
        forward_fn=role_d_concurrency.forward_to_origin,
        cache_engine=cache_engine,
        is_cacheable_fn=lru_cache.is_cacheable,
        key_from_url_fn=lru_cache.key_from_url,
        recv_fn=proxy.recv_request,
        send_error_fn=proxy.send_error,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Minimal 4-Role Integrated HTTP Relay Cache Server"
    )
    parser.add_argument(
        "--host",
        default=proxy.LISTEN_HOST,
        help=f"Listen host (default: {proxy.LISTEN_HOST})",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=proxy.LISTEN_PORT,
        help=f"Listen port (default: {proxy.LISTEN_PORT})",
    )
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=10 * 1024 * 1024,
        help="LRU max bytes (default: 10MB)",
    )
    parser.add_argument(
        "--max-item-bytes",
        type=int,
        default=None,
        help="LRU max single item bytes",
    )
    parser.add_argument(
        "--max-threads",
        type=int,
        default=64,
        help="Max concurrent worker threads (default: 64)",
    )
    parser.add_argument(
        "--single-thread",
        action="store_true",
        help="Run sequentially in single-thread mode",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable LRU cache",
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_START_URL,
        help=f"Initial URL to open in Chrome (default: {DEFAULT_START_URL})",
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Do not auto-launch Chrome window",
    )
    args = parser.parse_args()

    server = build_integrated_server(
        host=args.host,
        port=args.port,
        max_bytes=args.max_bytes,
        max_item_bytes=args.max_item_bytes,
        max_threads=args.max_threads,
        single_threaded=args.single_thread,
        disable_cache=args.no_cache,
    )

    srv_thread = threading.Thread(target=server.start_server, daemon=True)
    srv_thread.start()
    bound_port = server.wait_until_ready()

    print(
        f"Proxy listening on http://{args.host}:{bound_port} "
        "(wired: proxy.py + http_parser.py + lru_cache.py + role_d_concurrency.py)"
    )
    if not args.no_browser:
        chrome_path = launch_chrome(args.host, bound_port, args.url)
        if chrome_path:
            print(f"Chrome launched -> {args.url} ({chrome_path})")
        else:
            print("Chrome executable not found; proxy server is still running.")

    try:
        while srv_thread.is_alive() and not server._stop_event.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.stop()
        print(
            json.dumps(
                server.sync.get_benchmark_report(),
                indent=2,
                ensure_ascii=False,
            )
        )


if __name__ == "__main__":
    main()
