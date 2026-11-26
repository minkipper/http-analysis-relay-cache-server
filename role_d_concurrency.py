#!/usr/bin/env python3
"""
[Role D: 다중 접속 제어, Lock 동기화, 전체 통합, 벤치마킹 및 Upstage Solar 에이전트]
- 역할 D가 담당하는 동시성 제어(워커 스레딩, 뮤텍스 Lock, 데드락/경쟁 상태 방지),
  모듈 통합 파이프라인(역할 A · B · C 어댑터 호환), tail latency(p50/p95) 부하 벤치마킹,
  Upstage Solar Tool-Calling 기반 자율 진단 에이전트를 구현한 모듈입니다.
- 같은 폴더의 팀원 모듈과 자동 연동됩니다:
  * 역할 A (proxy.py / socket_core.py): TCP 송수신 및 Origin 릴레이
  * 역할 B (http_parser.py):            HTTP 요청 파싱 및 블랙리스트 필터
  * 역할 C (lru_cache.py):              바이트 기반 LRU 캐시 및 캐시 정책
"""

import argparse
from contextlib import contextmanager
import http.server
import json
import os
import socket
import threading
import time
import urllib.error
import urllib.request

# =====================================================================
# 1. 팀원 A, B, C 모듈 연결 인터페이스 & 호환 어댑터
# =====================================================================
def _extract_path_and_version(head: bytes, url: str = "") -> tuple:
    """HTTP 요청 첫 줄과 URL에서 origin-form 경로(path)와 HTTP 버전을 안전하게 추출합니다."""
    first_line = head.split(b"\r\n", 1)[0].decode("latin-1", errors="ignore")
    parts = first_line.split(" ", 2)
    version = parts[2].strip() if len(parts) >= 3 and parts[2].strip().startswith("HTTP/") else "HTTP/1.1"
    raw_target = (url or (parts[1] if len(parts) >= 2 else "/")).strip()
    raw_target = raw_target.split("#", 1)[0]

    if raw_target.lower().startswith("http://"):
        rest = raw_target[7:]
        cuts = [i for i in (rest.find("/"), rest.find("?")) if i != -1]
        cut = min(cuts) if cuts else len(rest)
        path = rest[cut:]
        if not path.startswith("/"):
            path = "/" + path
    elif raw_target.startswith("/"):
        path = raw_target
    else:
        path = "/"
    return path, version


def _extract_host_header(head: bytes) -> tuple:
    """상대 경로(origin-form) 요청 시 Host 헤더에서 (host, port)를 보조 추출합니다."""
    for line in head.split(b"\r\n")[1:]:
        name, _, value = line.partition(b":")
        if name.strip().lower() == b"host":
            host_val = value.decode("latin-1", errors="ignore").strip()
            if not host_val:
                return "", 80
            host, colon, port_str = host_val.rpartition(":")
            if not colon or "]" in port_str:
                return host_val, 80
            port = int(port_str)
            if not 1 <= port <= 65535:
                raise ValueError(f"Invalid port in Host header: {port}")
            return host.strip(), port
    return "", 80


def _fallback_recv_request(conn: socket.socket, buf_size: int = 4096) -> tuple:
    """proxy.py 미존재 시 동작하는 순수 stdlib TCP HTTP 요청 수신기."""
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = conn.recv(buf_size)
        if not chunk:
            return b"", b""
        data += chunk

    head, _, body = data.partition(b"\r\n\r\n")
    length = 0
    for line in head.split(b"\r\n")[1:]:
        name, _, value = line.partition(b":")
        if name.strip().lower() == b"content-length":
            length = int(value.strip())
    while len(body) < length:
        chunk = conn.recv(buf_size)
        if not chunk:
            break
        body += chunk
    return head, body


def _fallback_send_error(conn: socket.socket, code: int, reason: str) -> None:
    """proxy.py 미존재 시 동작하는 HTTP 에러 응답 송신기."""
    body = f"{code} {reason}\n".encode("utf-8")
    response = (
        f"HTTP/1.1 {code} {reason}\r\n"
        f"Content-Type: text/plain; charset=utf-8\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"Connection: close\r\n\r\n"
    ).encode("latin-1") + body
    try:
        conn.sendall(response)
    except OSError:
        pass


# --- 역할 A 연결: socket_core.py 우선, 없으면 실제 팀원 파일 proxy.py 자동 연결 ---
try:
    from proxy import (
        BUF_SIZE as PROXY_BUF_SIZE,
        TIMEOUT as PROXY_TIMEOUT,
        build_upstream_request as _proxy_build_upstream_request,
        recv_request as _proxy_recv_request,
        send_error as _proxy_send_error,
        tunnel as _proxy_tunnel,
    )
    _HAS_ROLE_A_PROXY = True
except ImportError:
    PROXY_BUF_SIZE = 4096
    PROXY_TIMEOUT = 10
    _proxy_build_upstream_request = None
    _proxy_recv_request = None
    _proxy_send_error = None
    _proxy_tunnel = None
    _HAS_ROLE_A_PROXY = False

recv_http_request = _proxy_recv_request or _fallback_recv_request
send_http_error = _proxy_send_error or _fallback_send_error

try:
    from socket_core import forward_to_origin
except ImportError:
    if _HAS_ROLE_A_PROXY:
        def forward_to_origin(host: str, port: int, request_bytes: bytes):
            """
            역할 A(proxy.py)의 build_upstream_request/TIMEOUT/BUF_SIZE를 재사용하고,
            TCP Handshake RTT 측정 및 캐시 저장용 전체 응답 바이트 수집을 수행하는 릴레이 함수.
            """
            req_head, _, req_body = request_bytes.partition(b"\r\n\r\n")
            first_line = req_head.split(b"\r\n", 1)[0].decode("latin-1", errors="ignore")
            parts = first_line.split(" ", 2)
            method = parts[0].strip() if parts and parts[0].strip() else "GET"
            raw_url = parts[1].strip() if len(parts) >= 2 else "/"
            path, version = _extract_path_and_version(req_head, raw_url)
            upstream_req = _proxy_build_upstream_request(req_head, req_body, method, path, version)

            t_conn = time.perf_counter()
            with socket.create_connection((host, port), timeout=PROXY_TIMEOUT) as upstream:
                handshake_rtt_ms = (time.perf_counter() - t_conn) * 1000.0
                upstream.settimeout(PROXY_TIMEOUT)
                upstream.sendall(upstream_req)
                chunks = []
                while True:
                    chunk = upstream.recv(PROXY_BUF_SIZE)
                    if not chunk:
                        break
                    chunks.append(chunk)
            return b"".join(chunks), handshake_rtt_ms
    else:
        def forward_to_origin(host: str, port: int, request_bytes: bytes):
            raise NotImplementedError("역할 A 모듈(proxy.py 또는 socket_core.py)이 아직 연결되지 않았습니다.")

# --- 역할 B 연결: http_parser.py ---
try:
    from http_parser import parse_http_request
except ImportError:
    def parse_http_request(raw_bytes: bytes):
        raise NotImplementedError("역할 B 모듈(http_parser.py -> parse_http_request)이 아직 연결되지 않았습니다.")

# --- 역할 C 연결: lru_cache.py ---
try:
    from lru_cache import LRUCache, is_cacheable, key_from_url, make_key
except ImportError:
    class LRUCache:
        def __init__(self, max_bytes: int = 10 * 1024 * 1024, max_item_bytes: int = None):
            self.max_bytes = max_bytes
            self.max_item_bytes = max_item_bytes

        def get(self, key: str):
            raise NotImplementedError("역할 C 모듈(lru_cache.py -> LRUCache.get)이 아직 연결되지 않았습니다.")

        def put(self, key: str, value: bytes) -> bool:
            raise NotImplementedError("역할 C 모듈(lru_cache.py -> LRUCache.put)이 아직 연결되지 않았습니다.")

        def stats(self) -> dict:
            raise NotImplementedError("역할 C 모듈(lru_cache.py -> LRUCache.stats)이 아직 연결되지 않았습니다.")

    def make_key(host: str, port: int, path: str) -> str:
        raise NotImplementedError("역할 C 모듈(lru_cache.py -> make_key)이 아직 연결되지 않았습니다.")

    def key_from_url(url: str):
        raise NotImplementedError("역할 C 모듈(lru_cache.py -> key_from_url)이 아직 연결되지 않았습니다.")

    def is_cacheable(method: str, response: bytes) -> bool:
        raise NotImplementedError("역할 C 모듈(lru_cache.py -> is_cacheable)이 아직 연결되지 않았습니다.")


def _percentile(values: list, pct: float) -> float:
    """순수 Python 기반 백분위수(p50, p95 등) 계산 헬퍼."""
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    rank = (pct / 100.0) * (len(ordered) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    frac = rank - lo
    return ordered[lo] + (ordered[hi] - ordered[lo]) * frac


# =====================================================================
# 2. [역할 D 핵심 1] 스레드 동기화, 뮤텍스 Lock 경합 계측 & 동적 정책 제어
# =====================================================================
class ThreadSafeSynchronizer:
    """
    다수의 스레드가 동시에 역할 C의 LRUCache 및 통계 지표에 접근할 때
    경쟁 상태(Race Condition)와 교착 상태(Deadlock)를 방지하는 동기화 클래스.
    """

    def __init__(self, cache_engine, max_threads: int = 64):
        self.cache_engine = cache_engine
        self.cache_lock = threading.Lock()
        self.stats_lock = threading.Lock()
        self._idle_cond = threading.Condition(self.stats_lock)
        self.thread_limiter = threading.BoundedSemaphore(max_threads)

        # cache_lock 내부에서만 읽고 쓰는 Lock 경합 계측 값 (중첩 Lock 방지)
        self._lock_acquisitions = 0
        self._lock_wait_total_ms = 0.0
        self._lock_wait_max_ms = 0.0

        self._stats = {
            "total_requests": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "cache_uncacheable": 0,
            "cache_rejected_oversize": 0,
            "blocked_requests": 0,
            "errors": 0,
            "active_threads": 0,
            "peak_threads": 0,
            "saved_bytes": 0,
            "hit_latencies_ms": [],
            "miss_latencies_ms": [],
            "handshake_rtts_ms": [],
        }

    @contextmanager
    def _timed_cache_lock(self):
        """캐시 Lock 획득 대기 시간을 계측하면서 안전하게 임계 영역을 보호합니다."""
        t0 = time.perf_counter()
        self.cache_lock.acquire()
        wait_ms = (time.perf_counter() - t0) * 1000.0
        try:
            self._lock_acquisitions += 1
            self._lock_wait_total_ms += wait_ms
            if wait_ms > self._lock_wait_max_ms:
                self._lock_wait_max_ms = wait_ms
            yield
        finally:
            self.cache_lock.release()

    def safe_cache_get(self, cache_key: str):
        """
        역할 C의 LRUCache.get()은 적중 시 노드를 이중 연결 리스트의 맨 앞(MRU)으로 이동시키므로
        단순 읽기가 아닌 쓰기 연산입니다. 따라서 반드시 뮤텍스 Lock으로 보호하여 조회합니다.
        """
        with self._timed_cache_lock():
            return self.cache_engine.get(cache_key)

    def safe_cache_put(self, cache_key: str, response_bytes: bytes) -> bool:
        """뮤텍스 Lock으로 보호된 상태에서 LRU 캐시 삽입 및 바이트 기반 퇴출(Eviction)"""
        with self._timed_cache_lock():
            return bool(self.cache_engine.put(cache_key, response_bytes))

    def safe_cache_stats(self):
        """뮤텍스 Lock으로 보호된 상태에서 역할 C LRU 캐시 내부 통계 조회"""
        with self._timed_cache_lock():
            if hasattr(self.cache_engine, "stats"):
                return self.cache_engine.stats()
            return None

    def _snapshot_cache_and_lock_metrics(self) -> tuple:
        """단일 cache_lock 진입으로 캐시 엔진 통계와 Lock 경합 통계를 함께 스냅샷합니다."""
        with self._timed_cache_lock():
            engine_stats = self.cache_engine.stats() if hasattr(self.cache_engine, "stats") else None
            acqs = self._lock_acquisitions
            avg_wait = (self._lock_wait_total_ms / acqs) if acqs > 0 else 0.0
            lock_metrics = {
                "cache_lock_acquisitions": acqs,
                "avg_cache_lock_wait_ms": round(avg_wait, 4),
                "max_cache_lock_wait_ms": round(self._lock_wait_max_ms, 4),
            }
            return engine_stats, lock_metrics

    def safe_update_cache_policy(self, max_bytes: int = None, max_item_bytes: int = None) -> dict:
        """뮤텍스 Lock 보호 하에 런타임 캐시 용량(max_bytes, max_item_bytes)을 동적 조정합니다."""
        if max_bytes is not None and max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        if max_item_bytes is not None and max_item_bytes <= 0:
            raise ValueError("max_item_bytes must be positive")

        with self._timed_cache_lock():
            evicted_on_resize = 0
            if max_bytes is not None and hasattr(self.cache_engine, "max_bytes"):
                self.cache_engine.max_bytes = max_bytes
                if hasattr(self.cache_engine, "max_item_bytes"):
                    self.cache_engine.max_item_bytes = min(self.cache_engine.max_item_bytes, max_bytes)
            if max_item_bytes is not None and hasattr(self.cache_engine, "max_item_bytes"):
                current_max = getattr(self.cache_engine, "max_bytes", max_item_bytes)
                self.cache_engine.max_item_bytes = min(max_item_bytes, current_max)

            # max_bytes 축소 시 현재 사용량이 새 상한을 넘으면 LRU 꼬리부터 즉시 퇴출
            if (
                hasattr(self.cache_engine, "_bytes")
                and hasattr(self.cache_engine, "max_bytes")
                and hasattr(self.cache_engine, "_tail")
                and hasattr(self.cache_engine, "_head")
                and hasattr(self.cache_engine, "_remove")
            ):
                while (
                    self.cache_engine._bytes > self.cache_engine.max_bytes
                    and self.cache_engine._tail.prev is not self.cache_engine._head
                ):
                    self.cache_engine._remove(self.cache_engine._tail.prev)
                    if hasattr(self.cache_engine, "_evictions"):
                        self.cache_engine._evictions += 1
                    evicted_on_resize += 1

            engine_stats = self.cache_engine.stats() if hasattr(self.cache_engine, "stats") else None
            return {
                "max_bytes": getattr(self.cache_engine, "max_bytes", None),
                "max_item_bytes": getattr(self.cache_engine, "max_item_bytes", None),
                "evicted_on_resize": evicted_on_resize,
                "cache_engine_stats": engine_stats,
            }

    def safe_cache_clear(self) -> None:
        """뮤텍스 Lock으로 보호된 상태에서 캐시 초기화"""
        with self._timed_cache_lock():
            if hasattr(self.cache_engine, "clear"):
                self.cache_engine.clear()
            else:
                max_bytes = getattr(self.cache_engine, "max_bytes", 10 * 1024 * 1024)
                max_item_bytes = getattr(self.cache_engine, "max_item_bytes", None)
                self.cache_engine = type(self.cache_engine)(
                    max_bytes=max_bytes,
                    max_item_bytes=max_item_bytes,
                )

    def on_thread_start(self) -> None:
        """활성 워커 스레드 수 및 최대 동시 실행 스레드 수 기록"""
        with self.stats_lock:
            self._stats["active_threads"] += 1
            if self._stats["active_threads"] > self._stats["peak_threads"]:
                self._stats["peak_threads"] = self._stats["active_threads"]

    def on_thread_end(self) -> None:
        """워커 스레드 종료 시 활성 스레드 수 감소 및 idle 대기자 알림"""
        with self._idle_cond:
            self._stats["active_threads"] = max(0, self._stats["active_threads"] - 1)
            if self._stats["active_threads"] == 0:
                self._idle_cond.notify_all()

    def wait_for_idle(self, timeout: float = 2.0) -> bool:
        """실행 중인 모든 워커 스레드가 정리될 때까지 대기 (Graceful Shutdown)"""
        deadline = time.monotonic() + timeout
        with self._idle_cond:
            while self._stats["active_threads"] > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._idle_cond.wait(timeout=remaining)
            return True

    def record_hit(self, latency_ms: float, byte_len: int) -> None:
        with self.stats_lock:
            self._stats["total_requests"] += 1
            self._stats["cache_hits"] += 1
            self._stats["saved_bytes"] += byte_len
            self._stats["hit_latencies_ms"].append(round(latency_ms, 3))

    def record_miss(self, latency_ms: float, handshake_rtt_ms: float = 0.0) -> None:
        with self.stats_lock:
            self._stats["total_requests"] += 1
            self._stats["cache_misses"] += 1
            self._stats["miss_latencies_ms"].append(round(latency_ms, 3))
            self._stats["handshake_rtts_ms"].append(round(handshake_rtt_ms, 3))

    def record_cache_uncacheable(self) -> None:
        """is_cacheable() 정책상 저장하지 않은 응답 건수 기록"""
        with self.stats_lock:
            self._stats["cache_uncacheable"] += 1

    def record_cache_rejected(self) -> None:
        """max_item_bytes 초과 등으로 put()이 거부한 응답 건수 기록"""
        with self.stats_lock:
            self._stats["cache_rejected_oversize"] += 1

    def record_blocked(self) -> None:
        with self.stats_lock:
            self._stats["total_requests"] += 1
            self._stats["blocked_requests"] += 1

    def record_error(self) -> None:
        with self.stats_lock:
            self._stats["total_requests"] += 1
            self._stats["errors"] += 1

    def get_benchmark_report(self) -> dict:
        """스레드 안전하게 최종 성능, 백분위수 지연(p50/p95), Lock 경합 및 캐시 적중률 리포트 생성"""
        # 중첩 Lock(Deadlock 위험)을 원천 차단하기 위해 cache_lock 조회를 stats_lock 밖에서 먼저 수행
        cache_engine_stats, lock_metrics = self._snapshot_cache_and_lock_metrics()

        with self.stats_lock:
            total = self._stats["total_requests"]
            hits = self._stats["cache_hits"]
            misses = self._stats["cache_misses"]
            hit_lats = list(self._stats["hit_latencies_ms"])
            miss_lats = list(self._stats["miss_latencies_ms"])
            rtts = list(self._stats["handshake_rtts_ms"])

            avg_hit = sum(hit_lats) / len(hit_lats) if hit_lats else 0.0
            avg_miss = sum(miss_lats) / len(miss_lats) if miss_lats else 0.0
            avg_rtt = sum(rtts) / len(rtts) if rtts else 0.0
            speedup = (avg_miss / avg_hit) if avg_hit > 0 and avg_miss > 0 else 0.0

            return {
                "total_requests": total,
                "cache_hits": hits,
                "cache_misses": misses,
                "cache_uncacheable": self._stats["cache_uncacheable"],
                "cache_rejected_oversize": self._stats["cache_rejected_oversize"],
                "hit_ratio_pct": round((hits / (hits + misses)) * 100, 2) if (hits + misses) > 0 else 0.0,
                "blocked_requests": self._stats["blocked_requests"],
                "errors": self._stats["errors"],
                "active_threads": self._stats["active_threads"],
                "peak_concurrent_threads": self._stats["peak_threads"],
                "saved_bandwidth_bytes": self._stats["saved_bytes"],
                "avg_hit_latency_ms": round(avg_hit, 3),
                "p50_hit_latency_ms": round(_percentile(hit_lats, 50), 3),
                "p95_hit_latency_ms": round(_percentile(hit_lats, 95), 3),
                "avg_miss_latency_ms": round(avg_miss, 3),
                "p50_miss_latency_ms": round(_percentile(miss_lats, 50), 3),
                "p95_miss_latency_ms": round(_percentile(miss_lats, 95), 3),
                "avg_tcp_handshake_rtt_ms": round(avg_rtt, 3),
                "cache_speedup_factor": round(speedup, 2),
                **lock_metrics,
                "cache_engine_stats": cache_engine_stats,
            }


# =====================================================================
# 3. [역할 D 핵심 2] 멀티스레드 프록시 메인 루프 & 전체 모듈 오케스트레이션
# =====================================================================
class MultiThreadedProxyServer:
    """
    다중 클라이언트 접속을 스레드로 병렬 처리하며,
    역할 B(파서/필터) -> 역할 D+C(Lock 기반 캐시 조회) -> 역할 A(소켓 릴레이) 파이프라인을 조율합니다.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8080,
        cache_max_bytes: int = 10 * 1024 * 1024,
        cache_max_item_bytes: int = None,
        max_threads: int = 64,
        single_threaded: bool = False,
        disable_cache: bool = False,
        parser_fn=None,
        forward_fn=None,
        cache_engine=None,
        is_cacheable_fn=None,
        key_from_url_fn=None,
        make_key_fn=None,
        recv_fn=None,
        send_error_fn=None,
    ):
        self.host = host
        self.port = port
        self.single_threaded = single_threaded
        self.disable_cache = disable_cache

        # 역할 A, B, C 구현체 연결
        self.parse_http_request = parser_fn or parse_http_request
        self.forward_to_origin = forward_fn or forward_to_origin
        self.is_cacheable = is_cacheable_fn or is_cacheable
        self.key_from_url = key_from_url_fn or key_from_url
        self.make_key = make_key_fn or make_key
        self.recv_request = recv_fn or recv_http_request
        self.send_error = send_error_fn or send_http_error

        self.sync = ThreadSafeSynchronizer(
            cache_engine=cache_engine or LRUCache(
                max_bytes=cache_max_bytes,
                max_item_bytes=cache_max_item_bytes,
            ),
            max_threads=max_threads,
        )

        self._stop_event = threading.Event()
        self._ready_event = threading.Event()
        self._startup_error = None
        self.bound_port = None
        self.server_socket = None

    def _normalize_parsed_request(self, parsed, head: bytes):
        """
        역할 B(http_parser.py)의 dict 반환 형식과 기존 5-튜플 반환 형식을 모두 수용하고,
        누락된 path / version / 상대 경로 Host 헤더를 안전하게 정규화합니다.
        """
        if parsed is None:
            return None

        if isinstance(parsed, dict):
            method = (parsed.get("method") or "").strip().upper()
            host = (parsed.get("host") or "").strip()
            port = parsed.get("port", 80)
            url_key = (parsed.get("url") or parsed.get("url_key") or "").strip()
            is_blocked = bool(parsed.get("is_blocked", False))
            path = parsed.get("path")
            version = parsed.get("version")
        elif isinstance(parsed, (tuple, list)) and len(parsed) >= 5:
            method = (parsed[0] or "").strip().upper()
            host = (parsed[1] or "").strip()
            port = parsed[2]
            url_key = (parsed[3] or "").strip()
            is_blocked = bool(parsed[4])
            path = parsed[5] if len(parsed) >= 6 else None
            version = parsed[6] if len(parsed) >= 7 else None
        else:
            return None

        if not method:
            return None

        port = int(port)
        if not 1 <= port <= 65535:
            raise ValueError(f"Port out of range: {port}")

        extracted_path, extracted_version = _extract_path_and_version(head, url_key)
        path = path or extracted_path
        version = version or extracted_version

        # 역할 B가 상대 경로("GET /path HTTP/1.1")에서 host를 비워둘 경우 Host 헤더로 보완
        if not host and method != "CONNECT" and url_key.startswith("/"):
            host, hdr_port = _extract_host_header(head)
            if host and port == 80:
                port = hdr_port

        return {
            "method": method,
            "host": host,
            "port": port,
            "url_key": url_key,
            "is_blocked": is_blocked,
            "path": path,
            "version": version,
        }

    def _resolve_cache_key(self, host: str, port: int, url_key: str, path: str = None):
        """역할 C의 key_from_url() / make_key() 규칙으로 캐시 키를 정규화합니다."""
        if url_key:
            normalized = self.key_from_url(url_key)
            if normalized is not None:
                return normalized
        if path or (url_key and url_key.startswith("/")):
            return self.make_key(host, port, path or url_key)
        return None

    def client_worker_thread(self, client_socket: socket.socket, client_addr) -> None:
        """개별 스레드가 실행하는 클라이언트 1건 처리 파이프라인"""
        t_start = time.perf_counter()
        self.sync.on_thread_start()
        try:
            client_socket.settimeout(float(PROXY_TIMEOUT))

            # 0) [역할 A] TCP 바이트 스트림에서 완전한 HTTP 요청(헤더 + 본문) 수신
            try:
                head, body = self.recv_request(client_socket)
            except ValueError:
                self.sync.record_error()
                self.send_error(client_socket, 400, "Bad Request")
                return
            except OSError:
                # 브라우저 사전 연결(preconnect) 후 조용히 끊긴 경우
                return

            if not head:
                return

            request_bytes = head + b"\r\n\r\n" + body

            # 1) [역할 B] HTTP 프로토콜 파싱 및 유해 사이트 필터링
            try:
                raw_parsed = self.parse_http_request(request_bytes)
                info = self._normalize_parsed_request(raw_parsed, head)
            except ValueError:
                self.sync.record_error()
                self.send_error(client_socket, 400, "Bad Request")
                return

            if not info or not info["host"] or not info["url_key"]:
                self.sync.record_error()
                self.send_error(client_socket, 400, "Bad Request")
                return

            method = info["method"]
            host = info["host"]
            port = info["port"]
            url_key = info["url_key"]
            path = info["path"]
            is_blocked = info["is_blocked"]

            if is_blocked:
                self.sync.record_blocked()
                blocked_body = b"<html><body><h1>403 Forbidden: Blocked by Proxy Filter</h1></body></html>"
                blocked_resp = (
                    f"HTTP/1.1 403 Forbidden\r\n"
                    f"Content-Type: text/html; charset=utf-8\r\n"
                    f"Content-Length: {len(blocked_body)}\r\n"
                    f"Connection: close\r\n\r\n"
                ).encode("latin-1") + blocked_body
                client_socket.sendall(blocked_resp)
                return

            # [역할 A] HTTPS(CONNECT): encrypted end to end, so relay bytes blindly (no parsing/caching)
            if method == "CONNECT":
                if _proxy_tunnel is None:
                    self.send_error(client_socket, 501, "Not Implemented")
                    return
                try:
                    upstream = socket.create_connection((host, port), timeout=float(PROXY_TIMEOUT))
                except socket.timeout:
                    self.sync.record_error()
                    self.send_error(client_socket, 504, "Gateway Timeout")
                    return
                except OSError:
                    self.sync.record_error()
                    self.send_error(client_socket, 502, "Bad Gateway")
                    return
                with upstream:
                    client_socket.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                    if body:  # bytes the client sent right after the CONNECT headers
                        upstream.sendall(body)
                    try:
                        _proxy_tunnel(client_socket, upstream)
                    except OSError:
                        pass  # either side closed or reset the tunnel
                return

            # 2) [역할 D Lock + 역할 C] 정규화된 키로 LRU 캐시 조회
            use_cache = (not self.disable_cache) and (method == "GET")
            cache_key = self._resolve_cache_key(host, port, url_key, path=path) if use_cache else None
            if cache_key is not None:
                cached_bytes = self.sync.safe_cache_get(cache_key)
                if cached_bytes is not None:
                    client_socket.sendall(cached_bytes)
                    elapsed_ms = (time.perf_counter() - t_start) * 1000.0
                    self.sync.record_hit(elapsed_ms, len(cached_bytes))
                    return

            # 3) [역할 A] Cache Miss 시 외부 웹 서버로 TCP 소켓 중계
            try:
                result = self.forward_to_origin(host, port, request_bytes)
            except socket.timeout:
                self.sync.record_error()
                self.send_error(client_socket, 504, "Gateway Timeout")
                return
            except OSError:
                self.sync.record_error()
                self.send_error(client_socket, 502, "Bad Gateway")
                return

            if isinstance(result, tuple):
                response_bytes, handshake_rtt_ms = result
            else:
                response_bytes, handshake_rtt_ms = result, 0.0

            # 4) [역할 D Lock + 역할 C] 캐시 정책(is_cacheable) 검증 후 LRU 캐시에 안전하게 저장
            if cache_key is not None and response_bytes:
                if self.is_cacheable(method, response_bytes):
                    stored = self.sync.safe_cache_put(cache_key, response_bytes)
                    if not stored:
                        self.sync.record_cache_rejected()
                else:
                    self.sync.record_cache_uncacheable()

            # 5) 클라이언트에 최종 응답 전달 및 통계 기록
            if response_bytes:
                client_socket.sendall(response_bytes)
            elapsed_ms = (time.perf_counter() - t_start) * 1000.0
            self.sync.record_miss(elapsed_ms, handshake_rtt_ms)

        except Exception:
            self.sync.record_error()
        finally:
            self.sync.on_thread_end()
            try:
                client_socket.close()
            except Exception:
                pass
            self.sync.thread_limiter.release()

    def wait_until_ready(self, timeout: float = 5.0) -> int:
        """서버 소켓이 bind/listen 완료될 때까지 Event 기반으로 안전하게 대기 후 실제 포트를 반환합니다."""
        if not self._ready_event.wait(timeout=timeout):
            raise TimeoutError("Proxy server did not become ready in time")
        if self._startup_error is not None:
            raise self._startup_error
        return self.bound_port

    def start_server(self) -> None:
        """메인 리스닝 소켓을 열고 접속마다 워커 스레드 생성"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(128)
            self.server_socket.settimeout(1.0)
            self.bound_port = self.server_socket.getsockname()[1]
        except Exception as exc:
            self._startup_error = exc
            self._ready_event.set()
            raise

        self._ready_event.set()

        try:
            while not self._stop_event.is_set():
                try:
                    client_sock, client_addr = self.server_socket.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break

                self.sync.thread_limiter.acquire()
                if self.single_threaded:
                    # 단일 스레드 비교 벤치마크용 순차 실행
                    self.client_worker_thread(client_sock, client_addr)
                else:
                    # 다중 접속 처리를 위한 워커 스레드 생성
                    worker = threading.Thread(
                        target=self.client_worker_thread,
                        args=(client_sock, client_addr),
                        daemon=True,
                    )
                    try:
                        worker.start()
                    except Exception:
                        self.sync.thread_limiter.release()
                        try:
                            client_sock.close()
                        except Exception:
                            pass
        finally:
            if self.server_socket:
                try:
                    self.server_socket.close()
                except Exception:
                    pass

    def stop(self, join_timeout: float = 2.0) -> None:
        """서버 종료 및 진행 중인 워커 스레드 정리 대기"""
        self._stop_event.set()
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass
        if join_timeout > 0:
            self.sync.wait_for_idle(timeout=join_timeout)


# =====================================================================
# 4. [역할 D 핵심 3] 동시성 부하 벤치마킹 (p50/p95 포함)
# =====================================================================
class BenchmarkRunner:
    """
    단일 스레드 vs 멀티스레드 vs 멀티스레드+LRU 캐시 모드의
    총 처리 시간, 초당 처리량(RPS), 평균 및 p50/p95 지연(ms)을 비교 측정합니다.
    """

    @staticmethod
    def _send_one_request(proxy_host: str, proxy_port: int, url: str, host_header: str) -> float:
        t0 = time.perf_counter()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(10.0)
            s.connect((proxy_host, proxy_port))
            req = f"GET {url} HTTP/1.1\r\nHost: {host_header}\r\nConnection: close\r\n\r\n".encode("latin-1")
            s.sendall(req)
            while s.recv(8192):
                pass
        return (time.perf_counter() - t0) * 1000.0

    def run_concurrent_load(
        self,
        proxy_host: str,
        proxy_port: int,
        urls: list,
        host_header: str,
        concurrency: int = 10,
    ) -> dict:
        """지정한 동시 접속 수(concurrency)로 프록시 서버에 병렬 요청 부하를 가합니다."""
        sem = threading.BoundedSemaphore(concurrency)
        latencies = []
        lat_lock = threading.Lock()

        def _client_task(target_url: str):
            with sem:
                lat_ms = self._send_one_request(proxy_host, proxy_port, target_url, host_header)
                with lat_lock:
                    latencies.append(lat_ms)

        t0 = time.perf_counter()
        threads = [threading.Thread(target=_client_task, args=(u,)) for u in urls]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        wall_sec = time.perf_counter() - t0

        return {
            "requests": len(urls),
            "concurrency": concurrency,
            "total_wall_sec": round(wall_sec, 3),
            "throughput_rps": round(len(urls) / wall_sec, 2) if wall_sec > 0 else 0.0,
            "avg_latency_ms": round(sum(latencies) / len(latencies), 2) if latencies else 0.0,
            "p50_latency_ms": round(_percentile(latencies, 50), 2),
            "p95_latency_ms": round(_percentile(latencies, 95), 2),
        }

    def compare_modes(
        self,
        urls: list,
        host_header: str,
        concurrency: int = 10,
        cache_max_bytes: int = 10 * 1024 * 1024,
        cache_max_item_bytes: int = None,
        parser_fn=None,
        forward_fn=None,
    ) -> list:
        """3가지 실행 모드(단일 스레드, 멀티스레드, 멀티스레드+캐시)를 순차 벤치마크"""
        configs = [
            ("1. Single-Thread (No Cache)", True, True),
            ("2. Multi-Thread  (No Cache)", False, True),
            ("3. Multi-Thread + LRU Cache", False, False),
        ]
        results = []
        for title, single_threaded, disable_cache in configs:
            proxy = MultiThreadedProxyServer(
                host="127.0.0.1",
                port=0,
                cache_max_bytes=cache_max_bytes,
                cache_max_item_bytes=cache_max_item_bytes,
                single_threaded=single_threaded,
                disable_cache=disable_cache,
                parser_fn=parser_fn,
                forward_fn=forward_fn,
            )
            srv_thread = threading.Thread(target=proxy.start_server, daemon=True)
            srv_thread.start()
            bound_port = proxy.wait_until_ready()

            load_res = self.run_concurrent_load("127.0.0.1", bound_port, urls, host_header, concurrency)
            report = proxy.sync.get_benchmark_report()
            proxy.stop()

            results.append({"title": title, "load": load_res, "report": report})
        return results

    @staticmethod
    def print_comparison_table(results: list) -> None:
        header = (
            f"{'Scenario':<30} | {'Total Time':>10} | {'Throughput':>12} | "
            f"{'Avg Lat':>10} | {'p95 Lat':>10} | {'Hit Ratio':>9}"
        )
        print(header)
        print("-" * len(header))
        for item in results:
            ld = item["load"]
            rp = item["report"]
            print(
                f"{item['title']:<30} | "
                f"{ld['total_wall_sec']:>8.3f} s | "
                f"{ld['throughput_rps']:>8.2f} r/s | "
                f"{ld['avg_latency_ms']:>7.2f} ms | "
                f"{ld['p95_latency_ms']:>7.2f} ms | "
                f"{rp['hit_ratio_pct']:>8.1f}%"
            )


# =====================================================================
# 5. [역할 D 핵심 4] Upstage Solar Function-Calling 진단 & 튜닝 에이전트
# =====================================================================
def _post_solar_chat(api_key: str, base_url: str, model: str, messages: list, tools: list = None) -> dict:
    """Upstage Solar Chat Completions API 공통 POST 호출 헬퍼."""
    body = {"model": model, "messages": messages, "temperature": 0.2}
    if tools is not None:
        body["tools"] = tools
    payload = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        f"{base_url.rstrip('/')}/chat/completions",
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


class SolarProxyAgent:
    """
    프록시 서버의 성능 지표를 조회하고 캐시 정책을 동적으로 튜닝할 수 있는
    Upstage Solar Tool-Calling 기반 자율 에이전트 (API 키 미설정 시 로컬 규칙 기반 폴백).
    """

    TOOLS = [
        {
            "type": "function",
            "function": {
                "name": "get_proxy_report",
                "description": "현재 멀티스레드 프록시 서버의 요청 수, 캐시 적중률, p50/p95 지연시간, TCP RTT, Lock 경합 및 캐시 엔진 통계를 조회합니다.",
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_cache_config",
                "description": "현재 프록시 서버의 캐시 용량 상한(max_bytes, max_item_bytes) 및 스레딩/캐시 활성화 설정을 조회합니다.",
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "update_cache_policy",
                "description": "런타임 중 LRU 캐시의 최대 용량(max_bytes) 또는 단일 항목 상한(max_item_bytes)을 동적으로 변경합니다.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "max_bytes": {"type": "integer", "description": "새 전체 캐시 용량 상한 (bytes)"},
                        "max_item_bytes": {"type": "integer", "description": "새 단일 항목 상한 (bytes)"},
                    },
                    "required": [],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "reset_cache",
                "description": "현재 LRU 캐시에 저장된 모든 항목을 비웁니다.",
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
        },
    ]

    def __init__(
        self,
        server: MultiThreadedProxyServer = None,
        api_key: str = None,
        model: str = "solar-pro",
        base_url: str = None,
    ):
        self.server = server
        self.api_key = api_key if api_key is not None else os.environ.get("UPSTAGE_API_KEY")
        self.model = model
        self.base_url = (base_url or os.environ.get("UPSTAGE_BASE_URL") or "https://api.upstage.ai/v1").rstrip("/")

    def dispatch_tool(self, name: str, arguments: dict = None) -> dict:
        """에이전트가 선택한 도구 이름과 인자를 실제 프록시 제어 메서드에 안전하게 연결합니다."""
        if self.server is None:
            return {"error": "Proxy server instance is not attached"}

        args = arguments or {}
        try:
            if name == "get_proxy_report":
                return self.server.sync.get_benchmark_report()
            if name == "get_cache_config":
                engine = self.server.sync.cache_engine
                return {
                    "max_bytes": getattr(engine, "max_bytes", None),
                    "max_item_bytes": getattr(engine, "max_item_bytes", None),
                    "disable_cache": self.server.disable_cache,
                    "single_threaded": self.server.single_threaded,
                }
            if name == "update_cache_policy":
                return self.server.sync.safe_update_cache_policy(
                    max_bytes=args.get("max_bytes"),
                    max_item_bytes=args.get("max_item_bytes"),
                )
            if name == "reset_cache":
                self.server.sync.safe_cache_clear()
                return {"cleared": True, "cache_engine_stats": self.server.sync.safe_cache_stats()}
            return {"error": f"Unknown tool: {name}"}
        except Exception as exc:
            return {"error": str(exc)}

    def _call_solar_chat(self, messages: list) -> dict:
        return _post_solar_chat(
            api_key=self.api_key,
            base_url=self.base_url,
            model=self.model,
            messages=messages,
            tools=self.TOOLS,
        )

    def _build_local_diagnosis(self, report: dict, config: dict) -> str:
        """UPSTAGE_API_KEY 미설정 시 로컬 규칙 기반 진단 리포트를 생성합니다."""
        engine = report.get("cache_engine_stats") or {}
        lines = [
            "[Local Agent Diagnosis - UPSTAGE_API_KEY 미설정]",
            (
                f"- 요청 처리: 총 {report['total_requests']}건 "
                f"(HIT {report['cache_hits']} / MISS {report['cache_misses']} / "
                f"적중률 {report['hit_ratio_pct']}%)"
            ),
            (
                f"- 지연 및 동시성: HIT 평균 {report['avg_hit_latency_ms']}ms (p95 {report['p95_hit_latency_ms']}ms) | "
                f"MISS 평균 {report['avg_miss_latency_ms']}ms (p95 {report['p95_miss_latency_ms']}ms) | "
                f"속도 향상 {report['cache_speedup_factor']}x | 최대 동시 스레드 {report['peak_concurrent_threads']}개"
            ),
            (
                f"- TCP Handshake & Lock 경합: 평균 Handshake RTT {report['avg_tcp_handshake_rtt_ms']}ms | "
                f"Lock 획득 {report['cache_lock_acquisitions']}회 "
                f"(평균 대기 {report['avg_cache_lock_wait_ms']}ms, 최대 {report['max_cache_lock_wait_ms']}ms)"
            ),
            (
                f"- 캐시 상태: 사용량 {engine.get('bytes_used', 0)}/{config.get('max_bytes')} bytes | "
                f"Eviction {engine.get('evictions', 0)}회 | "
                f"상한 초과 거부 {report['cache_rejected_oversize']}건 | "
                f"정책상 비캐시 {report['cache_uncacheable']}건"
            ),
        ]
        if report.get("cache_rejected_oversize", 0) > 0:
            lines.append("-> 권고: 단일 항목 상한(max_item_bytes) 초과 거부가 발생했으므로 상향 조정을 검토하세요.")
        if engine.get("evictions", 0) > max(5, report.get("cache_hits", 0)):
            lines.append("-> 권고: 캐시 적중 대비 축출(Eviction)이 빈번하므로 max_bytes 증설을 권장합니다.")
        if report.get("errors", 0) == 0:
            lines.append("-> 안정성: 동시성/소켓 에러 0건으로 정상 동작 중입니다.")
        return "\n".join(lines)

    def run(
        self,
        user_goal: str = "현재 프록시 서버의 동시성, TCP Handshake, LRU 캐시 성능을 진단하고 최적화 방안을 제안해 줘.",
        max_steps: int = 5,
    ) -> dict:
        """도구 호출 루프(Tool-Calling Loop)를 실행하여 프록시 상태를 진단하고 조치를 반환합니다."""
        if not self.api_key:
            report = self.dispatch_tool("get_proxy_report", {})
            config = self.dispatch_tool("get_cache_config", {})
            return {
                "mode": "local_fallback",
                "steps": 1,
                "tool_calls": ["get_proxy_report", "get_cache_config"],
                "summary": self._build_local_diagnosis(report, config),
            }

        messages = [
            {
                "role": "system",
                "content": (
                    "너는 멀티스레드 HTTP 프록시 및 LRU 캐시 성능 분석 에이전트야. "
                    "제공된 도구로 현재 메트릭과 캐시 설정을 먼저 확인한 뒤, "
                    "동시성, TCP Handshake 절감, Lock 경합, LRU 캐시 적중률 관점에서 진단해 줘."
                ),
            },
            {"role": "user", "content": user_goal},
        ]
        called_tools = []
        for step in range(1, max_steps + 1):
            resp = self._call_solar_chat(messages)
            msg = resp["choices"][0]["message"]
            messages.append(msg)
            tool_calls = msg.get("tool_calls") or []
            if not tool_calls:
                return {
                    "mode": "upstage_solar",
                    "steps": step,
                    "tool_calls": called_tools,
                    "summary": msg.get("content") or "",
                }

            for tc in tool_calls:
                fn_name = tc["function"]["name"]
                raw_args = tc["function"].get("arguments") or "{}"
                fn_args = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
                called_tools.append(fn_name)
                tool_result = self.dispatch_tool(fn_name, fn_args)
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "name": fn_name,
                    "content": json.dumps(tool_result, ensure_ascii=False),
                })

        return {
            "mode": "upstage_solar",
            "steps": max_steps,
            "tool_calls": called_tools,
            "summary": "최대 도구 호출 단계(max_steps)에 도달했습니다.",
        }


def analyze_with_solar_agent(report: dict, api_key: str = None) -> str:
    """Upstage Solar API를 호출해 서버 동시성 및 캐시 성능 리포트를 진단합니다 (단발성 호환 API)."""
    api_key = api_key if api_key is not None else os.environ.get("UPSTAGE_API_KEY")
    if not api_key:
        engine = report.get("cache_engine_stats") or {}
        return (
            "[Local Summary - UPSTAGE_API_KEY 미설정]\n"
            f"요청 {report['total_requests']}건 | 적중률 {report['hit_ratio_pct']}% | "
            f"p95 HIT {report.get('p95_hit_latency_ms', 0)}ms / MISS {report.get('p95_miss_latency_ms', 0)}ms | "
            f"최대 동시 스레드 {report['peak_concurrent_threads']}개 | "
            f"속도 향상 {report['cache_speedup_factor']}배 | "
            f"Lock 대기 평균 {report.get('avg_cache_lock_wait_ms', 0)}ms | "
            f"Eviction {engine.get('evictions', 0)}회"
        )

    base_url = os.environ.get("UPSTAGE_BASE_URL", "https://api.upstage.ai/v1").rstrip("/")
    data = _post_solar_chat(
        api_key=api_key,
        base_url=base_url,
        model="solar-pro",
        messages=[
            {"role": "system", "content": "너는 멀티스레드 HTTP 프록시 및 LRU 캐시 성능 분석 에이전트야."},
            {
                "role": "user",
                "content": (
                    "다음 프록시 서버 성능 측정 결과를 바탕으로 스레드 동시성, TCP Handshake 절감, "
                    f"Lock 경합, LRU 캐시 적중률 개선 포인트를 요약해 줘:\n{json.dumps(report, ensure_ascii=False)}"
                ),
            },
        ],
    )
    return data["choices"][0]["message"]["content"]


# =====================================================================
# 6. 로컬 모의 Origin 서버, 7단계 통합 자체 테스트(--self-test) & 벤치마크(--bench)
# =====================================================================
def _start_mock_origin(delay_sec: float = 0.0):
    """외부 인터넷 없이 4인 모듈 통합/벤치마크를 검증하기 위한 로컬 HTTP Origin 서버."""
    state = {"calls": 0, "lock": threading.Lock()}

    class _MockOriginHandler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format, *args):
            return

        def do_GET(self):
            with state["lock"]:
                state["calls"] += 1
            if delay_sec > 0:
                time.sleep(delay_sec)

            if self.path.startswith("/nostore"):
                body = b"no-store-secret"
                self.send_response(200)
                self.send_header("Cache-Control", "no-store")
            elif self.path.startswith("/cookie"):
                body = b"cookie-session"
                self.send_response(200)
                self.send_header("Set-Cookie", "session=abc123")
            elif self.path.startswith("/404"):
                body = b"not-found"
                self.send_response(404)
            elif self.path.startswith("/huge"):
                body = b"H" * 2048
                self.send_response(200)
            else:
                body = f"ok:{self.path}".encode("utf-8")
                self.send_response(200)
                self.send_header("Cache-Control", "public, max-age=60")

            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)

    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _MockOriginHandler)
    th = threading.Thread(target=httpd.serve_forever, daemon=True)
    th.start()
    return httpd, httpd.server_address[1], state


def _send_raw_proxy_request(proxy_port: int, request_line: str, extra_headers: str = "") -> bytes:
    with socket.create_connection(("127.0.0.1", proxy_port), timeout=5.0) as s:
        raw = f"{request_line}\r\n{extra_headers}Connection: close\r\n\r\n".encode("latin-1")
        s.sendall(raw)
        chunks = []
        while True:
            data = s.recv(4096)
            if not data:
                break
            chunks.append(data)
        return b"".join(chunks)


def _send_connect_tunnel_request(proxy_port: int, origin_port: int, path: str) -> bytes:
    """CONNECT through the proxy, then send a plain GET inside the tunnel (mock origin speaks plain HTTP)."""
    with socket.create_connection(("127.0.0.1", proxy_port), timeout=5.0) as s:
        s.sendall(f"CONNECT 127.0.0.1:{origin_port} HTTP/1.1\r\nHost: 127.0.0.1:{origin_port}\r\n\r\n".encode("latin-1"))
        chunks = [s.recv(4096)]  # "200 Connection Established" arrives before the origin sends anything
        s.sendall(f"GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n".encode("latin-1"))
        while True:
            data = s.recv(4096)
            if not data:
                break
            chunks.append(data)
        return b"".join(chunks)


def run_self_tests() -> None:
    """역할 A · B · C · D 전체 모듈 통합 검증 (7단계)."""
    def check(name: str, cond: bool):
        status = "PASS" if cond else "FAIL"
        print(f"[{status}] {name}")
        if not cond:
            raise AssertionError(f"Self-test failed: {name}")

    origin, origin_port, origin_state = _start_mock_origin(delay_sec=0.015)
    try:
        proxy = MultiThreadedProxyServer(
            host="127.0.0.1",
            port=0,
            cache_max_bytes=1200,
            cache_max_item_bytes=512,
            max_threads=64,
        )
        srv_thread = threading.Thread(target=proxy.start_server, daemon=True)
        srv_thread.start()
        proxy_port = proxy.wait_until_ready()

        # [1] 4인 모듈(A + B + C + D) End-to-End 연동: 1st MISS -> 2nd HIT
        r1 = _send_raw_proxy_request(
            proxy_port,
            f"GET http://127.0.0.1:{origin_port}/ok HTTP/1.1",
            f"Host: 127.0.0.1:{origin_port}\r\n",
        )
        r2 = _send_raw_proxy_request(
            proxy_port,
            f"GET http://127.0.0.1:{origin_port}/ok HTTP/1.1",
            f"Host: 127.0.0.1:{origin_port}\r\n",
        )
        check("1. 4-role E2E integration (1st MISS -> 2nd HIT, origin called once)",
              b"ok:/ok" in r1 and r1 == r2 and origin_state["calls"] == 1)

        # [2] 역할 B 블랙리스트 차단(403, CONNECT 포함), 비정상 포트(400), HTTPS CONNECT 터널(200)
        r_blk = _send_raw_proxy_request(proxy_port, "GET http://game.com/play HTTP/1.1", "Host: game.com\r\n")
        r_bad = _send_raw_proxy_request(proxy_port, "GET http://example.com:abc/ HTTP/1.1", "Host: example.com\r\n")
        r_con_blk = _send_raw_proxy_request(proxy_port, "CONNECT game.com:443 HTTP/1.1", "Host: game.com:443\r\n")
        r_con = _send_connect_tunnel_request(proxy_port, origin_port, "/tunnel")
        check("2. Role B blacklist (403), invalid port (400), HTTPS CONNECT tunnel (200)",
              r_blk.startswith(b"HTTP/1.1 403 Forbidden")
              and r_bad.startswith(b"HTTP/1.1 400 Bad Request")
              and r_con_blk.startswith(b"HTTP/1.1 403 Forbidden")
              and r_con.startswith(b"HTTP/1.1 200 Connection Established")
              and b"ok:/tunnel" in r_con)

        # [3] 역할 C URL 정규화(#frag) 및 is_cacheable 정책 필터링 (no-store, Set-Cookie, 404, oversize)
        r_frag = _send_raw_proxy_request(
            proxy_port,
            f"GET http://127.0.0.1:{origin_port}/ok#section1 HTTP/1.1",
            f"Host: 127.0.0.1:{origin_port}\r\n",
        )
        calls_before_policies = origin_state["calls"]
        _send_raw_proxy_request(proxy_port, f"GET http://127.0.0.1:{origin_port}/nostore HTTP/1.1", f"Host: 127.0.0.1:{origin_port}\r\n")
        _send_raw_proxy_request(proxy_port, f"GET http://127.0.0.1:{origin_port}/cookie HTTP/1.1", f"Host: 127.0.0.1:{origin_port}\r\n")
        _send_raw_proxy_request(proxy_port, f"GET http://127.0.0.1:{origin_port}/404 HTTP/1.1", f"Host: 127.0.0.1:{origin_port}\r\n")
        _send_raw_proxy_request(proxy_port, f"GET http://127.0.0.1:{origin_port}/huge HTTP/1.1", f"Host: 127.0.0.1:{origin_port}\r\n")
        rep_3 = proxy.sync.get_benchmark_report()
        check("3. Role C URL normalization (#frag) & is_cacheable/oversize enforcement",
              r_frag == r1 and calls_before_policies == 2  # /ok once (then cached) + /tunnel once in test 2
              and rep_3["cache_hits"] == 2
              and rep_3["cache_uncacheable"] == 3
              and rep_3["cache_rejected_oversize"] == 1)

        # [4] 32-스레드 동시 부하 스트레스 테스트 (실제 프록시 소켓 경유)
        barrier = threading.Barrier(32)
        stress_results = []
        stress_lock = threading.Lock()

        def stress_worker(idx: int):
            barrier.wait(timeout=5.0)
            resp = _send_raw_proxy_request(
                proxy_port,
                f"GET http://127.0.0.1:{origin_port}/item/{idx % 8} HTTP/1.1",
                f"Host: 127.0.0.1:{origin_port}\r\n",
            )
            with stress_lock:
                stress_results.append(resp)

        threads = [threading.Thread(target=stress_worker, args=(i,)) for i in range(32)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        rep_4 = proxy.sync.get_benchmark_report()
        eng_4 = rep_4["cache_engine_stats"]
        check("4. 32-thread concurrency stress test (peak > 1, bytes <= max_bytes, no corruption)",
              len(stress_results) == 32
              and all(b"ok:/item/" in r for r in stress_results)
              and rep_4["peak_concurrent_threads"] > 1
              and rep_4["cache_lock_acquisitions"] > 0
              and eng_4["bytes_used"] <= eng_4["max_bytes"])

        # [5] BenchmarkRunner 3-모드 비교 (Single vs Multi vs Multi+LRU)
        bench_urls = [f"http://127.0.0.1:{origin_port}/b{i % 4}" for i in range(24)]
        bench_results = BenchmarkRunner().compare_modes(
            urls=bench_urls,
            host_header=f"127.0.0.1:{origin_port}",
            concurrency=4,
            cache_max_bytes=1024 * 1024,
        )
        t_single = bench_results[0]["load"]["total_wall_sec"]
        t_lru = bench_results[2]["load"]["total_wall_sec"]
        hit_lru = bench_results[2]["report"]["hit_ratio_pct"]
        check("5. BenchmarkRunner 3-mode comparison (Multi+LRU faster than Single-Thread, hits > 0)",
              t_lru < t_single and hit_lru >= 50.0 and "p95_latency_ms" in bench_results[2]["load"])

        # [6] 런타임 캐시 정책 축소(safe_update_cache_policy) & 초기화(safe_cache_clear) & Graceful stop
        resized = proxy.sync.safe_update_cache_policy(max_bytes=300, max_item_bytes=200)
        after_resize = resized["cache_engine_stats"]
        proxy.sync.safe_cache_clear()
        after_clear = proxy.sync.safe_cache_stats()
        check("6. Dynamic policy resize eviction & safe_cache_clear",
              after_resize["bytes_used"] <= 300
              and resized["max_item_bytes"] == 200
              and after_clear["items"] == 0 and after_clear["bytes_used"] == 0)

        # [7] SolarProxyAgent 도구 디스패치 및 로컬 폴백 진단 테스트
        agent = SolarProxyAgent(server=proxy, api_key="")
        agent_res = agent.run()
        cfg = agent.dispatch_tool("get_cache_config", {})
        check("7. SolarProxyAgent tool dispatch & local diagnostic fallback",
              agent_res["mode"] == "local_fallback"
              and "get_proxy_report" in agent_res["tool_calls"]
              and cfg["max_bytes"] == 300)

        proxy.stop()
        check("Graceful server shutdown idle", proxy.sync.wait_for_idle(timeout=1.0))
    finally:
        origin.shutdown()
        origin.server_close()

    print("ALL 7 SELF-TESTS PASSED!")


def run_benchmark_demo() -> None:
    """로컬 모의 Origin 서버를 대상으로 3-모드 동시성 벤치마크 및 에이전트 진단을 실행합니다."""
    origin, origin_port, _ = _start_mock_origin(delay_sec=0.03)
    try:
        urls = [f"http://127.0.0.1:{origin_port}/asset/{i % 5}" for i in range(24)]
        runner = BenchmarkRunner()
        results = runner.compare_modes(
            urls=urls,
            host_header=f"127.0.0.1:{origin_port}",
            concurrency=8,
            cache_max_bytes=2 * 1024 * 1024,
        )
        print("\n📊 [3-Mode Concurrency & Cache Benchmark]")
        runner.print_comparison_table(results)
        print("\n🤖 [Solar Agent / Local Diagnostic Summary]")
        print(analyze_with_solar_agent(results[-1]["report"]))
    finally:
        origin.shutdown()
        origin.server_close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Role D: Multi-Threaded HTTP Proxy, Lock Sync & Benchmark")
    parser.add_argument("--host", default="127.0.0.1", help="Listen host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8080, help="Listen port (default: 8080)")
    parser.add_argument("--max-bytes", type=int, default=10 * 1024 * 1024, help="LRU max bytes (default: 10MB)")
    parser.add_argument("--max-item-bytes", type=int, default=None, help="LRU max single item bytes")
    parser.add_argument("--max-threads", type=int, default=64, help="Max concurrent worker threads (default: 64)")
    parser.add_argument("--single-thread", action="store_true", help="Run sequentially in single-thread mode")
    parser.add_argument("--no-cache", action="store_true", help="Disable LRU cache")
    parser.add_argument("--self-test", action="store_true", help="Run 7-stage integrated self-tests")
    parser.add_argument("--bench", action="store_true", help="Run 3-mode concurrency benchmark demo")
    args = parser.parse_args()

    if args.self_test:
        run_self_tests()
    elif args.bench:
        run_benchmark_demo()
    else:
        print(f"🚀 [Role D] MultiThreadedProxyServer 시작 -> http://{args.host}:{args.port}")
        server = MultiThreadedProxyServer(
            host=args.host,
            port=args.port,
            cache_max_bytes=args.max_bytes,
            cache_max_item_bytes=args.max_item_bytes,
            max_threads=args.max_threads,
            single_threaded=args.single_thread,
            disable_cache=args.no_cache,
        )
        try:
            server.start_server()
        except KeyboardInterrupt:
            print("\n🛑 서버 종료 중...")
            server.stop()
            final_report = server.sync.get_benchmark_report()
            print(json.dumps(final_report, indent=2, ensure_ascii=False))
            print(SolarProxyAgent(server=server).run()["summary"])
