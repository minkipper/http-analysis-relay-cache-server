#!/usr/bin/env python3
"""
[Role D: 다중 접속 제어, Lock 동기화, 전체 통합 및 벤치마킹]
- 본 파일은 오직 역할 D가 담당하는 순수 코드(스레딩, 뮤텍스 Lock, 통합 파이프라인,
  동시성 부하 벤치마크, Upstage Solar 진단 연동)만 깔끔하게 분리해 둔 완성본입니다.
- 나머지 팀원(역할 A, B, C)의 코드가 완성되어 같은 폴더에 놓이면 자동으로 연결됩니다.
  * 역할 A (socket_core.py): forward_to_origin(host, port, request_bytes) -> (response_bytes, handshake_rtt_ms)
  * 역할 B (http_parser.py): parse_http_request(raw_bytes) -> (method, host, port, url_key, is_blocked)
  * 역할 C (lru_cache.py):   LRUCache(max_bytes, max_item_bytes), make_key, key_from_url, is_cacheable
"""

import json
import os
import socket
import threading
import time
import urllib.request

# =====================================================================
# 1. 팀원 A, B, C 모듈 연결 인터페이스 (추후 조원 파일 합칠 때 연결됨)
# =====================================================================
try:
    from socket_core import forward_to_origin
except ImportError:
    def forward_to_origin(host: str, port: int, request_bytes: bytes):
        raise NotImplementedError("역할 A 모듈(socket_core.py -> forward_to_origin)이 아직 연결되지 않았습니다.")

try:
    from http_parser import parse_http_request
except ImportError:
    def parse_http_request(raw_bytes: bytes):
        raise NotImplementedError("역할 B 모듈(http_parser.py -> parse_http_request)이 아직 연결되지 않았습니다.")

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


# =====================================================================
# 2. [역할 D 핵심 1] 스레드 동기화 & 뮤텍스 Lock 관리자
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
        self.thread_limiter = threading.BoundedSemaphore(max_threads)

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

    def safe_cache_get(self, cache_key: str):
        """
        역할 C의 LRUCache.get()은 적중 시 노드를 이중 연결 리스트의 맨 앞(MRU)으로 이동시키므로
        단순 읽기가 아닌 쓰기 연산입니다. 따라서 반드시 뮤텍스 Lock으로 보호하여 조회합니다.
        """
        with self.cache_lock:
            return self.cache_engine.get(cache_key)

    def safe_cache_put(self, cache_key: str, response_bytes: bytes) -> bool:
        """뮤텍스 Lock으로 보호된 상태에서 LRU 캐시 삽입 및 바이트 기반 퇴출(Eviction)"""
        with self.cache_lock:
            return bool(self.cache_engine.put(cache_key, response_bytes))

    def safe_cache_stats(self):
        """뮤텍스 Lock으로 보호된 상태에서 역할 C LRU 캐시 내부 통계 조회"""
        with self.cache_lock:
            if hasattr(self.cache_engine, "stats"):
                return self.cache_engine.stats()
            return None

    def safe_cache_clear(self) -> None:
        """뮤텍스 Lock으로 보호된 상태에서 캐시 초기화"""
        with self.cache_lock:
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
        """워커 스레드 종료 시 활성 스레드 수 감소"""
        with self.stats_lock:
            self._stats["active_threads"] = max(0, self._stats["active_threads"] - 1)

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
        """스레드 안전하게 최종 성능 및 캐시 적중률 리포트 생성"""
        # 중첩 Lock(Deadlock 위험)을 피하기 위해 cache_lock 조회를 stats_lock 밖에서 먼저 수행
        cache_engine_stats = self.safe_cache_stats()

        with self.stats_lock:
            total = self._stats["total_requests"]
            hits = self._stats["cache_hits"]
            misses = self._stats["cache_misses"]
            hit_lats = self._stats["hit_latencies_ms"]
            miss_lats = self._stats["miss_latencies_ms"]
            rtts = self._stats["handshake_rtts_ms"]

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
                "peak_concurrent_threads": self._stats["peak_threads"],
                "saved_bandwidth_bytes": self._stats["saved_bytes"],
                "avg_hit_latency_ms": round(avg_hit, 3),
                "avg_miss_latency_ms": round(avg_miss, 3),
                "avg_tcp_handshake_rtt_ms": round(avg_rtt, 3),
                "cache_speedup_factor": round(speedup, 2),
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
        self.sync = ThreadSafeSynchronizer(
            cache_engine=cache_engine or LRUCache(
                max_bytes=cache_max_bytes,
                max_item_bytes=cache_max_item_bytes,
            ),
            max_threads=max_threads,
        )

        self._stop_event = threading.Event()
        self.server_socket = None

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
            client_socket.settimeout(5.0)
            request_bytes = client_socket.recv(8192)
            if not request_bytes:
                return

            # 1) [역할 B] HTTP 프로토콜 파싱 및 유해 사이트 필터링
            method, host, port, url_key, is_blocked = self.parse_http_request(request_bytes)
            if not host or not url_key:
                self.sync.record_error()
                return

            if method == "CONNECT":
                client_socket.sendall(b"HTTP/1.1 501 Not Implemented\r\nConnection: close\r\n\r\n")
                return

            if is_blocked:
                self.sync.record_blocked()
                blocked_body = b"<html><body><h1>403 Forbidden: Blocked by Proxy Filter</h1></body></html>"
                blocked_resp = (
                    f"HTTP/1.1 403 Forbidden\r\n"
                    f"Content-Type: text/html; charset=utf-8\r\n"
                    f"Content-Length: {len(blocked_body)}\r\n"
                    f"Connection: close\r\n\r\n"
                ).encode("iso-8859-1") + blocked_body
                client_socket.sendall(blocked_resp)
                return

            # 2) [역할 D Lock + 역할 C] 정규화된 키로 LRU 캐시 조회
            use_cache = (not self.disable_cache) and (method == "GET")
            cache_key = self._resolve_cache_key(host, port, url_key) if use_cache else None
            if cache_key is not None:
                cached_bytes = self.sync.safe_cache_get(cache_key)
                if cached_bytes is not None:
                    client_socket.sendall(cached_bytes)
                    elapsed_ms = (time.perf_counter() - t_start) * 1000.0
                    self.sync.record_hit(elapsed_ms, len(cached_bytes))
                    return

            # 3) [역할 A] Cache Miss 시 외부 웹 서버로 TCP 소켓 중계
            result = self.forward_to_origin(host, port, request_bytes)
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

    def start_server(self) -> None:
        """메인 리스닝 소켓을 열고 접속마다 워커 스레드 생성"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(128)
        self.server_socket.settimeout(1.0)

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
                    worker.start()
        finally:
            if self.server_socket:
                self.server_socket.close()

    def stop(self) -> None:
        self._stop_event.set()
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass


# =====================================================================
# 4. [역할 D 핵심 3] 동시성 부하 벤치마킹 & Upstage Solar 진단 연동
# =====================================================================
class BenchmarkRunner:
    """
    단일 스레드 vs 멀티스레드 vs 멀티스레드+LRU 캐시 모드의
    총 처리 시간, 초당 처리량(RPS), 평균 응답 지연(ms)을 비교 측정합니다.
    """

    @staticmethod
    def _send_one_request(proxy_host: str, proxy_port: int, url: str, host_header: str) -> float:
        t0 = time.perf_counter()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5.0)
            s.connect((proxy_host, proxy_port))
            req = f"GET {url} HTTP/1.1\r\nHost: {host_header}\r\nConnection: close\r\n\r\n".encode("iso-8859-1")
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
        }

    def compare_modes(
        self,
        urls: list,
        host_header: str,
        concurrency: int = 10,
        cache_max_bytes: int = 10 * 1024 * 1024,
        cache_max_item_bytes: int = None,
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
            )
            srv_thread = threading.Thread(target=proxy.start_server, daemon=True)
            srv_thread.start()
            while proxy.server_socket is None:
                time.sleep(0.02)
            bound_port = proxy.server_socket.getsockname()[1]

            load_res = self.run_concurrent_load("127.0.0.1", bound_port, urls, host_header, concurrency)
            report = proxy.sync.get_benchmark_report()
            proxy.stop()

            results.append({"title": title, "load": load_res, "report": report})
        return results

    @staticmethod
    def print_comparison_table(results: list) -> None:
        print(f"{'Scenario':<30} | {'Total Time':>10} | {'Throughput':>12} | {'Avg Latency':>12} | {'Hit Ratio':>9}")
        print("-" * 82)
        for item in results:
            ld = item["load"]
            rp = item["report"]
            print(
                f"{item['title']:<30} | "
                f"{ld['total_wall_sec']:>8.3f} s | "
                f"{ld['throughput_rps']:>8.2f} r/s | "
                f"{ld['avg_latency_ms']:>9.2f} ms | "
                f"{rp['hit_ratio_pct']:>8.1f}%"
            )


def analyze_with_solar_agent(report: dict, api_key: str = None) -> str:
    """Upstage Solar API를 호출해 서버 동시성 및 캐시 성능 리포트를 진단합니다."""
    api_key = api_key or os.environ.get("UPSTAGE_API_KEY")
    if not api_key:
        return (
            "[Local Summary - UPSTAGE_API_KEY 미설정]\n"
            f"요청 {report['total_requests']}건 | 적중률 {report['hit_ratio_pct']}% | "
            f"최대 동시 스레드 {report['peak_concurrent_threads']}개 | "
            f"속도 향상 {report['cache_speedup_factor']}배"
        )

    payload = json.dumps({
        "model": "solar-pro",
        "messages": [
            {"role": "system", "content": "너는 멀티스레드 HTTP 프록시 및 LRU 캐시 성능 분석 에이전트야."},
            {
                "role": "user",
                "content": (
                    "다음 프록시 서버 성능 측정 결과를 바탕으로 스레드 동시성, TCP Handshake 절감, "
                    f"LRU 캐시 적중률 개선 포인트를 요약해 줘:\n{json.dumps(report, ensure_ascii=False)}"
                ),
            },
        ],
        "temperature": 0.2,
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://api.upstage.ai/v1/solar/chat/completions",
        data=payload,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read().decode("utf-8"))
        return data["choices"][0]["message"]["content"]


if __name__ == "__main__":
    print("🚀 [Role D] MultiThreadedProxyServer 시작 (역할 A, B, C 모듈 연결 대기 중)")
    server = MultiThreadedProxyServer(host="127.0.0.1", port=8080)
    try:
        server.start_server()
    except KeyboardInterrupt:
        server.stop()
        final_report = server.sync.get_benchmark_report()
        print(json.dumps(final_report, indent=2, ensure_ascii=False))
        print(analyze_with_solar_agent(final_report))
