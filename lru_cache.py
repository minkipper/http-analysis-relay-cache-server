# ==============================================================================
# 역할 C: 캐싱 엔진 및 LRU 자료구조  (lru_cache.py)
# ==============================================================================
#
# [이 모듈이 하는 일]
#   원 서버에서 받은 HTTP 응답(bytes)을 URL 키로 메모리에 저장해 두었다가,
#   같은 URL 요청이 다시 오면 원 서버에 가지 않고 바로 돌려주게 합니다.
#   저장 공간(바이트 합계)이 가득 차면 "가장 오래 안 쓴(LRU)" 응답부터 지웁니다.
#
# [어떻게 돌아가나]
#   dict 와 이중 연결 리스트를 함께 씁니다.
#     dict        : key -> 노드.  키로 노드를 O(1) 에 찾음
#     연결 리스트 : head <-> 최근 ... 오래됨 <-> tail.  순서 변경·꼬리 삭제를 O(1) 에 함
#   head, tail 은 값이 없는 센티넬(dummy) 노드라서 "리스트가 비었을 때" 같은 예외 분기가 없습니다.
#
#     get(key)        적중 -> 그 노드를 맨 앞(최근)으로 옮기고 값 반환 / 없으면 None
#     put(key, value) 맨 앞에 넣고, 합계가 max_bytes 를 넘으면 꼬리(가장 오래됨)부터 축출
#
#   용량은 "항목 개수"가 아니라 "바이트 합계"로 셉니다(웹 응답은 수백 B ~ 수 MB 로 크기가 제각각).
#   큰 응답 하나가 캐시 전체를 밀어내지 않도록 항목 하나의 상한(max_item_bytes)도 둡니다.
#
# [사용법]
#   from lru_cache import LRUCache, make_key, is_cacheable
#
#   cache = LRUCache(max_bytes=10 * 1024 * 1024)      # 전체 10 MB, 항목 하나 2.5 MB 까지
#   key = make_key(host, port, path)                  # 예: "http://example.com/a"
#
#   hit = cache.get(key)
#   if hit is not None:
#       conn.sendall(hit)                             # 캐시 적중: 원 서버에 안 감
#   else:
#       response = ...원 서버에서 받은 응답 전체(bytes)...
#       if is_cacheable(method, response):
#           cache.put(key, response)
#
#   cache.stats()  ->  {"items", "bytes_used", "max_bytes", "hits", "misses", "evictions", "rejected"}
#
#   자체 테스트:  python3 lru_cache.py
#
# [주의: 스레드 안전하지 않음 (역할 D)]
#   이 클래스는 락이 없습니다. 여러 스레드가 같은 인스턴스를 쓰면 get 과 put 둘 다 Lock 으로 감싸야 합니다.
#   get 도 노드를 맨 앞으로 옮기므로 "읽기"가 아니라 "쓰기"입니다.
#
# [TEMP(B): 캐시 키 함수가 두 개인 이유]
#   B 의 parse_http_request() 가 아직 proxy.py 에 연결되지 않아 키를 어떤 값으로 만들지 미정입니다.
#     (가) B 의 dict 에 "path" 필드가 추가되면 -> make_key(info["host"], info["port"], info["path"])
#     (나) B 의 "url" 을 그대로 쓰기로 하면     -> key_from_url(info["url"])
#   두 함수는 같은 자원에 같은 키를 냅니다(자체 테스트 5번). 팀이 정하면 하나만 남깁니다.
# ==============================================================================


class _Node:
    """이중 연결 리스트의 노드 하나 = 캐시 항목 하나."""
    __slots__ = ("key", "value", "size", "prev", "next")

    def __init__(self, key=None, value=b""):
        self.key = key
        self.value = value
        self.size = len(value)
        self.prev = None
        self.next = None


class LRUCache:
    """
    바이트 용량 기준 LRU 캐시.

    [인자]
        max_bytes (int):      저장된 응답 크기 합계의 상한
        max_item_bytes (int): 항목 하나의 상한 (기본값: max_bytes 의 1/4, 최소 1).
                              max_bytes 보다 크게 주면 max_bytes 로 줄임
                              (그보다 큰 항목은 넣자마자 자기 자신까지 축출되기 때문)
    """

    def __init__(self, max_bytes=10 * 1024 * 1024, max_item_bytes=None):
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        if max_item_bytes is None:
            max_item_bytes = max(1, max_bytes // 4)
        if max_item_bytes <= 0:
            raise ValueError("max_item_bytes must be positive")
        self.max_bytes = max_bytes
        self.max_item_bytes = min(max_item_bytes, max_bytes)

        self._map = {}  # key -> _Node
        self._head = _Node()  # 센티넬: head.next 가 가장 최근
        self._tail = _Node()  # 센티넬: tail.prev 가 가장 오래됨
        self._head.next = self._tail
        self._tail.prev = self._head

        self._bytes = 0
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._rejected = 0  # max_item_bytes 초과로 저장 거부된 횟수

    # ---------- 연결 리스트 기본 연산 ----------
    def _unlink(self, node):
        """노드를 리스트에서 떼어 냄 (dict 와 바이트 합계는 그대로)."""
        node.prev.next = node.next
        node.next.prev = node.prev
        node.prev = node.next = None

    def _push_front(self, node):
        """노드를 head 바로 뒤(가장 최근 자리)에 붙임."""
        node.prev = self._head
        node.next = self._head.next
        self._head.next.prev = node
        self._head.next = node

    def _remove(self, node):
        """노드를 캐시에서 완전히 지움 (리스트 + dict + 바이트 합계)."""
        self._unlink(node)
        del self._map[node.key]
        self._bytes -= node.size

    # ---------- 공개 API ----------
    def get(self, key):
        """적중하면 응답 bytes 를 돌려주고 가장 최근으로 올림. 없으면 None."""
        node = self._map.get(key)
        if node is None:
            self._misses += 1
            return None
        self._unlink(node)
        self._push_front(node)
        self._hits += 1
        return node.value

    def put(self, key, value):
        """
        응답을 저장. 저장했으면 True, max_item_bytes 보다 커서 거부했으면 False.
        같은 키가 이미 있으면 새 응답으로 교체합니다. 새 응답이 너무 커서 거부될 때도
        예전 응답은 지웁니다(서버 내용이 바뀐 것이므로 예전 것을 계속 내주면 안 됨).
        """
        old = self._map.get(key)
        if old is not None:
            self._remove(old)
        if len(value) > self.max_item_bytes:
            self._rejected += 1
            return False

        node = _Node(key, value)
        self._map[key] = node
        self._push_front(node)
        self._bytes += node.size

        # 용량을 넘으면 가장 오래 안 쓴 것부터 축출 (방금 넣은 노드는 맨 앞이라 안전)
        while self._bytes > self.max_bytes:
            self._remove(self._tail.prev)
            self._evictions += 1
        return True

    def stats(self):
        """캐시 상태와 누적 통계 (벤치마크용)."""
        return {
            "items": len(self._map),
            "bytes_used": self._bytes,
            "max_bytes": self.max_bytes,
            "hits": self._hits,
            "misses": self._misses,
            "evictions": self._evictions,
            "rejected": self._rejected,
        }

    def keys_mru_to_lru(self):
        """디버깅·테스트용: 최근 -> 오래된 순서의 키 목록."""
        out, node = [], self._head.next
        while node is not self._tail:
            out.append(node.key)
            node = node.next
        return out

    def __len__(self):
        return len(self._map)


# ==============================================================================
# 캐시 키 함수 (TEMP(B): 팀 결정 뒤 하나만 남김, 맨 위 설명 참고)
# ==============================================================================
def make_key(host, port, path):
    """
    (host, port, path) -> 캐시 키.  [방식 (가)]

    정규화 규칙: host 소문자 + 끝의 점 제거(DNS 상 같은 이름), 기본 포트 80 생략,
    path 는 "/" 로 시작하게 맞추고 대소문자는 유지(서버마다 구분함).
      ("Example.COM.", 80, "/a") -> "http://example.com/a"
    """
    host = host.lower().rstrip(".")
    if not path.startswith("/"):
        path = "/" + path
    netloc = host if port == 80 else f"{host}:{port}"
    return f"http://{netloc}{path}"


def key_from_url(url):
    """
    요청 줄의 URL 문자열 -> 캐시 키.  [방식 (나)]

    절대 형식("http://Host:80/a?q=1")만 받습니다. 키를 만들 수 없으면 None
    ("/a" 처럼 호스트 없는 형식, https 등 다른 스킴, 1-65535 밖이거나 숫자 아닌 포트).
    None 이면 캐시를 건너뛰고 그냥 중계하면 됩니다.
    """
    url = url.split("#", 1)[0]  # "#조각"은 브라우저 안에서만 쓰이고 서버 자원과 무관
    scheme, sep, rest = url.partition("://")
    if not sep or scheme.lower() != "http":
        return None
    # 호스트 부분은 첫 "/" 또는 "?" 앞까지 ("http://a.com?x=1" 처럼 / 없이 쿼리가 올 수 있음)
    cuts = [i for i in (rest.find("/"), rest.find("?")) if i != -1]
    cut = min(cuts) if cuts else len(rest)
    netloc, path = rest[:cut], rest[cut:]
    if not path.startswith("/"):
        path = "/" + path

    host, colon, port_str = netloc.rpartition(":")
    if not colon or "]" in port_str:  # 포트 없음 (IPv6 "[::1]" 의 콜론은 포트가 아님)
        host, port_str = netloc, ""
    if not host:
        return None
    try:
        port = int(port_str) if port_str else 80
    except ValueError:
        return None
    if not 1 <= port <= 65535:
        return None
    return make_key(host, port, path)


# ==============================================================================
# 캐시 정책
# ==============================================================================
def is_cacheable(method, response):
    """
    저장해도 되는 응답인지 판단.
      - GET 요청만 (POST 등은 서버 상태를 바꾸므로 같은 URL 이라도 재사용하면 안 됨)
      - 상태 코드 200 만 (오류·리다이렉트는 저장하지 않음)
      - Cache-Control 에 no-store / private / no-cache 가 있으면 저장 안 함
        (no-cache 는 "매번 서버에 재검증" 이라는 뜻인데, 이 캐시는 재검증을 안 하므로 제외)
      - Set-Cookie 가 있으면 저장 안 함 (한 사람의 로그인 쿠키가 다른 사람에게 가는 것을 막음)
      - 본문이 끝까지 왔는지 확인: Content-Length 만큼 있거나, chunked 의 마지막 조각(0)이 있어야 함
        (원 서버가 중간에 연결을 끊으면 EOF 가 와도 본문이 잘려 있을 수 있음)
    """
    if method != "GET":
        return False
    end = response.find(b"\r\n\r\n")  # partition 은 본문까지 복사하므로 find 로 헤더만 자름
    if end == -1:
        return False  # 헤더 끝을 못 찾음 = 응답이 잘렸음
    head, body_len = response[:end], len(response) - (end + 4)
    lines = head.split(b"\r\n")
    status = lines[0].split(b" ", 2)
    if len(status) < 2 or not status[0].startswith(b"HTTP/") or status[1] != b"200":
        return False

    content_length, chunked = None, False
    for line in lines[1:]:
        name, _, value = line.partition(b":")
        name, value = name.strip().lower(), value.strip().lower()
        if name == b"cache-control":
            directives = {d.strip().split(b"=")[0] for d in value.split(b",")}
            if directives & {b"no-store", b"private", b"no-cache"}:
                return False
        elif name == b"set-cookie":
            return False
        elif name == b"content-length" and value.isdigit():
            content_length = int(value)
        elif name == b"transfer-encoding" and b"chunked" in value:
            chunked = True

    if chunked:
        return response.endswith(b"0\r\n\r\n")
    if content_length is not None:
        return body_len == content_length
    return True  # 길이 정보가 없으면 EOF 가 곧 끝 (A 가 Connection: close 를 붙임)


# ==============================================================================
# [독립 테스트용 코드] python3 lru_cache.py
# 다른 파일에서 import 할 때는 실행되지 않습니다.
# ==============================================================================
if __name__ == "__main__":
    def check(name, cond):
        print(f"[{'PASS' if cond else 'FAIL'}] {name}")
        if not cond:
            raise SystemExit(1)

    print("--- 1. LRU 축출 순서 ---")
    c = LRUCache(max_bytes=30, max_item_bytes=30)
    c.put("a", b"x" * 10)
    c.put("b", b"x" * 10)
    c.put("c", b"x" * 10)            # 30/30 꽉 참
    c.get("a")                        # a 를 최근으로 -> 가장 오래된 건 b
    c.put("d", b"x" * 10)            # 40 > 30 -> b 축출
    check("b 가 축출되고 a 는 남음", c.get("b") is None and c.get("a") is not None)
    check("순서 최근->오래됨", c.keys_mru_to_lru() == ["a", "d", "c"])

    print("--- 2. 바이트 기준 축출 (큰 항목 하나가 여러 개를 밀어냄) ---")
    c = LRUCache(max_bytes=30, max_item_bytes=30)
    for k in "abc":
        c.put(k, b"x" * 10)
    c.put("big", b"x" * 25)          # 55 -> a, b, c 모두 나가야 25
    check("big 만 남음", c.keys_mru_to_lru() == ["big"] and c.stats()["bytes_used"] == 25)

    print("--- 3. 항목 하나 상한 초과는 거부 ---")
    c = LRUCache(max_bytes=100, max_item_bytes=20)
    c.put("a", b"x" * 10)
    check("put 이 False", c.put("huge", b"x" * 21) is False)
    check("기존 항목은 그대로", c.keys_mru_to_lru() == ["a"] and c.stats()["rejected"] == 1)

    print("--- 4. 같은 키 갱신 시 크기 재계산 ---")
    c = LRUCache(max_bytes=100)
    c.put("a", b"x" * 10)
    c.put("a", b"y" * 15)
    check("bytes_used 15, 새 값", c.stats()["bytes_used"] == 15 and c.get("a") == b"y" * 15)

    print("--- 5. 키 정규화: make_key (가) 와 key_from_url (나) 가 같은 키를 내는지 ---")
    check("대소문자·기본 포트", make_key("Example.COM", 80, "/a") == "http://example.com/a")
    check("비기본 포트 유지", make_key("example.com", 8080, "/a") == "http://example.com:8080/a")
    same = [
        ("HTTP://Example.COM:80/a", ("example.com", 80, "/a")),
        ("http://example.com/a?x=1", ("example.com", 80, "/a?x=1")),
        ("http://example.com", ("example.com", 80, "/")),
        ("http://example.com?x=1", ("example.com", 80, "/?x=1")),
        ("http://example.com:8080/Path", ("example.com", 8080, "/Path")),
        ("http://[::1]:8080/a", ("[::1]", 8080, "/a")),
        ("http://[::1]/a", ("[::1]", 80, "/a")),
    ]
    for url, args in same:
        check(f"  {url!r} == make_key{args}", key_from_url(url) == make_key(*args))
    for bad in ["/a", "https://example.com/", "http://example.com:abc/", "http:///a", "example.com/a"]:
        check(f"  {bad!r} -> None", key_from_url(bad) is None)

    print("--- 6. is_cacheable ---")
    ok = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi"
    check("GET 200 저장", is_cacheable("GET", ok))
    check("POST 거부", not is_cacheable("POST", ok))
    check("404 거부", not is_cacheable("GET", b"HTTP/1.1 404 Not Found\r\n\r\n"))
    check("no-store 거부", not is_cacheable(
        "GET", b"HTTP/1.1 200 OK\r\nCache-Control: max-age=0, no-store\r\n\r\n"))
    check("max-age 는 허용", is_cacheable(
        "GET", b"HTTP/1.1 200 OK\r\nCache-Control: public, max-age=60\r\n\r\n"))
    check("잘린 응답 거부", not is_cacheable("GET", b"HTTP/1.1 200 OK\r\nContent-"))

    print("--- 7. 통계와 내부 일관성 ---")
    c = LRUCache(max_bytes=50, max_item_bytes=30)
    for i in range(20):
        c.put(f"k{i % 7}", b"x" * (i % 13 + 1))
        c.get(f"k{(i * 3) % 7}")
    keys = c.keys_mru_to_lru()
    s = c.stats()
    check("리스트와 dict 항목 수 일치", len(keys) == len(c) == s["items"])
    check("bytes_used 가 실제 합과 일치", sum(c._map[k].size for k in keys) == s["bytes_used"])
    check("용량 상한 지킴", s["bytes_used"] <= s["max_bytes"])
    check("get 20번 = hits + misses", s["hits"] + s["misses"] == 20)
    print(s)

    print("--- 8. 코드 리뷰 반영 (회귀 방지) ---")
    c = LRUCache(max_bytes=10, max_item_bytes=20)
    c.put("a", b"x" * 5)
    check("max_item_bytes 가 max_bytes 로 줄어듦", c.max_item_bytes == 10)
    check("전체보다 큰 항목은 거부되고 캐시가 안 비워짐",
          c.put("big", b"x" * 15) is False and c.keys_mru_to_lru() == ["a"])
    check("작은 max_bytes 에서도 항목 상한 최소 1", LRUCache(max_bytes=3).max_item_bytes == 1)
    check("path 에 / 가 없어도 붙여 줌", make_key("a", 80, "b") == "http://a/b")
    check("host 끝의 점 제거", make_key("Example.com.", 80, "/") == "http://example.com/")
    check("#조각 무시 (경로 없음)", key_from_url("http://example.com#f") == "http://example.com/")
    check("#조각 무시 (경로 있음)", key_from_url("http://example.com/a#x") == "http://example.com/a")
    check("포트 범위 밖 -> None",
          key_from_url("http://example.com:0/") is None and key_from_url("http://example.com:70000/") is None)
    check("Set-Cookie 응답 거부", not is_cacheable(
        "GET", b"HTTP/1.1 200 OK\r\nSet-Cookie: session=abc\r\nContent-Length: 2\r\n\r\nhi"))
    check("Content-Length 보다 본문이 짧으면(잘림) 거부", not is_cacheable(
        "GET", b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nhi"))
    check("chunked 가 끝까지 왔으면 저장", is_cacheable(
        "GET", b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nhi\r\n0\r\n\r\n"))
    check("chunked 가 중간에 끊겼으면 거부", not is_cacheable(
        "GET", b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nhi\r\n"))
