# [컴퓨터 네트워크 팀 프로젝트 기획안]
## 프로젝트명: 멀티스레드 기반 고성능 HTTP 프록시 및 LRU 캐싱 서버 구현

### 1. 프로젝트 개요 및 목표
* **목표**: HTTP 프로토콜의 동작 원리를 뜯어보고, 네트워크 대역폭 절감 및 응답 속도 향상을 위한 중계 캐시 서버를 순수 파이썬(Python) 소켓 프로그래밍으로 구현한다.
* **개발 환경**: Python 3 표준 라이브러리만 사용 (외부 패키지 설치 없음). 네트워크 통신은 `socket`, 동시성은 `threading`으로 직접 구현. WSL (Ubuntu Linux), macOS 등
* **핵심 기술**: TCP 소켓 양방향 통신, HTTP 메시지 텍스트 파싱, HTTPS CONNECT 터널링, LRU(Least Recently Used) 알고리즘, 스레드(Thread) 동시성 제어 및 Lock 동기화.

### 2. 4인 팀 역할 분담

* **역할 A: 네트워크 소켓 코어 설계**
  * **담당 업무**: 브라우저-프록시-외부 웹 서버를 잇는 양방향 TCP 릴레이 파이프라인 구축. HTTPS 요청(`CONNECT`)은 암호화된 채로 양방향 통과시키는 터널링 처리.
  * **배정 이유**: 네트워크 통신의 가장 기본이 되는 길을 뚫는 작업입니다. 파이썬 소켓 API의 기초적인 사용법과 예외 처리(타임아웃, 연결 끊김)를 다루므로 난이도가 무난합니다.

* **역할 B: HTTP 프로토콜 파서 및 필터**
  * **담당 업무**: A가 뚫어놓은 통로로 오가는 바이트 데이터를 가로채어 HTTP 헤더를 파싱하고 목적지 주소(Host)와 경로(URL)를 추출. 특정 유해 사이트 접속 차단 로직(블랙리스트) 추가.
  * **배정 이유**: 규격화된 텍스트에서 원하는 문자열을 분리해 내는 작업이므로 논리 구조가 명확합니다. 꼼꼼한 문자열 처리 역량이 요구됩니다.

* **역할 C: 캐싱 엔진 및 LRU 자료구조**
  * **담당 업무**: B가 추출한 URL을 기반으로 응답 데이터를 메모리에 임시 저장(Cache). 딕셔너리와 이중 연결 리스트를 조합해 캐시 용량 초과 시 오래된 데이터를 교체하는 LRU 엔진 구현.
  * **배정 이유**: 전공에서 배운 자료구조를 실제 알고리즘으로 밑바닥부터 짜야 하며, 파일 I/O 속도 최적화까지 고려해야 하므로 구현 난이도가 높습니다.

* **역할 D: 다중 접속 제어 및 벤치마킹**
  * **담당 업무**: 다수의 브라우저 탭 접속을 감당하기 위한 스레드 생성(Threading) 및 스레드 간 캐시 데이터 충돌 방지용 뮤텍스(Lock) 구현. 최종 통합 테스트 및 응답 속도 비교 결과 도출.
  * **배정 이유**: 운영체제의 핵심인 동시성 제어를 담당합니다. 디버깅이 가장 까다로운 비동기 에러나 교착 상태(Deadlock)를 방지하며 전체 파이프라인을 조율해야 합니다.

### 3. 개발 파이프라인 (협업 방식)
전체 팀원이 모여 1단계(A의 단일 접속 소켓 통신) 뼈대를 함께 완성해 화면에 웹페이지가 뜨는 것을 먼저 확인합니다. 이후 GitHub를 통해 각자의 모듈(B의 파싱 함수, C의 캐시 엔진, D의 스레딩 구조)을 독립적으로 개발하고 메인 루프에 하나씩 조립해 나가는 방식으로 코드 충돌을 방지합니다.

### 4. 현재 구현 상태

| 파일 | 담당 | 내용 |
|---|---|---|
| `proxy.py` | A | 요청 수신, Origin 요청 변환, 에러 응답, HTTPS CONNECT 터널(`tunnel()`) |
| `http_parser.py` | B | HTTP 요청 파싱, 블랙리스트 차단 판별 |
| `lru_cache.py` | C | 바이트 용량 기반 LRU 캐시, URL 키 정규화, 캐시 정책(`is_cacheable`) |
| `role_d_concurrency.py` | D | 멀티스레드 통합 서버, Lock 동기화, 벤치마크, 자체 테스트(`--self-test`) |
| `integrated_proxy.py` | 통합 | 4개 모듈을 연결해 실행하고 프록시 설정된 크롬 창을 띄우는 실행 파일 |

* **HTTP**: 파싱 → 블랙리스트 차단(403) → LRU 캐시 조회 → (미적중 시) Origin 중계 후 캐시 저장
* **HTTPS**: 블랙리스트 차단(403) → CONNECT 터널로 암호화된 채 통과 (종단 간 암호화로 내용 확인이 불가능하므로 캐시하지 않음)

### 5. 실행 방법

```bash
python3 integrated_proxy.py
```

* `127.0.0.1:8888`에서 프록시가 실행되고, 프록시가 설정된 별도 크롬 창이 열립니다. (`Ctrl+C`로 종료하면 캐시 적중률 등 통계 출력)
* macOS에서는 크롬 자동 실행이 되지 않으므로 다른 터미널에서 직접 실행합니다.

```bash
open -na "Google Chrome" --args --proxy-server="http://127.0.0.1:8888" --user-data-dir=/tmp/chrome-proxy-test
```

* 주요 옵션: `--no-cache` (캐시 끄기), `--single-thread` (단일 스레드), `--no-browser` (크롬 자동 실행 안 함), `--port` (포트 변경)
* 통합 자체 테스트: `python3 role_d_concurrency.py --self-test`

### 6. 다음 단계 (논의 중)
* HTTPS에서도 캐시 효과를 얻기 위해 **지정 도메인만 MITM 복호화 후 정적 리소스(이미지·CSS·JS)를 캐싱**하고, 그 외 HTTPS는 기존 터널로 통과시키는 방향을 검토 중입니다.
* 사설 Root CA 인증서 생성에 `openssl`이 필요하므로, 개발 환경 원칙(표준 라이브러리만 사용)에 대한 예외 여부를 함께 논의합니다.
* MITM 기능은 본인 기기·본인 트래픽에서만 사용하며, 인증서 개인 키는 저장소에 올리지 않고 로그인·결제 사이트는 대상에서 제외합니다.

---

# [Computer Networks Team Project Proposal]
## Project Title: Implementation of a Multi-Threaded High-Performance HTTP Proxy and LRU Caching Server

### 1. Project Overview & Objectives
* **Objective**: Dissect the inner workings of the HTTP protocol and implement a relay cache server from scratch using pure Python socket programming to reduce network bandwidth consumption and improve response latency.
* **Development Environment**: Python 3 standard library only (no third-party packages). Networking is built directly on `socket` and concurrency on `threading`. WSL (Ubuntu Linux), macOS, etc.
* **Key Technologies**: Bidirectional TCP socket communication, HTTP message text parsing, HTTPS CONNECT tunneling, LRU (Least Recently Used) eviction algorithm, thread concurrency control, and mutex (`Lock`) synchronization.

### 2. Team Roles (4 Members)

* **Role A: Network Socket Core Design**
  * **Responsibilities**: Build the bidirectional TCP relay pipeline connecting the client browser, the proxy server, and external origin web servers. Tunnel HTTPS (`CONNECT`) requests through both directions while they stay encrypted.
  * **Rationale**: Establishes the foundational communication path. It covers fundamental Python socket API usage and exception handling (timeouts, connection drops) with a balanced difficulty level.

* **Role B: HTTP Protocol Parser & Filter**
  * **Responsibilities**: Intercept byte streams flowing through Role A's pipeline, parse HTTP headers to extract the destination `Host` and request `URL` path, and implement domain/URL blacklist filtering logic to block restricted websites.
  * **Rationale**: Extracting structured fields from standardized protocol text offers a clear logical flow and requires meticulous string/byte manipulation skills.

* **Role C: Caching Engine & LRU Data Structure**
  * **Responsibilities**: Temporarily store HTTP response payloads in memory based on the URLs extracted by Role B. Implement an LRU cache engine from scratch by combining a hash map (dictionary) and a doubly linked list to evict the least recently used entries when capacity is exceeded.
  * **Rationale**: Requires implementing core data structures and algorithms from the ground up while optimizing memory and I/O performance, representing a high implementation challenge.

* **Role D: Concurrency Control & Benchmarking**
  * **Responsibilities**: Implement multi-threading to handle concurrent browser connections/tabs and apply mutex locks (`Lock`) to prevent race conditions on shared cache data across threads. Lead final system integration testing and benchmark response latency improvements.
  * **Rationale**: Handles operating-system-level concurrency control, preventing subtle asynchronous bugs and deadlocks while coordinating the overall integration pipeline.

### 3. Development Pipeline (Collaboration Workflow)
All team members first collaborate to complete Stage 1 (Role A's single-connection socket relay skeleton) and verify that a web page loads properly in the browser. Afterwards, each member independently develops their assigned module (Role B's parser/filter, Role C's LRU cache engine, Role D's multi-threading & locking architecture) via GitHub and incrementally integrates them into the main server loop to prevent merge conflicts.

### 4. Current Implementation

| File | Role | Contents |
|---|---|---|
| `proxy.py` | A | Request receiving, origin request rewriting, error responses, HTTPS CONNECT tunnel (`tunnel()`) |
| `http_parser.py` | B | HTTP request parsing, blacklist check |
| `lru_cache.py` | C | Byte-bounded LRU cache, URL key normalization, cache policy (`is_cacheable`) |
| `role_d_concurrency.py` | D | Multi-threaded integrated server, lock synchronization, benchmark, self-tests (`--self-test`) |
| `integrated_proxy.py` | Integration | Wires the four modules together and opens a Chrome window configured to use the proxy |

* **HTTP**: parse → blacklist (403) → LRU cache lookup → on miss, relay to origin and store in cache
* **HTTPS**: blacklist (403) → pass through the CONNECT tunnel still encrypted (end-to-end encryption hides the content, so it is not cached)

### 5. How to Run

```bash
python3 integrated_proxy.py
```

* Starts the proxy on `127.0.0.1:8888` and opens a separate Chrome window that uses it (press `Ctrl+C` to stop and print cache statistics).
* On macOS Chrome is not launched automatically; start it from another terminal:

```bash
open -na "Google Chrome" --args --proxy-server="http://127.0.0.1:8888" --user-data-dir=/tmp/chrome-proxy-test
```

* Options: `--no-cache`, `--single-thread`, `--no-browser`, `--port`
* Integrated self-tests: `python3 role_d_concurrency.py --self-test`

### 6. Next Steps (Under Discussion)
* To gain caching benefits for HTTPS as well, we are considering MITM decryption **only for designated domains**, caching their static resources (images, CSS, JS), while all other HTTPS keeps using the existing tunnel.
* Generating a private Root CA requires `openssl`, so an exception to the standard-library-only rule is being discussed.
* MITM is used only on our own devices and traffic; the CA private key is never committed, and login/payment sites are excluded.
