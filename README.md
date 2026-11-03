# [컴퓨터 네트워크 팀 프로젝트 기획안]
## 프로젝트명: 멀티스레드 기반 고성능 HTTP 프록시 및 LRU 캐싱 서버 구현

### 1. 프로젝트 개요 및 목표
* **목표**: HTTP 프로토콜의 동작 원리를 뜯어보고, 네트워크 대역폭 절감 및 응답 속도 향상을 위한 중계 캐시 서버를 순수 파이썬(Python) 소켓 프로그래밍으로 구현한다.
* **개발 환경**: Python 3 (내장 `socket`, `threading` 모듈 전용), WSL (Ubuntu Linux) 등
* **핵심 기술**: TCP 소켓 양방향 통신, HTTP 메시지 텍스트 파싱, LRU(Least Recently Used) 알고리즘, 스레드(Thread) 동시성 제어 및 Lock 동기화.

### 2. 4인 팀 역할 분담

* **역할 A: 네트워크 소켓 코어 설계**
  * **담당 업무**: 브라우저-프록시-외부 웹 서버를 잇는 양방향 TCP 릴레이 파이프라인 구축.
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

---

# [Computer Networks Team Project Proposal]
## Project Title: Implementation of a Multi-Threaded High-Performance HTTP Proxy and LRU Caching Server

### 1. Project Overview & Objectives
* **Objective**: Dissect the inner workings of the HTTP protocol and implement a relay cache server from scratch using pure Python socket programming to reduce network bandwidth consumption and improve response latency.
* **Development Environment**: Python 3 (built-in `socket` and `threading` modules only), WSL (Ubuntu Linux), etc.
* **Key Technologies**: Bidirectional TCP socket communication, HTTP message text parsing, LRU (Least Recently Used) eviction algorithm, thread concurrency control, and mutex (`Lock`) synchronization.

### 2. Team Roles (4 Members)

* **Role A: Network Socket Core Design**
  * **Responsibilities**: Build the bidirectional TCP relay pipeline connecting the client browser, the proxy server, and external origin web servers.
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
