"""Role A: single-connection HTTP relay proxy (browser <-> proxy <-> origin server).

Run:  python3 proxy.py
Test: curl -x http://127.0.0.1:8888 http://example.com
"""
import socket

LISTEN_HOST = "127.0.0.1"
LISTEN_PORT = 8888
TIMEOUT = 10  # seconds, for both client and upstream sockets
BUF_SIZE = 4096


def recv_request(conn):
    """Read one HTTP request: headers until a blank line, then Content-Length bytes of body."""
    data = b""
    # TCP is a byte stream: one recv() may return only part of the request.
    while b"\r\n\r\n" not in data:
        chunk = conn.recv(BUF_SIZE)
        if not chunk:  # client closed the connection
            return b"", b""
        data += chunk

    head, _, body = data.partition(b"\r\n\r\n")
    length = 0
    for line in head.split(b"\r\n")[1:]:
        name, _, value = line.partition(b":")
        if name.strip().lower() == b"content-length":
            length = int(value.strip())
    while len(body) < length:
        chunk = conn.recv(BUF_SIZE)
        if not chunk:
            break
        body += chunk
    return head, body


def parse_target(head):
    """Split 'GET http://host:port/path HTTP/1.1' into its parts."""
    # shortcut: minimal request-line parsing, replace with Role B's parser when it lands
    request_line = head.split(b"\r\n", 1)[0].decode("latin-1")
    method, url, version = request_line.split(" ", 2)
    if url.startswith("http://"):
        url = url[len("http://"):]
    host_port, _, path = url.partition("/")
    host, _, port = host_port.partition(":")
    return method, host, int(port or 80), "/" + path, version


def build_upstream_request(head, body, method, path, version):
    """Rewrite the request for the origin server: origin-form path and Connection: close."""
    lines = [f"{method} {path} {version}".encode("latin-1")]
    for line in head.split(b"\r\n")[1:]:
        name = line.partition(b":")[0].strip().lower()
        if name not in (b"connection", b"proxy-connection", b"keep-alive"):
            lines.append(line)
    # Forces the origin to close after the response, so "recv until b''" marks its end.
    lines.append(b"Connection: close")
    return b"\r\n".join(lines) + b"\r\n\r\n" + body


def send_error(conn, code, reason):
    body = f"{code} {reason}\n".encode()
    response = (
        f"HTTP/1.1 {code} {reason}\r\n"
        f"Content-Type: text/plain\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"Connection: close\r\n\r\n"
    ).encode() + body
    try:
        conn.sendall(response)
    except OSError:
        pass  # client already gone


def handle_client(conn, addr):
    """Serve one client connection end to end. Roles B/C/D plug in here."""
    with conn:
        conn.settimeout(TIMEOUT)
        try:
            head, body = recv_request(conn)
        except ValueError:
            send_error(conn, 400, "Bad Request")
            return
        except OSError:
            return  # client idle (e.g. browser preconnect) or disconnected
        if not head:
            return

        try:
            method, host, port, path, version = parse_target(head)
        except ValueError:
            send_error(conn, 400, "Bad Request")
            return
        print(f"{addr[0]}:{addr[1]} {method} {host}:{port}{path}")

        if method == "CONNECT":  # HTTPS tunnel, not implemented yet
            send_error(conn, 501, "Not Implemented")
            return

        sent = False
        try:
            with socket.create_connection((host, port), timeout=TIMEOUT) as upstream:
                upstream.sendall(build_upstream_request(head, body, method, path, version))
                while True:
                    chunk = upstream.recv(BUF_SIZE)
                    if not chunk:  # origin closed: response complete
                        break
                    conn.sendall(chunk)
                    sent = True
        except socket.timeout:
            if not sent:
                send_error(conn, 504, "Gateway Timeout")
        except OSError as e:  # DNS failure, refused, reset, broken pipe
            print(f"  relay error: {e!r}")
            if not sent:
                send_error(conn, 502, "Bad Gateway")


def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((LISTEN_HOST, LISTEN_PORT))
        server.listen()
        print(f"Proxy listening on {LISTEN_HOST}:{LISTEN_PORT}")
        try:
            while True:
                conn, addr = server.accept()
                handle_client(conn, addr)  # one at a time; Role D moves this into a thread
        except KeyboardInterrupt:
            print("\nShutting down")


if __name__ == "__main__":
    main()
