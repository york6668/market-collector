"""Small RFC 6455 client for the collectors, without compression/subprotocols.

Handshake, fragmentation and control rules: https://www.rfc-editor.org/rfc/rfc6455
Receive bytes survive timeouts; a read has a total deadline so a partial frame
cannot indefinitely prevent the collector from checking connection health.
"""

import base64
import hashlib
import hmac
import os
import socket
import ssl
import struct
import time
from urllib.parse import urlsplit


GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class WebSocketError(RuntimeError):
    pass


class WebSocketConnection:
    def __init__(self, sock, initial=b"", read_timeout=5, max_frame=1048576,
                 max_message=1048576):
        self.sock = sock
        self.buffer = bytearray(initial)
        self.read_timeout = max(0.01, float(read_timeout))
        self.max_frame = int(max_frame)
        self.max_message = int(max_message)
        self.fragment_opcode = None
        self.fragments = bytearray()
        self.last_server_response_mono = time.monotonic()

    def close(self):
        self.sock.close()

    def _fill(self, size, deadline):
        while len(self.buffer) < size:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise socket.timeout("websocket read deadline")
            self.sock.settimeout(min(self.read_timeout, remaining))
            chunk = self.sock.recv(min(65536, size - len(self.buffer)))
            if not chunk:
                raise WebSocketError("websocket closed")
            self.buffer.extend(chunk)

    @staticmethod
    def _validate_close(payload):
        if len(payload) == 1:
            raise WebSocketError("invalid websocket close payload")
        if payload:
            code = struct.unpack("!H", payload[:2])[0]
            if code not in (1000, 1001, 1002, 1003, 1007, 1008, 1009,
                            1010, 1011, 1012, 1013, 1014) and not 3000 <= code <= 4999:
                raise WebSocketError("invalid websocket close code")
            payload[2:].decode("utf-8")

    def read_frame(self):
        """Return complete data messages or one control frame, preserving fragments."""
        deadline = time.monotonic() + self.read_timeout
        while True:
            self._fill(2, deadline)
            first, second = self.buffer[:2]
            fin, opcode = bool(first & 0x80), first & 0x0f
            if first & 0x70 or second & 0x80:
                raise WebSocketError("unexpected websocket RSV bits or server mask")
            if opcode not in (0, 1, 2, 8, 9, 10):
                raise WebSocketError("invalid websocket opcode")
            length, offset = second & 0x7f, 2
            if opcode >= 8 and (not fin or length > 125):
                raise WebSocketError("invalid websocket control frame")
            if length == 126:
                self._fill(4, deadline)
                length, offset = struct.unpack("!H", self.buffer[2:4])[0], 4
                if length < 126:
                    raise WebSocketError("nonminimal websocket frame length")
            elif length == 127:
                self._fill(10, deadline)
                length, offset = struct.unpack("!Q", self.buffer[2:10])[0], 10
                if length < 65536 or length >= 1 << 63:
                    raise WebSocketError("invalid websocket 64-bit length")
            if length > self.max_frame:
                raise WebSocketError("websocket frame exceeds limit")
            if opcode < 8:
                if opcode == 0 and self.fragment_opcode is None:
                    raise WebSocketError("unexpected websocket continuation")
                if opcode != 0 and self.fragment_opcode is not None:
                    raise WebSocketError("new websocket data frame during fragmentation")
                accumulated = len(self.fragments) if opcode == 0 else 0
                if accumulated + length > self.max_message:
                    raise WebSocketError("websocket message exceeds limit")
            self._fill(offset + length, deadline)
            payload = bytes(self.buffer[offset:offset + length])
            del self.buffer[:offset + length]
            if opcode == 8:
                self._validate_close(payload)
            if opcode in (8, 9, 10):
                self.last_server_response_mono = time.monotonic()
                return opcode, payload
            if opcode in (1, 2) and not fin:
                self.fragment_opcode = opcode
                self.fragments.extend(payload)
            elif opcode == 0:
                self.fragments.extend(payload)
                if fin:
                    opcode, payload = self.fragment_opcode, bytes(self.fragments)
                    self.fragment_opcode = None
                    self.fragments.clear()
            if fin:
                if opcode == 1:
                    payload.decode("utf-8")
                self.last_server_response_mono = time.monotonic()
                return opcode, payload
            self.last_server_response_mono = time.monotonic()
            if time.monotonic() >= deadline:
                raise socket.timeout("websocket message read deadline")

    def send_frame(self, opcode, payload=b""):
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        payload = bytes(payload or b"")
        if opcode not in (1, 2, 8, 9, 10):
            raise WebSocketError("invalid client websocket opcode")
        length = len(payload)
        if length > self.max_message or (opcode >= 8 and length > 125):
            raise WebSocketError("client websocket frame exceeds limit")
        if opcode == 1:
            payload.decode("utf-8")
        if opcode == 8:
            self._validate_close(payload)
        header = bytearray([0x80 | opcode])
        if length < 126:
            header.append(0x80 | length)
        elif length <= 65535:
            header.extend(bytes([0xfe]) + struct.pack("!H", length))
        else:
            header.extend(bytes([0xff]) + struct.pack("!Q", length))
        mask = os.urandom(4)
        masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        self.sock.settimeout(self.read_timeout)
        self.sock.sendall(bytes(header) + mask + masked)


def connect_websocket(url, connect_timeout=10, read_timeout=5, user_agent="btcpush",
                      max_header=16384, max_frame=1048576, max_message=1048576):
    parsed = urlsplit(url)
    if parsed.scheme not in ("ws", "wss") or not parsed.hostname:
        raise WebSocketError("invalid websocket URL")
    if parsed.username is not None or parsed.password is not None or parsed.fragment:
        raise WebSocketError("unsupported websocket URL components")
    if any(char in url + user_agent for char in "\r\n"):
        raise WebSocketError("invalid websocket request header")
    host = parsed.hostname
    default_port = 443 if parsed.scheme == "wss" else 80
    port = parsed.port or default_port
    authority = f"[{host}]" if ":" in host else host
    if port != default_port:
        authority += f":{port}"
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    request = (f"GET {path} HTTP/1.1\r\nHost: {authority}\r\n"
               "Upgrade: websocket\r\nConnection: Upgrade\r\n"
               f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n"
               f"User-Agent: {user_agent}\r\n\r\n").encode("ascii")
    sock = socket.create_connection((host, port), timeout=connect_timeout)
    try:
        if parsed.scheme == "wss":
            sock = ssl.create_default_context().wrap_socket(sock, server_hostname=host)
        deadline = time.monotonic() + connect_timeout
        sock.settimeout(connect_timeout)
        sock.sendall(request)
        response = bytearray()
        while True:
            boundary = response.find(b"\r\n\r\n")
            if boundary >= 0:
                if boundary + 4 > max_header:
                    raise WebSocketError("websocket handshake exceeds limit")
                break
            if len(response) >= max_header:
                raise WebSocketError("websocket handshake exceeds limit")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise socket.timeout("websocket handshake deadline")
            sock.settimeout(remaining)
            chunk = sock.recv(min(4096, max_header - len(response)))
            if not chunk:
                raise WebSocketError("websocket handshake closed")
            response.extend(chunk)
        lines = bytes(response[:boundary]).decode("latin1").split("\r\n")
        status = lines[0].split(" ", 2)
        if len(status) < 2 or status[0] != "HTTP/1.1" or status[1] != "101":
            raise WebSocketError("websocket upgrade rejected")
        headers = {}
        for line in lines[1:]:
            if ":" not in line or line[:1] in (" ", "\t"):
                raise WebSocketError("invalid websocket response header")
            name, value = line.split(":", 1)
            if not name or name != name.strip():
                raise WebSocketError("invalid websocket response header name")
            headers.setdefault(name.lower(), []).append(value.strip())
        def tokens(name):
            return {token.strip().lower() for value in headers.get(name, [])
                    for token in value.split(",")}
        if "websocket" not in tokens("upgrade") or "upgrade" not in tokens("connection"):
            raise WebSocketError("invalid websocket upgrade headers")
        for name in ("sec-websocket-accept", "sec-websocket-extensions", "sec-websocket-protocol"):
            if len(headers.get(name, [])) > 1:
                raise WebSocketError("duplicate websocket negotiation header")
        expected = base64.b64encode(hashlib.sha1((key + GUID).encode("ascii")).digest()).decode("ascii")
        accepts = headers.get("sec-websocket-accept", [])
        if len(accepts) != 1 or not hmac.compare_digest(accepts[0], expected):
            raise WebSocketError("invalid websocket accept")
        if "sec-websocket-extensions" in headers or "sec-websocket-protocol" in headers:
            raise WebSocketError("unrequested websocket extension or subprotocol")
        return WebSocketConnection(sock, response[boundary + 4:], read_timeout,
                                   max_frame, max_message)
    except BaseException:
        sock.close()
        raise


class WebSocketLiveness:
    """Monotonic health clocks; outgoing pings never count as remote evidence."""
    def __init__(self, connected_at, response_timeout, sample_timeout=None, ping_interval=30):
        self.connected_at = connected_at
        self.last_server_response = connected_at
        self.last_client_ping = connected_at
        self.last_valid_sample = None
        self.response_timeout = response_timeout
        self.sample_timeout = sample_timeout
        self.ping_interval = ping_interval

    def check(self, now):
        if now - self.last_server_response >= self.response_timeout:
            raise WebSocketError("websocket server response overdue")
        sample_start = self.connected_at if self.last_valid_sample is None else self.last_valid_sample
        if self.sample_timeout is not None and now - sample_start >= self.sample_timeout:
            raise WebSocketError("websocket valid sample overdue")

    def ping_due(self, now):
        return now - self.last_client_ping >= self.ping_interval
