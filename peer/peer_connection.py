"""
peer_connection.py
==================
Quản lý 1 kết nối TCP giữa 2 peer.

Vai trò:
  - Wrap socket thành interface dễ dùng hơn
  - Gửi/nhận chunk data trực tiếp (không qua tracker)
  - Theo dõi thống kê: tốc độ, số byte đã gửi/nhận
  - Xử lý lỗi kết nối (timeout, disconnect)

Luồng dữ liệu:

  Leecher                          Seeder
  ───────                          ──────
  connect()       ──TCP──→         accept()
  send BITFIELD   ──────→          recv BITFIELD
  recv BITFIELD   ←──────          send BITFIELD
  send REQUEST(3) ──────→          recv REQUEST(3)
                  ←──────          send PIECE(3, data)
  recv PIECE(3)
  verify hash
  save chunk
  send HAVE(3)   ──────→

Tại sao tách PeerConnection ra file riêng?
  - Downloader và Uploader dùng chung class này
  - Dễ mock khi test (thay bằng FakeConnection)
  - Logic retry, timeout tập trung 1 chỗ

Giao thức truyền chunk (binary-aware):
  Message thông thường (JSON): {"type":..., "data":...}\n
  Message PIECE đặc biệt     : header JSON\n + binary data
  Vì chunk data là bytes thô, không encode được JSON trực tiếp.
"""

import socket
import time
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import protocol as P


class PeerConnection:
    def __init__(self, peer_id: str, ip: str, port: int,
                 sock: socket.socket = None):
        """
        peer_id → ID của peer ở đầu kia
        ip, port → địa chỉ của peer đó
        sock     → nếu đã có socket (accept từ server), truyền vào luôn
                   nếu None → sẽ tạo mới khi connect()
        """
        self.peer_id  = peer_id
        self.ip       = ip
        self.port     = port
        self._sock    = sock

        # Thống kê
        self.bytes_sent     = 0
        self.bytes_received = 0
        self.connected_at   = None
        self._connected     = sock is not None  # True nếu đã có socket

        # Bitfield của peer kia (cập nhật khi nhận BITFIELD message)
        self.remote_bitfield: list[int] = []

    # ── Kết nối ───────────────────────────────────────────────
    def connect(self, timeout: float = 10.0) -> bool:
        """
        Kết nối đến peer (dùng bởi Leecher để kết nối đến Seeder).
        Return True nếu thành công, False nếu thất bại.
        """
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock.settimeout(timeout)
            self._sock.connect((self.ip, self.port))
            self._connected  = True
            self.connected_at = time.time()
            print(f"[PeerConn] Kết nối đến {self.peer_id} ({self.ip}:{self.port}) ✓")
            return True
        except (socket.timeout, ConnectionRefusedError, OSError) as e:
            print(f"[PeerConn] Không thể kết nối {self.ip}:{self.port}: {e}")
            self._connected = False
            return False

    def disconnect(self) -> None:
        """Đóng kết nối."""
        self._connected = False
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None
        print(f"[PeerConn] Ngắt kết nối {self.peer_id}")

    @property
    def is_connected(self) -> bool:
        return self._connected and self._sock is not None

    # ── Gửi/nhận message JSON ─────────────────────────────────
    def send_msg(self, msg_type: str, data: dict = None) -> bool:
        """
        Gửi message JSON thông thường.
        Return False nếu gửi thất bại.
        """
        if not self.is_connected:
            return False
        try:
            raw = P.encode_message(msg_type, data)
            self._sock.sendall(raw)
            self.bytes_sent += len(raw)
            return True
        except OSError as e:
            print(f"[PeerConn] Lỗi gửi đến {self.peer_id}: {e}")
            self._connected = False
            return False

    def recv_msg(self, timeout: float = 30.0) -> dict | None:
        """
        Nhận 1 message JSON từ peer.
        Return None nếu kết nối đóng hoặc timeout.
        """
        if not self.is_connected:
            return None
        try:
            self._sock.settimeout(timeout)
            msg = P.recv_message(self._sock)
            if msg:
                self.bytes_received += 1  # approximate
            return msg
        except socket.timeout:
            print(f"[PeerConn] Timeout nhận từ {self.peer_id}")
            return None
        except OSError as e:
            print(f"[PeerConn] Lỗi nhận từ {self.peer_id}: {e}")
            self._connected = False
            return None

    # ── Gửi/nhận chunk (binary) ───────────────────────────────
    def send_piece(self, chunk_index: int, data: bytes) -> bool:
        """
        Gửi 1 chunk cho peer kia.

        Format đặc biệt (2 bước):
          Bước 1: gửi header JSON  → {"type":"PIECE","index":3,"size":524288}\n
          Bước 2: gửi binary data  → <raw bytes>

        Tại sao không encode data vào JSON?
          JSON chỉ xử lý text. Bytes phải base64 → tốn thêm 33% băng thông.
          Tách header + binary thẳng → hiệu quả hơn.
        """
        if not self.is_connected:
            return False
        try:
            # Bước 1: header
            header = json.dumps({
                "type":  P.MSG_PIECE,
                "index": chunk_index,
                "size":  len(data)
            }) + "\n"
            self._sock.sendall(header.encode("utf-8"))

            # Bước 2: binary data
            self._sock.sendall(data)

            self.bytes_sent += len(header) + len(data)
            return True

        except OSError as e:
            print(f"[PeerConn] Lỗi gửi chunk {chunk_index}: {e}")
            self._connected = False
            return False

    def recv_piece(self, timeout: float = 60.0) -> tuple[int, bytes] | None:
        """
        Nhận 1 chunk từ peer kia.

        Bước 1: đọc header JSON (đến \n)
        Bước 2: đọc đúng `size` bytes binary

        Return: (chunk_index, data) hoặc None nếu lỗi
        """
        if not self.is_connected:
            return None
        try:
            self._sock.settimeout(timeout)

            # Bước 1: đọc header
            header_raw = b""
            while True:
                byte = self._sock.recv(1)
                if not byte:
                    return None
                if byte == b"\n":
                    break
                header_raw += byte

            header = json.loads(header_raw.decode("utf-8"))
            if header.get("type") != P.MSG_PIECE:
                print(f"[PeerConn] Mong PIECE, nhận {header.get('type')}")
                return None

            chunk_index = header["index"]
            size        = header["size"]

            # Bước 2: đọc đúng `size` bytes
            data = b""
            remaining = size
            while remaining > 0:
                chunk = self._sock.recv(min(remaining, 65536))  # 64KB mỗi lần
                if not chunk:
                    print(f"[PeerConn] Kết nối đóng khi đang nhận chunk {chunk_index}")
                    return None
                data      += chunk
                remaining -= len(chunk)

            self.bytes_received += len(data)
            return chunk_index, data

        except socket.timeout:
            print(f"[PeerConn] Timeout nhận chunk từ {self.peer_id}")
            return None
        except (OSError, json.JSONDecodeError) as e:
            print(f"[PeerConn] Lỗi nhận chunk: {e}")
            self._connected = False
            return None

    # ── Handshake ─────────────────────────────────────────────
    def do_handshake_initiator(self, my_peer_id: str,
                                info_hash: str,
                                my_chunks: list[int]) -> bool:
        """
        Bên khởi tạo kết nối (leecher) thực hiện handshake:
          1. Gửi BITFIELD của mình
          2. Nhận BITFIELD của peer kia
        Return True nếu handshake thành công.
        """
        # Gửi BITFIELD
        ok = self.send_msg(P.MSG_BITFIELD, {
            "peer_id":   my_peer_id,
            "info_hash": info_hash,
            "chunks":    my_chunks
        })
        if not ok:
            return False

        # Nhận BITFIELD từ peer kia
        msg = self.recv_msg(timeout=10.0)
        if not msg or msg.get("type") != P.MSG_BITFIELD:
            print(f"[PeerConn] Handshake thất bại: không nhận được BITFIELD")
            return False

        self.remote_bitfield = msg["data"].get("chunks", [])
        self.peer_id = msg["data"].get("peer_id", self.peer_id)
        print(f"[PeerConn] Handshake OK với {self.peer_id} | peer có {len(self.remote_bitfield)} chunks")
        return True

    def do_handshake_receiver(self, my_peer_id: str,
                               info_hash: str,
                               my_chunks: list[int]) -> bool:
        """
        Bên nhận kết nối (seeder/peer đang upload) thực hiện handshake:
          1. Nhận BITFIELD của peer kia
          2. Gửi BITFIELD của mình
        """
        # Nhận trước
        msg = self.recv_msg(timeout=10.0)
        if not msg or msg.get("type") != P.MSG_BITFIELD:
            print(f"[PeerConn] Handshake thất bại: không nhận BITFIELD đầu tiên")
            return False

        self.remote_bitfield = msg["data"].get("chunks", [])
        self.peer_id = msg["data"].get("peer_id", self.peer_id)

        # Gửi lại
        ok = self.send_msg(P.MSG_BITFIELD, {
            "peer_id":   my_peer_id,
            "info_hash": info_hash,
            "chunks":    my_chunks
        })
        if not ok:
            return False

        print(f"[PeerConn] Handshake OK với {self.peer_id} | peer có {len(self.remote_bitfield)} chunks")
        return True

    # ── Thống kê ──────────────────────────────────────────────
    def get_stats(self) -> dict:
        elapsed = time.time() - self.connected_at if self.connected_at else 0
        return {
            "peer_id":       self.peer_id,
            "address":       f"{self.ip}:{self.port}",
            "connected":     self.is_connected,
            "bytes_sent":    self.bytes_sent,
            "bytes_received":self.bytes_received,
            "elapsed_sec":   round(elapsed, 1),
            "upload_kbps":   round(self.bytes_sent / max(elapsed,1) / 1024, 2),
            "download_kbps": round(self.bytes_received / max(elapsed,1) / 1024, 2),
        }

    def __str__(self) -> str:
        status = "connected" if self.is_connected else "disconnected"
        return f"PeerConnection({self.peer_id} @ {self.ip}:{self.port} [{status}])"
