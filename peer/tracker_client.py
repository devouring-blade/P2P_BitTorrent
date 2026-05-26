"""
tracker_client.py
=================
Client để peer giao tiếp với Tracker.

Mỗi lần cần nói chuyện với tracker:
  - Mở kết nối TCP mới
  - Gửi message
  - Nhận response
  - Đóng kết nối

Tại sao không giữ kết nối thường trực?
  Heartbeat dùng kết nối riêng, ngắn gọn.
  Đơn giản hơn, ít bug hơn cho đồ án này.
"""

import socket
import json
import time
import threading
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import protocol as P


class TrackerClient:
    def __init__(self, tracker_host: str, tracker_port: int,
                 peer_id: str, peer_port: int):
        self.tracker_host = tracker_host
        self.tracker_port = tracker_port
        self.peer_id      = peer_id
        self.peer_port    = peer_port

        # Heartbeat
        self._hb_thread   = None
        self._hb_running  = False
        self._hb_interval = 10   # giây

    # ── Gửi 1 message, nhận 1 response ───────────────────────
    def _send(self, msg_type: str, data: dict,
              timeout: float = 10.0) -> dict | None:
        """Mở kết nối, gửi, nhận, đóng."""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(timeout)
            s.connect((self.tracker_host, self.tracker_port))
            s.sendall(P.encode_message(msg_type, data))

            # Nhận response
            raw = b""
            while True:
                chunk = s.recv(1)
                if not chunk or chunk == b"\n":
                    break
                raw += chunk
            s.close()

            if raw:
                return json.loads(raw.decode("utf-8"))
        except (socket.timeout, ConnectionRefusedError, OSError) as e:
            print(f"[TrackerClient] Lỗi kết nối tracker: {e}")
        return None

    # ── Các lệnh giao tiếp ────────────────────────────────────
    def register(self, info_hash: str, chunks: list[int]) -> bool:
        """Đăng ký peer với tracker."""
        resp = self._send(P.MSG_REGISTER, {
            "info_hash": info_hash,
            "peer_id":   self.peer_id,
            "port":      self.peer_port,
            "chunks":    chunks
        })
        ok = resp and resp.get("type") == P.MSG_OK
        if ok:
            print(f"[TrackerClient] Đã đăng ký với tracker | {len(chunks)} chunks")
        return ok

    def get_peers(self, info_hash: str) -> list[dict]:
        """Lấy danh sách peer đang chia sẻ torrent này."""
        resp = self._send(P.MSG_GET_PEERS, {
            "info_hash": info_hash,
            "peer_id":   self.peer_id
        })
        if resp and resp.get("type") == P.MSG_PEER_LIST:
            peers = resp["data"].get("peers", [])
            print(f"[TrackerClient] Nhận {len(peers)} peer từ tracker")
            return peers
        return []

    def have(self, info_hash: str, chunk_index: int) -> bool:
        """Báo tracker vừa tải xong 1 chunk."""
        resp = self._send(P.MSG_HAVE, {
            "info_hash":   info_hash,
            "peer_id":     self.peer_id,
            "chunk_index": chunk_index
        })
        return resp and resp.get("type") == P.MSG_OK

    def unregister(self, info_hash: str) -> None:
        """Báo tracker rời mạng."""
        self._send(P.MSG_UNREGISTER, {
            "info_hash": info_hash,
            "peer_id":   self.peer_id
        })
        print(f"[TrackerClient] Đã unregister khỏi tracker")

    def heartbeat(self, info_hash: str) -> bool:
        """Gửi heartbeat để tracker biết peer vẫn online."""
        resp = self._send(P.MSG_HEARTBEAT, {
            "info_hash": info_hash,
            "peer_id":   self.peer_id
        }, timeout=5.0)
        return resp and resp.get("type") == P.MSG_OK

    # ── Heartbeat tự động ─────────────────────────────────────
    def start_heartbeat(self, info_hash: str) -> None:
        """Bắt đầu gửi heartbeat mỗi 10 giây trong thread nền."""
        self._hb_running = True

        def _hb_loop():
            while self._hb_running:
                time.sleep(self._hb_interval)
                if self._hb_running:
                    ok = self.heartbeat(info_hash)
                    if not ok:
                        print(f"[TrackerClient] Heartbeat thất bại!")

        self._hb_thread = threading.Thread(target=_hb_loop, daemon=True)
        self._hb_thread.start()
        print(f"[TrackerClient] Heartbeat started (mỗi {self._hb_interval}s)")

    def stop_heartbeat(self) -> None:
        """Dừng heartbeat."""
        self._hb_running = False
