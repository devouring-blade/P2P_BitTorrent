"""
tracker_server.py
=================
TCP server lắng nghe kết nối từ các peer.

Mô hình hoạt động:
  Tracker chính (main thread)
      ↓ accept() — chờ kết nối mới
  Khi có peer kết nối → spawn thread mới
      Thread xử lý peer đó
          ↓ nhận message liên tục (vòng lặp while)
          ↓ gọi TrackerAPI.handle()
          ↓ gửi response
          ↓ khi peer ngắt kết nối → thread kết thúc

Tại sao mỗi peer 1 thread?
- Peer kết nối lâu dài (heartbeat, nhiều request)
- Nếu dùng 1 thread → chặn, không xử lý peer khác
- Threading đơn giản hơn asyncio cho đồ án này
"""

import socket
import threading
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tracker.peer_manager import PeerManager
from tracker.tracker_api   import TrackerAPI
from common                import protocol as P


class TrackerServer:
    def __init__(self, host: str = "0.0.0.0", port: int = 6969):
        """
        host="0.0.0.0" → lắng nghe trên tất cả network interface
        port=6969       → port truyền thống của BitTorrent tracker
        """
        self.host = host
        self.port = port
        self.peer_manager = PeerManager()
        self.api          = TrackerAPI(self.peer_manager)
        self._running     = False
        self._server_sock = None

    def start(self) -> None:
        """Khởi động tracker server."""
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # SO_REUSEADDR: cho phép bind lại port ngay sau khi restart
        # (tránh lỗi "Address already in use")
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.port))
        self._server_sock.listen(50)   # tối đa 50 kết nối chờ

        self._running = True
        print(f"[Tracker] Đang chạy tại {self.host}:{self.port}")
        print(f"[Tracker] Chờ peer kết nối...\n")

        try:
            while self._running:
                try:
                    # Chờ peer kết nối (blocking)
                    client_sock, addr = self._server_sock.accept()
                    client_ip = addr[0]
                    print(f"[Tracker] Peer kết nối từ {client_ip}:{addr[1]}")

                    # Spawn thread riêng cho peer này
                    thread = threading.Thread(
                        target=self._handle_peer,
                        args=(client_sock, client_ip),
                        daemon=True    # thread tự kết thúc khi main thread kết thúc
                    )
                    thread.start()

                except OSError:
                    if self._running:
                        raise
        finally:
            self._server_sock.close()

    def stop(self) -> None:
        """Dừng tracker server."""
        self._running = False
        if self._server_sock:
            self._server_sock.close()
        print("[Tracker] Đã dừng.")

    def _handle_peer(self, client_sock: socket.socket, client_ip: str) -> None:
        """
        Xử lý toàn bộ vòng đời của 1 peer:
          - Nhận message
          - Xử lý qua API
          - Gửi response
          - Lặp lại cho đến khi peer ngắt kết nối
        """
        client_sock.settimeout(60)   # timeout 60s mỗi recv
        try:
            while True:
                msg = P.recv_message(client_sock)
                if msg is None:
                    print(f"[Tracker] Peer {client_ip} ngắt kết nối")
                    break

                # Xử lý và lấy response
                response = self.api.handle(msg, client_ip)

                # Gửi response
                client_sock.sendall(
                    (json.dumps(response) + "\n").encode("utf-8")
                )

        except socket.timeout:
            print(f"[Tracker] Peer {client_ip} timeout")
        except Exception as e:
            print(f"[Tracker] Lỗi peer {client_ip}: {e}")
        finally:
            client_sock.close()

    def get_stats(self) -> dict:
        """Lấy thống kê hiện tại (để debug)."""
        return self.peer_manager.get_stats()


# ── Chạy trực tiếp ────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="P2P Tracker Server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=6969)
    args = parser.parse_args()

    tracker = TrackerServer(args.host, args.port)
    try:
        tracker.start()
    except KeyboardInterrupt:
        print("\n[Tracker] Nhận Ctrl+C, đang dừng...")
        tracker.stop()
