"""
peer_node.py
============
Điều phối trung tâm của 1 peer — tích hợp tất cả thành phần.

1 PeerNode = 1 người dùng trong mạng P2P

Một peer có thể:
  - Là SEEDER : có file đầy đủ, chỉ upload
  - Là LEECHER: chưa có file, chỉ download
  - Là CẢ HAI : đang tải nhưng đồng thời upload chunk đã có

PeerNode quản lý:
  ┌─────────────────────────────────┐
  │           PeerNode              │
  │  ┌──────────┐  ┌─────────────┐ │
  │  │ Uploader │  │ Downloader  │ │
  │  │(thread 1)│  │(thread 2..N)│ │
  │  └──────────┘  └─────────────┘ │
  │  ┌──────────────────────────┐  │
  │  │      PieceManager        │  │
  │  │  (shared state, locked)  │  │
  │  └──────────────────────────┘  │
  │  ┌──────────────────────────┐  │
  │  │   Heartbeat thread       │  │
  │  │   (mỗi 10s → tracker)    │  │
  │  └──────────────────────────┘  │
  └─────────────────────────────────┘
"""

import os
import sys
import uuid
import time
import threading
import socket
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common              import protocol as P
from common.torrent_parser import load_torrent
from common.file_handler   import merge_chunks, DEFAULT_CHUNK_SIZE
from peer.piece_manager    import PieceManager
from peer.uploader         import Uploader
from peer.downloader       import Downloader


class PeerNode:
    def __init__(self, upload_port: int = 6881,
                 chunk_dir: str = "chunks",
                 output_dir: str = "downloads",
                 max_download_connections: int = 4):
        """
        upload_port → port để peer khác kết nối vào tải file
        chunk_dir   → thư mục lưu chunk đang tải
        output_dir  → thư mục lưu file hoàn chỉnh sau khi ghép
        """
        # Tạo peer_id duy nhất mỗi lần chạy
        self.peer_id    = f"peer-{uuid.uuid4().hex[:8]}"
        self.upload_port = upload_port
        self.chunk_dir   = chunk_dir
        self.output_dir  = output_dir
        self.max_dl_conn = max_download_connections

        # Components (khởi tạo khi seed/download)
        self._uploader   = None
        self._downloader = None
        self._pm         = None

        # Tracker info (set khi load torrent)
        self._tracker_ip   = None
        self._tracker_port = None
        self._info_hash    = None
        self._torrent_info = None

        # Heartbeat
        self._heartbeat_thread = None
        self._running          = False

        print(f"[PeerNode] Khởi tạo peer: {self.peer_id}")

    # ══════════════════════════════════════════════════════════
    # SEEDER: chia sẻ file đã có
    # ══════════════════════════════════════════════════════════
    def seed(self, torrent_path: str, file_path: str) -> None:
        """
        Bắt đầu chia sẻ 1 file.

        Quy trình:
        1. Load file .torrent → lấy metadata
        2. Đăng ký với tracker (báo có đủ tất cả chunk)
        3. Khởi động Uploader → lắng nghe peer muốn tải
        4. Gửi heartbeat định kỳ

        file_path → file gốc trên ổ đĩa (để Uploader đọc chunk)
        """
        self._load_torrent(torrent_path)

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File không tồn tại: {file_path}")

        # PieceManager với đủ tất cả chunk (seeder có hết)
        all_chunks = list(range(self._torrent_info["num_chunks"]))
        self._pm = PieceManager(
            num_chunks=self._torrent_info["num_chunks"],
            have_chunks=all_chunks
        )

        print(f"[PeerNode] SEEDER mode | {self._torrent_info['name']} | "
              f"{self._torrent_info['num_chunks']} chunks")

        # Đăng ký với tracker
        self._register_with_tracker(all_chunks)

        # Khởi động uploader
        self._uploader = Uploader(
            peer_id=self.peer_id,
            info_hash=self._info_hash,
            file_path=file_path,
            piece_manager=self._pm,
            host="0.0.0.0",
            port=self.upload_port,
            chunk_size=self._torrent_info["chunk_size"]
        )
        self._running = True
        self._uploader.start()

        # Heartbeat thread
        self._start_heartbeat()

        print(f"[PeerNode] Đang seed tại port {self.upload_port}...")
        print(f"[PeerNode] Nhấn Ctrl+C để dừng.\n")

    # ══════════════════════════════════════════════════════════
    # LEECHER: tải file từ các peer
    # ══════════════════════════════════════════════════════════
    def download(self, torrent_path: str, upload_port: int = None) -> str | None:
        """
        Tải file từ mạng P2P.

        Quy trình:
        1. Load .torrent → biết cần tải gì
        2. Khởi động Uploader (peer vừa tải vừa chia sẻ chunk đã có)
        3. Khởi động Downloader → tải từ nhiều peer song song
        4. Khi đủ chunk → ghép thành file hoàn chỉnh
        5. Return đường dẫn file đã tải

        upload_port → port để upload cho peer khác trong lúc tải
                      (None = không upload, chỉ download)
        """
        self._load_torrent(torrent_path)

        num_chunks = self._torrent_info["num_chunks"]
        chunk_size = self._torrent_info["chunk_size"]

        # Tạo thư mục lưu chunk
        os.makedirs(self.chunk_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)

        # PieceManager bắt đầu rỗng
        self._pm = PieceManager(num_chunks=num_chunks)

        print(f"[PeerNode] LEECHER mode | {self._torrent_info['name']} | "
              f"{num_chunks} chunks cần tải\n")

        # Khởi động Uploader song song (nếu có upload_port)
        # → peer khác có thể tải chunk mình vừa có
        port = upload_port or self.upload_port
        self._uploader = Uploader(
            peer_id=self.peer_id,
            info_hash=self._info_hash,
            file_path=None,      # chưa có file gốc, serve từ chunk_dir
            piece_manager=self._pm,
            host="0.0.0.0",
            port=port,
            chunk_size=chunk_size
        )
        self._running = True
        self._uploader.start()

        # Khởi động Downloader
        self._downloader = Downloader(
            peer_id=self.peer_id,
            info_hash=self._info_hash,
            torrent_info=self._torrent_info,
            chunk_dir=self.chunk_dir,
            piece_manager=self._pm,
            tracker_ip=self._tracker_ip,
            tracker_port=self._tracker_port,
            max_connections=self.max_dl_conn,
            chunk_size=chunk_size
        )

        # Chạy downloader (blocking cho đến khi xong)
        self._downloader.start()

        # Ghép file
        if self._pm.is_complete():
            return self._assemble_file()
        else:
            print("[PeerNode] Tải không hoàn thành.")
            return None

    # ══════════════════════════════════════════════════════════
    # Internal helpers
    # ══════════════════════════════════════════════════════════
    def _load_torrent(self, torrent_path: str) -> None:
        """Load file .torrent và parse tracker URL."""
        self._torrent_info = load_torrent(torrent_path)
        self._info_hash    = self._torrent_info["info_hash"]

        # Parse tracker URL "ip:port"
        tracker_url = self._torrent_info.get("tracker_url", "127.0.0.1:6969")
        parts = tracker_url.split(":")
        self._tracker_ip   = parts[0]
        self._tracker_port = int(parts[1]) if len(parts) > 1 else 6969

    def _register_with_tracker(self, chunks: list[int]) -> bool:
        """Gửi REGISTER đến tracker."""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(10)
            s.connect((self._tracker_ip, self._tracker_port))
            msg = json.dumps({"type": P.MSG_REGISTER, "data": {
                "info_hash": self._info_hash,
                "peer_id":   self.peer_id,
                "port":      self.upload_port,
                "chunks":    chunks
            }}) + "\n"
            s.sendall(msg.encode())
            raw = b""
            while True:
                b = s.recv(1)
                if not b or b == b"\n": break
                raw += b
            s.close()
            resp = json.loads(raw.decode()) if raw else None
            if resp and resp["type"] == P.MSG_OK:
                print(f"[PeerNode] Đã đăng ký với tracker ✓")
                return True
        except Exception as e:
            print(f"[PeerNode] Không thể đăng ký tracker: {e}")
        return False

    def _start_heartbeat(self) -> None:
        """Gửi HEARTBEAT đến tracker mỗi 10 giây."""
        def heartbeat_loop():
            while self._running:
                time.sleep(10)
                if not self._running:
                    break
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(5)
                    s.connect((self._tracker_ip, self._tracker_port))
                    msg = json.dumps({"type": P.MSG_HEARTBEAT, "data": {
                        "info_hash": self._info_hash,
                        "peer_id":   self.peer_id
                    }}) + "\n"
                    s.sendall(msg.encode())
                    s.recv(256)
                    s.close()
                except:
                    pass  # heartbeat thất bại không quan trọng

        self._heartbeat_thread = threading.Thread(
            target=heartbeat_loop, daemon=True
        )
        self._heartbeat_thread.start()

    def _assemble_file(self) -> str:
        """Ghép tất cả chunk thành file hoàn chỉnh."""
        filename    = self._torrent_info["name"]
        output_path = os.path.join(self.output_dir, filename)
        num_chunks  = self._torrent_info["num_chunks"]

        print(f"\n[PeerNode] Đang ghép {num_chunks} chunks → {output_path}")
        ok = merge_chunks(self.chunk_dir, output_path, num_chunks)

        if ok:
            print(f"[PeerNode] ✅ File hoàn chỉnh: {output_path}")
            return output_path
        else:
            print(f"[PeerNode] ❌ Ghép file thất bại")
            return None

    def stop(self) -> None:
        """Dừng peer node."""
        self._running = False
        if self._uploader:
            self._uploader.stop()
        if self._downloader:
            self._downloader.stop()
        # Unregister với tracker
        if self._tracker_ip and self._info_hash:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(3)
                s.connect((self._tracker_ip, self._tracker_port))
                msg = json.dumps({"type": P.MSG_UNREGISTER, "data": {
                    "info_hash": self._info_hash,
                    "peer_id":   self.peer_id
                }}) + "\n"
                s.sendall(msg.encode())
                s.close()
            except:
                pass
        print(f"[PeerNode] {self.peer_id} đã dừng.")

    def get_stats(self) -> dict:
        stats = {"peer_id": self.peer_id}
        if self._pm:
            stats["piece_manager"] = str(self._pm)
        if self._uploader:
            stats["uploader"] = self._uploader.get_stats()
        if self._downloader:
            stats["downloader"] = self._downloader.get_stats()
        return stats
