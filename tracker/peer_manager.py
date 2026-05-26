"""
peer_manager.py
===============
Quản lý toàn bộ thông tin peer trong bộ nhớ RAM.

Tracker lưu dữ liệu theo cấu trúc:

  peers_db = {
    "info_hash_abc123": {               <- mỗi torrent 1 entry
      "peer_id_1": {
          "ip":        "192.168.1.2",
          "port":      6881,
          "chunks":    [0, 1, 2, 5],   <- chunk peer này đang có
          "last_seen": 1700000000.0    <- timestamp lần cuối heartbeat
      },
      "peer_id_2": { ... }
    },
    "info_hash_def456": { ... }
  }

Tại sao dùng RAM thay vì database?
- Dữ liệu peer thay đổi liên tục (join, leave, tải thêm chunk)
- Chỉ cần lưu trong phiên làm việc, không cần persistent
- Truy cập O(1), nhanh hơn SQL rất nhiều
"""

import time
import threading
from typing import Optional


# Peer bị coi là "chết" nếu không gửi heartbeat trong 30 giây
PEER_TIMEOUT = 30.0


class PeerManager:
    def __init__(self):
        # Dict lồng nhau: info_hash → peer_id → peer_info
        self._db: dict[str, dict[str, dict]] = {}

        # Lock để tránh race condition khi nhiều thread cùng truy cập
        # (Tracker xử lý nhiều kết nối đồng thời bằng thread)
        self._lock = threading.Lock()

    # ── Đăng ký peer ──────────────────────────────────────────────
    def register(self, info_hash: str, peer_id: str, ip: str, port: int,
                 chunks: list[int] = None) -> None:
        """
        Peer mới đăng ký với tracker.

        Được gọi khi:
        - Seeder bắt đầu chia sẻ (chunks = danh sách đầy đủ)
        - Leecher bắt đầu tải (chunks = [] vì chưa có gì)
        """
        with self._lock:
            if info_hash not in self._db:
                self._db[info_hash] = {}

            self._db[info_hash][peer_id] = {
                "ip":        ip,
                "port":      port,
                "chunks":    chunks or [],
                "last_seen": time.time()
            }
            print(f"[PeerManager] REGISTER | torrent={info_hash[:8]}... | peer={peer_id} | {ip}:{port} | {len(chunks or [])} chunks")

    # ── Cập nhật khi peer tải xong 1 chunk ────────────────────────
    def update_chunks(self, info_hash: str, peer_id: str,
                      chunk_index: int) -> bool:
        """
        Peer báo "tôi vừa tải xong chunk N".
        Tracker cập nhật danh sách chunk của peer.
        Return False nếu peer chưa đăng ký.
        """
        with self._lock:
            peer = self._get_peer(info_hash, peer_id)
            if not peer:
                return False
            if chunk_index not in peer["chunks"]:
                peer["chunks"].append(chunk_index)
            peer["last_seen"] = time.time()
            return True

    # ── Heartbeat ─────────────────────────────────────────────────
    def heartbeat(self, info_hash: str, peer_id: str) -> bool:
        """
        Peer gửi tín hiệu "tôi vẫn còn online".
        Cập nhật timestamp để tránh bị tracker xóa.
        """
        with self._lock:
            peer = self._get_peer(info_hash, peer_id)
            if not peer:
                return False
            peer["last_seen"] = time.time()
            return True

    # ── Hủy đăng ký ───────────────────────────────────────────────
    def unregister(self, info_hash: str, peer_id: str) -> None:
        """Peer rời mạng, xóa khỏi danh sách."""
        with self._lock:
            if info_hash in self._db and peer_id in self._db[info_hash]:
                del self._db[info_hash][peer_id]
                print(f"[PeerManager] UNREGISTER | peer={peer_id}")
                # Xóa torrent nếu không còn peer nào
                if not self._db[info_hash]:
                    del self._db[info_hash]

    # ── Lấy danh sách peer ────────────────────────────────────────
    def get_peers(self, info_hash: str,
                  exclude_peer_id: str = None) -> list[dict]:
        """
        Trả về danh sách peer đang chia sẻ torrent này.
        Loại trừ chính peer đang hỏi (exclude_peer_id).

        Output:
        [
            {"peer_id": "...", "ip": "...", "port": 6881, "chunks": [0,1,2]},
            ...
        ]
        """
        with self._lock:
            self._remove_dead_peers()   # dọn peer timeout trước
            if info_hash not in self._db:
                return []

            result = []
            for pid, info in self._db[info_hash].items():
                if pid == exclude_peer_id:
                    continue
                result.append({
                    "peer_id": pid,
                    "ip":      info["ip"],
                    "port":    info["port"],
                    "chunks":  list(info["chunks"])
                })
            return result

    # ── Thống kê ──────────────────────────────────────────────────
    def get_stats(self) -> dict:
        """Thông tin tổng quan về tracker (dùng debug)."""
        with self._lock:
            stats = {}
            for info_hash, peers in self._db.items():
                alive = {pid: p for pid, p in peers.items()
                         if time.time() - p["last_seen"] < PEER_TIMEOUT}
                stats[info_hash[:12] + "..."] = {
                    "total_peers": len(alive),
                    "peers": [
                        {"id": pid[:8]+"...", "ip": p["ip"],
                         "port": p["port"], "chunks": len(p["chunks"])}
                        for pid, p in alive.items()
                    ]
                }
            return stats

    # ── Private helpers ────────────────────────────────────────────
    def _get_peer(self, info_hash: str, peer_id: str) -> Optional[dict]:
        """Lấy thông tin 1 peer (không lock, gọi từ method đã lock)."""
        return self._db.get(info_hash, {}).get(peer_id)

    def _remove_dead_peers(self) -> None:
        """
        Xóa các peer không gửi heartbeat trong PEER_TIMEOUT giây.
        Gọi mỗi khi có request GET_PEERS.
        (Không lock vì đã được gọi từ method có lock)
        """
        now = time.time()
        dead = []
        for info_hash, peers in self._db.items():
            for peer_id, info in peers.items():
                if now - info["last_seen"] > PEER_TIMEOUT:
                    dead.append((info_hash, peer_id))

        for info_hash, peer_id in dead:
            print(f"[PeerManager] TIMEOUT → xóa peer {peer_id[:8]}...")
            del self._db[info_hash][peer_id]
            if not self._db[info_hash]:
                del self._db[info_hash]
