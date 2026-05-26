"""
tracker_api.py
==============
Xử lý từng loại message từ peer và trả về response.

Đây là tầng "business logic" của tracker:
- Nhận message đã decode
- Gọi PeerManager để thao tác dữ liệu
- Trả về dict response

Tách ra khỏi tracker_server.py để:
- Dễ test (không cần socket thật)
- Dễ thêm logic mới (auth, rate limit, v.v.)
"""

from common import protocol as P
from tracker.peer_manager import PeerManager


class TrackerAPI:
    def __init__(self, peer_manager: PeerManager):
        self.pm = peer_manager

    def handle(self, msg: dict, client_ip: str) -> dict:
        """
        Nhận message từ peer, xử lý, trả về response dict.

        Input:
            msg       -> {"type": "REGISTER", "data": {...}}
            client_ip -> IP thực của client (từ socket)
        Output:
            {"type": "OK", "data": {...}}
        """
        msg_type = msg.get("type")
        data     = msg.get("data", {})

        handlers = {
            P.MSG_REGISTER:   self._handle_register,
            P.MSG_GET_PEERS:  self._handle_get_peers,
            P.MSG_HAVE:       self._handle_have,
            P.MSG_HEARTBEAT:  self._handle_heartbeat,
            P.MSG_UNREGISTER: self._handle_unregister,
        }

        handler = handlers.get(msg_type)
        if not handler:
            return {"type": P.MSG_ERROR,
                    "data": {"message": f"Unknown message type: {msg_type}"}}

        try:
            return handler(data, client_ip)
        except KeyError as e:
            return {"type": P.MSG_ERROR,
                    "data": {"message": f"Thiếu trường bắt buộc: {e}"}}
        except Exception as e:
            return {"type": P.MSG_ERROR,
                    "data": {"message": str(e)}}

    # ── REGISTER ──────────────────────────────────────────────────
    def _handle_register(self, data: dict, client_ip: str) -> dict:
        """
        Peer gửi:
        {
            "info_hash": "abc123...",
            "peer_id":   "peer-uuid-...",
            "port":      6881,
            "chunks":    [0, 1, 2]     <- chunk đang có (seeder: đủ; leecher: [])
        }
        """
        info_hash = data["info_hash"]
        peer_id   = data["peer_id"]
        port      = int(data["port"])
        chunks    = data.get("chunks", [])

        # Dùng IP từ socket (không tin IP peer tự khai)
        # Vì peer có thể ở sau NAT, khai IP sai
        self.pm.register(info_hash, peer_id, client_ip, port, chunks)

        return {
            "type": P.MSG_OK,
            "data": {"message": "Đăng ký thành công", "peer_id": peer_id}
        }

    # ── GET_PEERS ─────────────────────────────────────────────────
    def _handle_get_peers(self, data: dict, client_ip: str) -> dict:
        """
        Peer gửi:
        {
            "info_hash": "abc123...",
            "peer_id":   "peer-uuid-..."   <- để tracker loại trừ chính nó
        }

        Tracker trả về:
        {
            "peers": [
                {"peer_id": "...", "ip": "...", "port": 6881, "chunks": [0,1]},
                ...
            ]
        }
        """
        info_hash = data["info_hash"]
        peer_id   = data.get("peer_id")

        peers = self.pm.get_peers(info_hash, exclude_peer_id=peer_id)

        return {
            "type": P.MSG_PEER_LIST,
            "data": {"peers": peers, "count": len(peers)}
        }

    # ── HAVE ──────────────────────────────────────────────────────
    def _handle_have(self, data: dict, client_ip: str) -> dict:
        """
        Peer vừa tải xong 1 chunk, báo tracker biết.
        {
            "info_hash":   "abc123...",
            "peer_id":     "peer-uuid-...",
            "chunk_index": 5
        }
        """
        info_hash   = data["info_hash"]
        peer_id     = data["peer_id"]
        chunk_index = int(data["chunk_index"])

        ok = self.pm.update_chunks(info_hash, peer_id, chunk_index)
        if not ok:
            return {"type": P.MSG_ERROR,
                    "data": {"message": "Peer chưa đăng ký"}}

        return {"type": P.MSG_OK, "data": {"chunk_index": chunk_index}}

    # ── HEARTBEAT ─────────────────────────────────────────────────
    def _handle_heartbeat(self, data: dict, client_ip: str) -> dict:
        """
        Peer gửi mỗi 10 giây để báo "tôi vẫn online".
        Tracker cập nhật last_seen để không bị xóa.
        """
        info_hash = data["info_hash"]
        peer_id   = data["peer_id"]

        ok = self.pm.heartbeat(info_hash, peer_id)
        if not ok:
            return {"type": P.MSG_ERROR,
                    "data": {"message": "Peer chưa đăng ký"}}

        return {"type": P.MSG_OK, "data": {"message": "pong"}}

    # ── UNREGISTER ────────────────────────────────────────────────
    def _handle_unregister(self, data: dict, client_ip: str) -> dict:
        """Peer báo rời mạng, tracker xóa khỏi danh sách ngay."""
        info_hash = data["info_hash"]
        peer_id   = data["peer_id"]

        self.pm.unregister(info_hash, peer_id)
        return {"type": P.MSG_OK, "data": {"message": "Đã hủy đăng ký"}}
