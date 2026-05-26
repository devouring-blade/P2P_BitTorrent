"""
uploader.py - Lắng nghe kết nối và phục vụ chunk cho peer khác.
"""

import socket
import threading
import time
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common             import protocol as P
from common.file_handler import read_chunk, DEFAULT_CHUNK_SIZE
from common.hash_utils   import verify_chunk
from peer.peer_connection import PeerConnection


class Uploader:
    def __init__(self, peer_id: str, host: str, port: int,
                 filepath: str, torrent_info: dict):
        """
        peer_id      → ID của peer này
        host/port    → địa chỉ lắng nghe upload
        filepath     → đường dẫn file gốc
        torrent_info → dict từ .torrent
        """
        self.peer_id      = peer_id
        self.host         = host
        self.port         = port
        self.filepath     = filepath
        self.torrent_info = torrent_info
        self.chunk_size   = torrent_info.get("chunk_size", DEFAULT_CHUNK_SIZE)

        self._chunk_hashes = {
            c["index"]: c["hash"]
            for c in torrent_info["chunks"]
        }

        self._server_sock = None
        self._running     = False
        self._lock        = threading.Lock()

        # Thống kê
        self.total_uploaded = 0
        self.chunks_served  = 0

    def start(self) -> None:
        """Bắt đầu lắng nghe — blocking."""
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.port))
        self._server_sock.listen(20)
        self._running = True
        print(f"[Uploader] {self.peer_id} lắng nghe tại {self.host}:{self.port}")

        while self._running:
            try:
                self._server_sock.settimeout(1.0)
                conn, addr = self._server_sock.accept()
                peer_conn  = PeerConnection("unknown", addr[0], addr[1], sock=conn)
                t = threading.Thread(
                    target=self._serve_peer,
                    args=(peer_conn,),
                    daemon=True
                )
                t.start()
            except socket.timeout:
                continue
            except OSError:
                if self._running:
                    raise
                break

        print(f"[Uploader] Đã dừng. Đã phục vụ {self.chunks_served} chunks "
              f"({self.total_uploaded/1024:.1f} KB)")

    def start_background(self) -> threading.Thread:
        """Chạy uploader trong thread nền."""
        t = threading.Thread(target=self.start, daemon=True)
        t.start()
        time.sleep(0.3)   # chờ bind xong
        return t

    def stop(self) -> None:
        self._running = False
        if self._server_sock:
            try:
                self._server_sock.close()
            except OSError:
                pass

    def _serve_peer(self, peer_conn: PeerConnection) -> None:
        """Phục vụ 1 peer đến khi ngắt kết nối."""
        try:
            my_chunks = list(self._chunk_hashes.keys())
            ok = peer_conn.do_handshake_receiver(
                my_peer_id=self.peer_id,
                info_hash=self.torrent_info["info_hash"],
                my_chunks=my_chunks
            )
            if not ok:
                peer_conn.disconnect()
                return

            print(f"[Uploader] Phục vụ peer {peer_conn.peer_id}")

            while self._running and peer_conn.is_connected:
                msg = peer_conn.recv_msg(timeout=30.0)
                if msg is None:
                    break

                if msg.get("type") == P.MSG_REQUEST:
                    self._handle_request(peer_conn, msg["data"])
                elif msg.get("type") == P.MSG_BYE:
                    break

        except Exception as e:
            print(f"[Uploader] Lỗi: {e}")
        finally:
            peer_conn.disconnect()

    def _handle_request(self, peer_conn: PeerConnection, data: dict) -> None:
        """Đọc chunk từ file và gửi cho peer."""
        chunk_index = data.get("chunk_index")
        if chunk_index is None or chunk_index not in self._chunk_hashes:
            peer_conn.send_msg(P.MSG_ERROR, {"message": "Chunk không hợp lệ"})
            return

        try:
            chunk_data = self._read_chunk_data(chunk_index)
            if chunk_data is None:
                peer_conn.send_msg(P.MSG_ERROR, {"message": "Không đọc được chunk"})
                return

            if not verify_chunk(chunk_data, self._chunk_hashes[chunk_index]):
                peer_conn.send_msg(P.MSG_ERROR, {"message": "Hash lỗi"})
                return

            ok = peer_conn.send_piece(chunk_index, chunk_data)
            if ok:
                with self._lock:
                    self.total_uploaded += len(chunk_data)
                    self.chunks_served  += 1
                print(f"[Uploader] → chunk {chunk_index} ({len(chunk_data):,}b) → {peer_conn.peer_id}")

        except Exception as e:
            print(f"[Uploader] Lỗi chunk {chunk_index}: {e}")

    def _read_chunk_data(self, chunk_index: int) -> bytes | None:
        """Đọc chunk từ chunk_dir trước, nếu không có thì đọc file gốc."""
        # Ưu tiên chunk_dir (partial seeder hoặc đã tải xong)
        chunk_dir = getattr(self, "_chunk_dir", None)
        if chunk_dir:
            path = os.path.join(chunk_dir, f"chunk_{chunk_index:03d}")
            if os.path.exists(path):
                with open(path, "rb") as f:
                    return f.read()

        # Fallback: đọc từ file gốc
        if self.filepath and isinstance(self.filepath, str) and os.path.exists(self.filepath):
            return read_chunk(self.filepath, chunk_index, self.chunk_size)

        return None

    def get_stats(self) -> dict:
        with self._lock:
            return {
                "total_uploaded_bytes": self.total_uploaded,
                "total_uploaded_mb":    round(self.total_uploaded/1024/1024, 2),
                "chunks_served":        self.chunks_served,
            }
