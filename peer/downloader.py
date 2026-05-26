"""
downloader.py
=============
Tải chunk từ nhiều peer song song và lưu xuống đĩa.
"""

import threading
import time
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common             import protocol as P
from common.hash_utils   import verify_chunk
from common.file_handler import save_chunk, DEFAULT_CHUNK_SIZE
from peer.piece_manager  import PieceManager
from peer.peer_connection import PeerConnection


class Downloader:
    def __init__(self, peer_id: str, torrent_info: dict,
                 piece_manager: PieceManager, chunk_dir: str):
        self.peer_id       = peer_id
        self.torrent_info  = torrent_info
        self.piece_manager = piece_manager
        self.chunk_dir     = chunk_dir

        self._chunk_hashes = {
            c["index"]: c["hash"]
            for c in torrent_info["chunks"]
        }

        self.total_downloaded  = 0
        self.chunks_downloaded = 0
        self.failed_chunks     = 0
        self._start_time       = None
        self._lock             = threading.Lock()
        self.done_event        = threading.Event()
        self.on_chunk_complete = None

    def download_from_peers(self, peers: list[dict],
                            max_connections: int = 5) -> bool:
        if not peers:
            print(f"[Downloader] Không có peer nào!")
            return False

        self._start_time = time.time()
        print(f"[Downloader] Bắt đầu tải từ {len(peers)} peer(s)")

        active_peers = peers[:max_connections]
        threads      = []

        for peer_info in active_peers:
            conn = PeerConnection(
                peer_id=peer_info["peer_id"],
                ip=peer_info["ip"],
                port=peer_info["port"]
            )
            t = threading.Thread(
                target=self._download_from_peer,
                args=(conn, peer_info.get("chunks", [])),
                daemon=True
            )
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        if self.piece_manager.is_complete():
            elapsed = time.time() - self._start_time
            speed   = self.total_downloaded / max(elapsed,1) / 1024
            print(f"\n[Downloader] ✓ Tải hoàn chỉnh! "
                  f"{elapsed:.1f}s | {speed:.1f} KB/s")
            self.done_event.set()
            return True
        else:
            missing = self.piece_manager.get_missing()
            print(f"[Downloader] ✗ Còn thiếu {len(missing)} chunk")
            return False

    def _download_from_peer(self, conn: PeerConnection,
                            peer_chunks: list[int]) -> None:
        if not conn.connect(timeout=10.0):
            return

        my_chunks = self.piece_manager.get_have_list()
        ok = conn.do_handshake_initiator(
            my_peer_id=self.peer_id,
            info_hash=self.torrent_info["info_hash"],
            my_chunks=my_chunks
        )
        if not ok:
            conn.disconnect()
            return

        if conn.remote_bitfield:
            peer_chunks = conn.remote_bitfield

        print(f"[Downloader] Kết nối {conn.peer_id} | {len(peer_chunks)} chunks")

        try:
            while not self.piece_manager.is_complete():
                chunk_index = self.piece_manager.next_needed(peer_has=peer_chunks)
                if chunk_index is None:
                    break

                ok = conn.send_msg(P.MSG_REQUEST, {"chunk_index": chunk_index})
                if not ok:
                    self.piece_manager.mark_failed(chunk_index)
                    break

                result = conn.recv_piece(timeout=60.0)
                if result is None:
                    self.piece_manager.mark_failed(chunk_index)
                    with self._lock:
                        self.failed_chunks += 1
                    break

                recv_index, data = result
                expected = self._chunk_hashes.get(recv_index)
                if not expected or not verify_chunk(data, expected):
                    print(f"[Downloader] ✗ Hash sai chunk {recv_index}!")
                    self.piece_manager.mark_failed(recv_index)
                    with self._lock:
                        self.failed_chunks += 1
                    continue

                save_chunk(self.chunk_dir, recv_index, data)
                self.piece_manager.mark_complete(recv_index)

                with self._lock:
                    self.total_downloaded  += len(data)
                    self.chunks_downloaded += 1

                if self.on_chunk_complete:
                    self.on_chunk_complete(recv_index)

                progress = self.piece_manager.progress() * 100
                print(f"[Downloader] ✓ Chunk {recv_index:03d} | "
                      f"{self.piece_manager.num_have()}/"
                      f"{self.torrent_info['num_chunks']} | {progress:.1f}%")

        except Exception as e:
            print(f"[Downloader] Lỗi: {e}")
        finally:
            conn.send_msg(P.MSG_BYE, {})
            conn.disconnect()

    def get_stats(self) -> dict:
        elapsed = time.time() - self._start_time if self._start_time else 0
        with self._lock:
            return {
                "chunks_downloaded":   self.chunks_downloaded,
                "total_downloaded_mb": round(self.total_downloaded/1024/1024, 2),
                "failed_chunks":       self.failed_chunks,
                "progress_pct":        round(self.piece_manager.progress()*100, 1),
                "elapsed_sec":         round(elapsed, 1),
                "avg_speed_kbps":      round(
                    self.total_downloaded/max(elapsed,1)/1024, 1
                ),
            }
