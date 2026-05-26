"""
simulate_peers.py
=================
Mô phỏng hệ thống P2P với nhiều peer hoạt động đồng thời.

Kịch bản mô phỏng:
  1. Khởi động Tracker
  2. Tạo file test + .torrent
  3. Khởi động 1 Seeder
  4. Lần lượt 3 Leecher tham gia tải (join)
  5. Leecher đầu tiên tải xong → tự trở thành seeder
  6. Mô phỏng peer churn: peer rời mạng giữa chừng
  7. In thống kê tổng kết

Đây là script demo cho thầy thấy hệ thống hoạt động thật.
"""

import os, sys, time, threading, tempfile, shutil, random

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common.torrent_parser  import create_torrent, load_torrent
from common.file_handler    import merge_chunks, save_chunk
from common.hash_utils      import hash_file
from peer.piece_manager     import PieceManager
from peer.uploader          import Uploader
from peer.downloader        import Downloader
from peer.tracker_client    import TrackerClient
from tracker.tracker_server import TrackerServer


# ── Cấu hình mô phỏng ─────────────────────────────────────────
TRACKER_PORT = 7969
BASE_PORT    = 8000
FILE_SIZE_KB = 2048    # 2MB file test
CHUNK_SIZE   = 256 * 1024  # 256KB chunks
NUM_LEECHERS = 3


def banner(text: str) -> None:
    print(f"\n{'='*55}")
    print(f"  {text}")
    print(f"{'='*55}")


def log(peer_id: str, msg: str) -> None:
    t = time.strftime("%H:%M:%S")
    print(f"  [{t}] [{peer_id:<12}] {msg}")


class SimulatedPeer:
    """Đại diện cho 1 peer trong mô phỏng."""
    def __init__(self, peer_id, port, tmp_dir, torrent, is_seeder=False,
                 src_file=None):
        self.peer_id   = peer_id
        self.port      = port
        self.tmp_dir   = tmp_dir
        self.torrent   = torrent
        self.is_seeder = is_seeder
        self.src_file  = src_file

        self.chunk_dir    = os.path.join(tmp_dir, f"{peer_id}_chunks")
        self.output_file  = os.path.join(tmp_dir, f"{peer_id}_output.bin")
        self.pm           = None
        self.uploader     = None
        self.downloader   = None
        self.tracker_cli  = None
        self.thread       = None

        self.start_time   = None
        self.end_time     = None
        self.success      = False

    def setup_as_seeder(self) -> None:
        """Cấu hình peer như seeder (đã có đủ chunk)."""
        num_chunks = self.torrent["num_chunks"]
        self.uploader = Uploader(
            self.peer_id, "127.0.0.1", self.port,
            self.src_file, self.torrent
        )
        self.uploader.start_background()

        self.tracker_cli = TrackerClient(
            "127.0.0.1", TRACKER_PORT, self.peer_id, self.port
        )
        self.tracker_cli.register(
            self.torrent["info_hash"], list(range(num_chunks))
        )
        self.tracker_cli.start_heartbeat(self.torrent["info_hash"])
        log(self.peer_id, f"🌱 Seeder online | {num_chunks} chunks sẵn sàng")

    def run_as_leecher(self, delay: float = 0) -> None:
        """Chạy peer như leecher trong thread riêng."""
        def _run():
            time.sleep(delay)   # join mạng sau một khoảng delay
            self.start_time = time.time()
            log(self.peer_id, "⬇  Bắt đầu tải...")

            num_chunks = self.torrent["num_chunks"]
            self.pm    = PieceManager(num_chunks)

            self.tracker_cli = TrackerClient(
                "127.0.0.1", TRACKER_PORT, self.peer_id, self.port
            )
            self.tracker_cli.register(self.torrent["info_hash"], [])
            self.tracker_cli.start_heartbeat(self.torrent["info_hash"])

            self.downloader = Downloader(
                self.peer_id, self.torrent, self.pm, self.chunk_dir
            )

            def on_chunk_done(idx):
                self.tracker_cli.have(self.torrent["info_hash"], idx)

            self.downloader.on_chunk_complete = on_chunk_done

            # Lấy peers và tải
            for attempt in range(5):
                peers = self.tracker_cli.get_peers(self.torrent["info_hash"])
                if not peers:
                    log(self.peer_id, f"Chờ peer... (lần {attempt+1})")
                    time.sleep(2)
                    continue

                log(self.peer_id, f"Tìm thấy {len(peers)} peer(s)")
                ok = self.downloader.download_from_peers(peers)
                if ok or self.pm.is_complete():
                    break

            if self.pm.is_complete():
                # Ghép file
                os.makedirs(os.path.dirname(self.output_file) or ".", exist_ok=True)
                merge_chunks(self.chunk_dir, self.output_file, num_chunks)
                self.end_time = time.time()
                elapsed = self.end_time - self.start_time
                stats   = self.downloader.get_stats()
                self.success = True

                log(self.peer_id,
                    f"✓ Tải xong! {elapsed:.1f}s | "
                    f"{stats['avg_speed_kbps']:.0f} KB/s")

                # Chuyển thành seeder
                self.uploader = Uploader(
                    self.peer_id, "127.0.0.1", self.port,
                    self.src_file, self.torrent
                )
                self.tracker_cli.register(
                    self.torrent["info_hash"],
                    list(range(num_chunks))
                )
                log(self.peer_id, "🔄 Chuyển thành Seeder")
            else:
                log(self.peer_id, "✗ Tải thất bại!")

        self.thread = threading.Thread(target=_run, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.tracker_cli.stop_heartbeat()
        self.tracker_cli.unregister(self.torrent["info_hash"])
        if self.uploader:
            self.uploader.stop()


def run_simulation():
    tmp = tempfile.mkdtemp(prefix="p2p_sim_")
    print(f"\nThư mục tạm: {tmp}")

    try:
        # ── Bước 1: Khởi động Tracker ──────────────────────
        banner("BƯỚC 1: Khởi động Tracker")
        tracker = TrackerServer("127.0.0.1", TRACKER_PORT)
        threading.Thread(target=tracker.start, daemon=True).start()
        time.sleep(0.3)
        log("TRACKER", f"Online tại 127.0.0.1:{TRACKER_PORT}")

        # ── Bước 2: Tạo file test + torrent ───────────────
        banner("BƯỚC 2: Tạo file test")
        src = os.path.join(tmp, "demo_file.bin")
        with open(src, "wb") as f:
            f.write(os.urandom(FILE_SIZE_KB * 1024))

        torrent_path = create_torrent(
            src,
            tracker_url=f"127.0.0.1:{TRACKER_PORT}",
            chunk_size=CHUNK_SIZE,
            output_dir=tmp
        )
        torrent = load_torrent(torrent_path)
        log("SYSTEM", f"File: {FILE_SIZE_KB}KB | {torrent['num_chunks']} chunks | "
                      f"Chunk size: {CHUNK_SIZE//1024}KB")

        # ── Bước 3: Khởi động Seeder ───────────────────────
        banner("BƯỚC 3: Seeder tham gia mạng")
        seeder = SimulatedPeer(
            "seeder", BASE_PORT, tmp, torrent,
            is_seeder=True, src_file=src
        )
        seeder.setup_as_seeder()
        time.sleep(0.5)

        # ── Bước 4: Leecher tham gia lần lượt ─────────────
        banner(f"BƯỚC 4: {NUM_LEECHERS} Leecher tham gia mạng")
        leechers = []
        for i in range(NUM_LEECHERS):
            peer = SimulatedPeer(
                f"leecher_{i+1}", BASE_PORT + i + 1,
                tmp, torrent, src_file=src
            )
            peer.run_as_leecher(delay=i * 1.5)  # join so le nhau
            leechers.append(peer)
            log(f"leecher_{i+1}", f"Sẽ tham gia sau {i*1.5:.1f}s")

        # ── Bước 5: Mô phỏng peer churn ───────────────────
        banner("BƯỚC 5: Mô phỏng peer churn (peer join/leave)")
        def churn_simulation():
            time.sleep(3)
            log("CHURN", "Seeder gốc rời mạng tạm thời (3 giây)...")
            seeder.uploader.stop()
            time.sleep(3)
            seeder.uploader = Uploader(
                "seeder", "127.0.0.1", BASE_PORT, src, torrent
            )
            seeder.uploader.start_background()
            seeder.tracker_cli.register(
                torrent["info_hash"], list(range(torrent["num_chunks"]))
            )
            log("CHURN", "Seeder quay lại mạng!")

        threading.Thread(target=churn_simulation, daemon=True).start()

        # ── Bước 6: Chờ tất cả leecher xong ──────────────
        banner("BƯỚC 6: Chờ tất cả leecher hoàn thành...")
        for leecher in leechers:
            leecher.thread.join(timeout=60)

        # ── Bước 7: Thống kê ──────────────────────────────
        banner("BƯỚC 7: KẾT QUẢ MÔ PHỎNG")
        src_hash = hash_file(src)
        all_ok   = True

        print(f"\n  {'Peer':<14} {'Kết quả':<10} {'Thời gian':<12} {'File OK'}")
        print(f"  {'-'*55}")

        for leecher in leechers:
            if leecher.success and os.path.exists(leecher.output_file):
                elapsed  = leecher.end_time - leecher.start_time
                file_ok  = hash_file(leecher.output_file) == src_hash
                status   = "✓ Xong" if leecher.success else "✗ Thất bại"
                file_str = "✓ Đúng" if file_ok else "✗ Sai"
                print(f"  {leecher.peer_id:<14} {status:<10} {elapsed:<12.1f} {file_str}")
                if not file_ok:
                    all_ok = False
            else:
                print(f"  {leecher.peer_id:<14} {'✗ Thất bại':<10} {'N/A':<12} {'N/A'}")
                all_ok = False

        print(f"\n  Upload stats (seeder): "
              f"{seeder.uploader.get_stats()['chunks_served']} chunks served | "
              f"{seeder.uploader.get_stats()['total_uploaded_mb']} MB uploaded")

        print(f"\n{'='*55}")
        if all_ok:
            print(f"  ✓ MÔ PHỎNG THÀNH CÔNG! Tất cả peer nhận đúng file.")
        else:
            print(f"  ✗ Có lỗi trong mô phỏng.")
        print(f"{'='*55}\n")

        # Dọn dẹp
        seeder.stop()
        for l in leechers:
            if l.tracker_cli:
                l.stop()
        tracker.stop()

        return all_ok

    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    success = run_simulation()
    sys.exit(0 if success else 1)
