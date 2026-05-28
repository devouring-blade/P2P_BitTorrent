"""
demo_advanced.py
================
Demo 2 kịch bản:
  1. Tải song song từ nhiều peer — thấy rõ chunk nào từ peer nào
  2. Peer rớt mạng giữa chừng — hệ thống tự phục hồi

Chạy:
    python demo_advanced.py [--scenario 1|2|all]
"""

import os, sys, time, threading, tempfile, shutil, argparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common.torrent_parser  import create_torrent, load_torrent
from common.file_handler    import merge_chunks
from common.hash_utils      import hash_file
from peer.piece_manager     import PieceManager
from peer.uploader          import Uploader
from peer.downloader        import Downloader
from peer.tracker_client    import TrackerClient
from tracker.tracker_server import TrackerServer


# ── Màu sắc terminal ──────────────────────────────────────────
class Color:
    RED    = "\033[91m"
    GREEN  = "\033[92m"
    YELLOW = "\033[93m"
    BLUE   = "\033[94m"
    PURPLE = "\033[95m"
    CYAN   = "\033[96m"
    WHITE  = "\033[97m"
    BOLD   = "\033[1m"
    RESET  = "\033[0m"

def cprint(color, text):
    print(f"{color}{text}{Color.RESET}")

def header(text):
    print(f"\n{Color.BOLD}{'═'*60}{Color.RESET}")
    print(f"{Color.BOLD}  {text}{Color.RESET}")
    print(f"{Color.BOLD}{'═'*60}{Color.RESET}")

def step(n, text):
    print(f"\n{Color.CYAN}[Bước {n}]{Color.RESET} {text}")


# ── Downloader với log chi tiết ───────────────────────────────
class VerboseDownloader(Downloader):
    """
    Kế thừa Downloader, override để in log chi tiết:
    - Chunk nào lấy từ peer nào
    - Màu sắc phân biệt từng peer
    """
    PEER_COLORS = [Color.GREEN, Color.YELLOW, Color.PURPLE,
                   Color.CYAN,  Color.WHITE]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._peer_colors  = {}
        self._color_index  = 0
        self._color_lock   = threading.Lock()
        self._chunk_log    = []   # [(chunk_index, peer_id, size, ms)]
        self._log_lock     = threading.Lock()

    def _get_peer_color(self, peer_id):
        with self._color_lock:
            if peer_id not in self._peer_colors:
                color = self.PEER_COLORS[self._color_index % len(self.PEER_COLORS)]
                self._peer_colors[peer_id] = color
                self._color_index += 1
            return self._peer_colors[peer_id]

    def _download_from_peer(self, conn, peer_chunks):
        """Override để thêm log màu sắc."""
        from common import protocol as P
        from common.hash_utils  import verify_chunk
        from common.file_handler import save_chunk

        color = self._get_peer_color(conn.peer_id)

        if not conn.connect(timeout=10.0):
            cprint(Color.RED, f"  ✗ Không kết nối được {conn.peer_id} ({conn.ip}:{conn.port})")
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

        cprint(color, f"  ● Kết nối {conn.peer_id} ({conn.ip}:{conn.port}) "
                      f"| peer có {len(peer_chunks)} chunks")

        try:
            while not self.piece_manager.is_complete():
                chunk_index = self.piece_manager.next_needed(peer_has=peer_chunks)
                if chunk_index is None:
                    cprint(color, f"  ○ {conn.peer_id}: không còn chunk nào cần tải")
                    break

                ok = conn.send_msg(P.MSG_REQUEST, {"chunk_index": chunk_index})
                if not ok:
                    self.piece_manager.mark_failed(chunk_index)
                    cprint(Color.RED, f"  ✗ {conn.peer_id}: mất kết nối khi gửi REQUEST chunk {chunk_index}")
                    break

                t_start = time.time()
                result  = conn.recv_piece(timeout=30.0)

                if result is None:
                    self.piece_manager.mark_failed(chunk_index)
                    with self._lock:
                        self.failed_chunks += 1
                    cprint(Color.RED,
                           f"  ✗ {conn.peer_id}: RỚTMẠNG khi đang nhận chunk {chunk_index}!")
                    cprint(Color.YELLOW,
                           f"    → chunk {chunk_index} được đưa lại hàng chờ để peer khác tải")
                    break

                recv_index, data = result
                elapsed_ms = (time.time() - t_start) * 1000

                expected = self._chunk_hashes.get(recv_index)
                if not verify_chunk(data, expected):
                    self.piece_manager.mark_failed(recv_index)
                    cprint(Color.RED, f"  ✗ Hash sai chunk {recv_index} từ {conn.peer_id}!")
                    with self._lock:
                        self.failed_chunks += 1
                    continue

                save_chunk(self.chunk_dir, recv_index, data)
                self.piece_manager.mark_complete(recv_index)

                with self._lock:
                    self.total_downloaded  += len(data)
                    self.chunks_downloaded += 1

                # Log đẹp
                have    = self.piece_manager.num_have()
                total   = self.torrent_info["num_chunks"]
                pct     = self.piece_manager.progress() * 100
                bar_len = 20
                filled  = int(bar_len * pct / 100)
                bar     = "█" * filled + "░" * (bar_len - filled)

                cprint(color,
                       f"  ✓ chunk {recv_index:03d} ← {conn.peer_id:12s} "
                       f"| {len(data)/1024:.0f}KB | {elapsed_ms:.0f}ms "
                       f"| [{bar}] {pct:.0f}%")

                with self._log_lock:
                    self._chunk_log.append((recv_index, conn.peer_id,
                                           len(data), elapsed_ms))

                if self.on_chunk_complete:
                    self.on_chunk_complete(recv_index)

        except Exception as e:
            cprint(Color.RED, f"  ✗ Lỗi không mong đợi từ {conn.peer_id}: {e}")
        finally:
            conn.send_msg(P.MSG_BYE, {})
            conn.disconnect()

    def print_summary(self):
        """In bảng tổng kết: chunk nào từ peer nào."""
        print(f"\n{Color.BOLD}{'─'*60}{Color.RESET}")
        cprint(Color.BOLD, "  BẢNG TỔNG KẾT — CHUNK NÀO TỪ PEER NÀO")
        print(f"{Color.BOLD}{'─'*60}{Color.RESET}")

        # Đếm chunk từng peer
        peer_stats = {}
        for chunk_idx, peer_id, size, ms in sorted(self._chunk_log):
            color = self._get_peer_color(peer_id)
            if peer_id not in peer_stats:
                peer_stats[peer_id] = {"chunks": [], "total": 0, "color": color}
            peer_stats[peer_id]["chunks"].append(chunk_idx)
            peer_stats[peer_id]["total"] += size

        for peer_id, info in peer_stats.items():
            color   = info["color"]
            chunks  = sorted(info["chunks"])
            total   = info["total"] / 1024
            cprint(color,
                   f"  {peer_id:15s} → chunks {chunks} "
                   f"| {len(chunks)} chunks | {total:.1f} KB")

        stats = self.get_stats()
        print(f"\n  Tổng: {stats['chunks_downloaded']} chunks | "
              f"{stats['total_downloaded_mb']:.2f} MB | "
              f"{stats['elapsed_sec']:.2f}s | "
              f"{stats['avg_speed_kbps']:.0f} KB/s")
        if stats['failed_chunks'] > 0:
            cprint(Color.YELLOW,
                   f"  Chunks tải lại: {stats['failed_chunks']} "
                   f"(do peer rớt mạng hoặc hash sai)")


# ── Helpers ───────────────────────────────────────────────────
_port = [21500]
def next_port():
    p = _port[0]; _port[0] += 1; return p

def make_file(path, size_mb):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "wb") as f:
        f.write(os.urandom(int(size_mb * 1024 * 1024)))

def start_tracker(port):
    t = TrackerServer("127.0.0.1", port)
    threading.Thread(target=t.start, daemon=True).start()
    time.sleep(0.3)
    return t


# ════════════════════════════════════════════════════════════════
# KỊCH BẢN 1: TẢI SONG SONG TỪ NHIỀU PEER
# ════════════════════════════════════════════════════════════════

def scenario_parallel(tmp, use_real_file=None):
    header("KỊCH BẢN 1: TẢI SONG SONG TỪ NHIỀU PEER")

    print("""
  Mục tiêu chứng minh:
  ┌─────────────────────────────────────────────────┐
  │  Leecher tải từ NHIỀU peer cùng lúc             │
  │  Mỗi peer cung cấp 1 PHẦN của file              │
  │  → Thấy rõ chunk nào đến từ peer nào            │
  └─────────────────────────────────────────────────┘
    """)

    t_port  = next_port()
    tracker = start_tracker(t_port)

    # Dùng file thật hoặc tạo file test
    if use_real_file and os.path.exists(use_real_file):
        src = use_real_file
        cprint(Color.GREEN, f"  Dùng file thật: {src}")
    else:
        src = os.path.join(tmp, "demo.bin")
        make_file(src, 2)
        cprint(Color.YELLOW, f"  Tạo file test: 2MB")

    # Tạo torrent với chunk nhỏ để thấy nhiều chunk
    torrent_path = create_torrent(
        src,
        tracker_url=f"127.0.0.1:{t_port}",
        chunk_size=256 * 1024,   # 256KB → thấy nhiều chunk hơn
        output_dir=tmp
    )
    torrent   = load_torrent(torrent_path)
    num_c     = torrent["num_chunks"]
    info_hash = torrent["info_hash"]

    cprint(Color.CYAN,
           f"\n  File: {torrent['name']} | "
           f"{torrent['file_size']:,} bytes | "
           f"{num_c} chunks x 256KB")

    # ── Khởi động 2 seeder ──────────────────────────────────
    step(1, "Khởi động 2 Seeder (cùng file, port khác nhau)")

    ports = [next_port(), next_port()]
    uploaders = []
    for i, port in enumerate(ports):
        pid = f"seeder_{chr(65+i)}"   # seeder_A, seeder_B
        up  = Uploader(pid, "127.0.0.1", port, src, torrent)
        up.start_background()
        sc = TrackerClient("127.0.0.1", t_port, pid, port)
        sc.register(info_hash, list(range(num_c)))
        uploaders.append(up)
        cprint(Color.GREEN, f"  ✓ {pid} online tại port {port}")

    time.sleep(0.3)

    # ── Leecher tải song song ────────────────────────────────
    step(2, "Leecher bắt đầu tải từ 2 seeder SONG SONG")
    print()

    chunk_dir = os.path.join(tmp, "leecher_chunks")
    pm        = PieceManager(num_c)
    dl        = VerboseDownloader("leecher", torrent, pm, chunk_dir)

    lc = TrackerClient("127.0.0.1", t_port, "leecher", 9999)
    lc.register(info_hash, [])
    peers = lc.get_peers(info_hash)

    cprint(Color.CYAN, f"  Tracker trả về {len(peers)} peer(s): "
                       f"{[p['peer_id'] for p in peers]}")
    print()

    success = dl.download_from_peers(peers, max_connections=2)

    # ── Kết quả ─────────────────────────────────────────────
    step(3, "Kết quả")

    if success:
        out = os.path.join(tmp, "leecher_output.bin")
        merge_chunks(chunk_dir, out, num_c)
        ok = os.path.exists(out) and hash_file(src) == hash_file(out)
        cprint(Color.GREEN if ok else Color.RED,
               f"\n  File tải về: {'ĐÚNG ✓' if ok else 'SAI ✗'} "
               f"(SHA256 {'khớp' if ok else 'không khớp'} với file gốc)")

    dl.print_summary()

    for up in uploaders:
        up.stop()
    tracker.stop()
    return success


# ════════════════════════════════════════════════════════════════
# KỊCH BẢN 2: PEER RỚT MẠNG GIỮA CHỪNG
# ════════════════════════════════════════════════════════════════

def scenario_peer_drop(tmp, use_real_file=None):
    header("KỊCH BẢN 2: PEER RỚT MẠNG GIỮA CHỪNG")

    print("""
  Mục tiêu chứng minh:
  ┌─────────────────────────────────────────────────┐
  │  seeder_A rớt mạng sau 1 giây                   │
  │  seeder_B tiếp tục → leecher vẫn hoàn thành     │
  │  → Hệ thống KHÔNG phụ thuộc 1 peer duy nhất     │
  └─────────────────────────────────────────────────┘
    """)

    t_port  = next_port()
    tracker = start_tracker(t_port)

    if use_real_file and os.path.exists(use_real_file):
        src = use_real_file
        cprint(Color.GREEN, f"  Dùng file thật: {src}")
    else:
        src = os.path.join(tmp, "demo2.bin")
        make_file(src, 2)
        cprint(Color.YELLOW, f"  Tạo file test: 2MB")

    torrent_path = create_torrent(
        src,
        tracker_url=f"127.0.0.1:{t_port}",
        chunk_size=256 * 1024,
        output_dir=tmp
    )
    torrent   = load_torrent(torrent_path)
    num_c     = torrent["num_chunks"]
    info_hash = torrent["info_hash"]

    cprint(Color.CYAN,
           f"\n  File: {torrent['name']} | {num_c} chunks x 256KB")

    # ── 2 seeder ────────────────────────────────────────────
    step(1, "Khởi động seeder_A và seeder_B")

    port_a = next_port()
    port_b = next_port()
    up_a   = Uploader("seeder_A", "127.0.0.1", port_a, src, torrent)
    up_b   = Uploader("seeder_B", "127.0.0.1", port_b, src, torrent)
    up_a.start_background()
    up_b.start_background()

    sc_a = TrackerClient("127.0.0.1", t_port, "seeder_A", port_a)
    sc_b = TrackerClient("127.0.0.1", t_port, "seeder_B", port_b)
    sc_a.register(info_hash, list(range(num_c)))
    sc_b.register(info_hash, list(range(num_c)))

    cprint(Color.GREEN, f"  ✓ seeder_A online tại port {port_a}")
    cprint(Color.GREEN, f"  ✓ seeder_B online tại port {port_b}")

    # ── Lên lịch tắt seeder_A sau 1 giây ───────────────────
    step(2, "Lên lịch: seeder_A sẽ RỚT MẠNG sau 1 giây")
    cprint(Color.YELLOW, "  ⏱  Đồng hồ đếm ngược bắt đầu...")

    killed_at = [None]
    def kill_seeder_a():
        time.sleep(1.0)
        up_a.stop()
        killed_at[0] = time.time()
        cprint(Color.RED, "\n  💀 seeder_A ĐÃ RỚT MẠNG!")
        cprint(Color.YELLOW,
               "     → Các chunk đang pending sẽ được seeder_B tải thay")

    threading.Thread(target=kill_seeder_a, daemon=True).start()

    # ── Leecher tải ─────────────────────────────────────────
    step(3, "Leecher bắt đầu tải (sẽ thấy seeder_A mất giữa chừng)")
    print()

    chunk_dir = os.path.join(tmp, "leecher2_chunks")
    pm        = PieceManager(num_c)
    dl        = VerboseDownloader("leecher", torrent, pm, chunk_dir)

    lc = TrackerClient("127.0.0.1", t_port, "leecher", 9998)
    lc.register(info_hash, [])
    peers = lc.get_peers(info_hash)

    cprint(Color.CYAN, f"  Peers ban đầu: {[p['peer_id'] for p in peers]}\n")

    # Lần 1: tải từ cả 2 peer (seeder_A sẽ rớt giữa chừng)
    success = dl.download_from_peers(peers, max_connections=2)

    # Nếu chưa xong (do seeder_A rớt trước khi hoàn thành)
    if not success and not pm.is_complete():
        step(4, "Lần 1 chưa xong — hỏi tracker lại và retry với peer còn lại")
        remaining = pm.get_missing()
        cprint(Color.YELLOW,
               f"  Còn thiếu {len(remaining)} chunk: {remaining[:5]}{'...' if len(remaining) > 5 else ''}")
        cprint(Color.CYAN, "  Hỏi tracker: ai còn online?")

        peers2 = lc.get_peers(info_hash)
        cprint(Color.GREEN,
               f"  Tracker trả về: {[p['peer_id'] for p in peers2]} (seeder_A đã bị xóa)")
        print()

        success = dl.download_from_peers(peers2, max_connections=1)

    # ── Kết quả ─────────────────────────────────────────────
    step(5 if not success else 4, "Kết quả cuối cùng")

    final_ok = pm.is_complete()
    if final_ok:
        out = os.path.join(tmp, "leecher2_output.bin")
        merge_chunks(chunk_dir, out, num_c)
        file_ok = hash_file(src) == hash_file(out)

        cprint(Color.GREEN, f"\n  ✓ TẢI HOÀN CHỈNH dù seeder_A đã rớt mạng!")
        cprint(Color.GREEN if file_ok else Color.RED,
               f"  File: {'ĐÚNG ✓' if file_ok else 'SAI ✗'} "
               f"(SHA256 {'khớp' if file_ok else 'không khớp'})")
    else:
        cprint(Color.RED, "  ✗ Tải chưa hoàn chỉnh")

    dl.print_summary()

    up_b.stop()
    tracker.stop()
    return final_ok


# ════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="P2P BitTorrent Advanced Demo")
    parser.add_argument("--scenario", default="all",
                        choices=["1", "2", "all"],
                        help="1=song song, 2=rớt mạng, all=cả hai")
    parser.add_argument("--file", default=None,
                        help="Dùng file thật thay vì file test (VD: video.mp4)")
    args = parser.parse_args()

    tmp = tempfile.mkdtemp(prefix="p2p_demo_")
    print(f"\n{Color.BOLD}P2P BitTorrent — Advanced Demo{Color.RESET}")
    print(f"Thư mục tạm: {tmp}")

    results = {}
    try:
        if args.scenario in ("1", "all"):
            results["parallel"] = scenario_parallel(tmp, args.file)

        if args.scenario in ("2", "all"):
            results["peer_drop"] = scenario_peer_drop(tmp, args.file)

        # Tổng kết
        header("TỔNG KẾT")
        for name, ok in results.items():
            icon = "✓" if ok else "✗"
            color = Color.GREEN if ok else Color.RED
            label = {
                "parallel":  "Tải song song nhiều peer",
                "peer_drop": "Peer rớt mạng giữa chừng"
            }.get(name, name)
            cprint(color, f"  {icon} {label}: {'THÀNH CÔNG' if ok else 'THẤT BẠI'}")
        print()

    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
