"""
main_peer.py - Chạy 1 peer (seeder hoặc leecher).

Dùng:
    # Seed:
    python main_peer.py seed --port 7001 --file video.mp4 --torrent video.mp4.torrent

    # Download:
    python main_peer.py download --port 7002 --torrent video.mp4.torrent
"""

import argparse, sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common.torrent_parser  import load_torrent
from common.file_handler    import merge_chunks
from common.hash_utils      import hash_file
from peer.piece_manager     import PieceManager
from peer.uploader          import Uploader
from peer.downloader        import Downloader
from peer.tracker_client    import TrackerClient
import time, uuid, threading


def cmd_seed(args):
    """Chạy seeder — chia sẻ file cho peer khác tải."""
    if not os.path.exists(args.file):
        print(f"Lỗi: Không tìm thấy file '{args.file}'")
        sys.exit(1)
    if not os.path.exists(args.torrent):
        print(f"Lỗi: Không tìm thấy file '{args.torrent}'")
        sys.exit(1)

    torrent   = load_torrent(args.torrent)
    info_hash = torrent["info_hash"]
    num_c     = torrent["num_chunks"]
    peer_id   = args.id

    print(f"\n{'='*50}")
    print(f"  CHẾ ĐỘ SEEDER")
    print(f"  Peer ID : {peer_id}")
    print(f"  Port    : {args.port}")
    print(f"  File    : {args.file} ({torrent['file_size']:,} bytes)")
    print(f"  Chunks  : {num_c}")
    print(f"  Tracker : {args.tracker_host}:{args.tracker_port}")
    print(f"{'='*50}\n")

    # Khởi động uploader
    uploader = Uploader(peer_id, "0.0.0.0", args.port, args.file, torrent)
    uploader.start_background()

    # Đăng ký với tracker
    tc = TrackerClient(args.tracker_host, args.tracker_port, peer_id, args.port)
    ok = tc.register(info_hash, list(range(num_c)))
    if not ok:
        print("Lỗi: Không kết nối được tracker! Kiểm tra main_tracker.py đang chạy chưa.")
        sys.exit(1)

    tc.start_heartbeat(info_hash)
    print(f"[Seeder] Đang seed... (Ctrl+C để dừng)\n")

    try:
        while True:
            time.sleep(5)
            stats = uploader.get_stats()
            print(f"[Seeder] Đã upload: {stats['chunks_served']} chunks | "
                  f"{stats['total_uploaded_mb']} MB")
    except KeyboardInterrupt:
        print("\n[Seeder] Đang dừng...")
        tc.stop_heartbeat()
        tc.unregister(info_hash)
        uploader.stop()


def cmd_download(args):
    """Chạy leecher — tải file từ mạng P2P."""
    if not os.path.exists(args.torrent):
        print(f"Lỗi: Không tìm thấy file '{args.torrent}'")
        sys.exit(1)

    torrent   = load_torrent(args.torrent)
    info_hash = torrent["info_hash"]
    num_c     = torrent["num_chunks"]
    peer_id   = args.id

    print(f"\n{'='*50}")
    print(f"  CHẾ ĐỘ LEECHER")
    print(f"  Peer ID : {peer_id}")
    print(f"  Port    : {args.port}")
    print(f"  File    : {torrent['name']} ({torrent['file_size']:,} bytes)")
    print(f"  Chunks  : {num_c}")
    print(f"  Tracker : {args.tracker_host}:{args.tracker_port}")
    print(f"{'='*50}\n")

    # Thư mục lưu chunk và output
    chunk_dir  = os.path.join(args.out, peer_id, torrent["name"] + "_chunks")
    output_dir = os.path.join(args.out, peer_id)
    output_file = os.path.join(output_dir, torrent["name"])
    os.makedirs(chunk_dir, exist_ok=True)

    # Đăng ký với tracker
    tc = TrackerClient(args.tracker_host, args.tracker_port, peer_id, args.port)
    ok = tc.register(info_hash, [])
    if not ok:
        print("Lỗi: Không kết nối được tracker! Kiểm tra main_tracker.py đang chạy chưa.")
        sys.exit(1)
    tc.start_heartbeat(info_hash)

    # Tải file
    pm = PieceManager(num_c)
    dl = Downloader(peer_id, torrent, pm, chunk_dir)

    def on_chunk_done(idx):
        tc.have(info_hash, idx)

    dl.on_chunk_complete = on_chunk_done

    # Thử tải tối đa 5 lần
    success = False
    for attempt in range(1, 6):
        print(f"\n[Leecher] Lần thử {attempt}/5 — Hỏi tracker danh sách peer...")
        peers = tc.get_peers(info_hash)

        if not peers:
            print(f"[Leecher] Không tìm thấy peer nào! Chờ 3 giây...")
            time.sleep(3)
            continue

        print(f"[Leecher] Tìm thấy {len(peers)} peer(s): {[p['peer_id'] for p in peers]}")
        success = dl.download_from_peers(peers)
        if success or pm.is_complete():
            break
        time.sleep(2)

    if pm.is_complete():
        # Ghép file
        print(f"\n[Leecher] Đang ghép {num_c} chunks...")
        os.makedirs(output_dir, exist_ok=True)
        merge_chunks(chunk_dir, output_file, num_c)
        size = os.path.getsize(output_file)
        print(f"\n{'='*50}")
        print(f"  ✓ TẢI HOÀN CHỈNH!")
        print(f"  File: {output_file}")
        print(f"  Kích thước: {size:,} bytes")
        stats = dl.get_stats()
        print(f"  Tốc độ TB: {stats['avg_speed_kbps']:.1f} KB/s")
        print(f"  Thời gian: {stats['elapsed_sec']:.1f}s")
        print(f"{'='*50}\n")

        # Chuyển thành seeder
        print(f"[Leecher] Chuyển sang chế độ SEEDER...")
        uploader = Uploader(peer_id, "0.0.0.0", args.port, output_file, torrent)
        uploader.start_background()
        tc.register(info_hash, list(range(num_c)))
        print(f"[Seeder] Đang seed lại cho peer khác... (Ctrl+C để dừng)\n")
        try:
            while True:
                time.sleep(10)
        except KeyboardInterrupt:
            pass
    else:
        print(f"\n✗ Tải thất bại sau 5 lần thử!")

    tc.stop_heartbeat()
    tc.unregister(info_hash)


def main():
    parser = argparse.ArgumentParser(description="P2P Peer Node")
    sub    = parser.add_subparsers(dest="mode", required=True)

    # Seed
    sp = sub.add_parser("seed")
    sp.add_argument("--id",           default=f"seeder-{uuid.uuid4().hex[:6]}")
    sp.add_argument("--port",         type=int, required=True)
    sp.add_argument("--file",         required=True)
    sp.add_argument("--torrent",      required=True)
    sp.add_argument("--tracker-host", default="127.0.0.1")
    sp.add_argument("--tracker-port", type=int, default=6969)

    # Download
    dp = sub.add_parser("download")
    dp.add_argument("--id",           default=f"leecher-{uuid.uuid4().hex[:6]}")
    dp.add_argument("--port",         type=int, required=True)
    dp.add_argument("--torrent",      required=True)
    dp.add_argument("--out",          default="downloads")
    dp.add_argument("--tracker-host", default="127.0.0.1")
    dp.add_argument("--tracker-port", type=int, default=6969)

    args = parser.parse_args()
    if args.mode == "seed":
        cmd_seed(args)
    else:
        cmd_download(args)


if __name__ == "__main__":
    main()
