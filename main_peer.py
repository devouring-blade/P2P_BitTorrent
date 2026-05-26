"""
main_peer.py
============
Chạy 1 peer (seeder hoặc leecher).

Dùng:
    # Seed (chia sẻ file):
    python main_peer.py seed --id peer1 --port 7001 --file video.mp4 --torrent video.mp4.torrent

    # Download (tải file):
    python main_peer.py download --id peer2 --port 7002 --torrent video.mp4.torrent
"""

import argparse, sys, os, uuid
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from peer.peer_node import PeerNode

def main():
    parser = argparse.ArgumentParser(description="P2P Peer Node")
    sub    = parser.add_subparsers(dest="mode", required=True)

    # Seed mode
    sp = sub.add_parser("seed", help="Chia sẻ file")
    sp.add_argument("--id",      default=f"peer-{uuid.uuid4().hex[:6]}")
    sp.add_argument("--host",    default="127.0.0.1")
    sp.add_argument("--port",    type=int, required=True)
    sp.add_argument("--file",    required=True, help="Đường dẫn file gốc")
    sp.add_argument("--torrent", required=True, help="Đường dẫn file .torrent")
    sp.add_argument("--tracker-host", default="127.0.0.1")
    sp.add_argument("--tracker-port", type=int, default=6969)

    # Download mode
    dp = sub.add_parser("download", help="Tải file")
    dp.add_argument("--id",      default=f"peer-{uuid.uuid4().hex[:6]}")
    dp.add_argument("--host",    default="127.0.0.1")
    dp.add_argument("--port",    type=int, required=True)
    dp.add_argument("--torrent", required=True)
    dp.add_argument("--out",     default="downloads")
    dp.add_argument("--tracker-host", default="127.0.0.1")
    dp.add_argument("--tracker-port", type=int, default=6969)

    args = parser.parse_args()

    node = PeerNode(
        peer_id=args.id,
        host=args.host,
        upload_port=args.port,
        tracker_host=args.tracker_host,
        tracker_port=args.tracker_port,
        download_dir=getattr(args, "out", "downloads")
    )

    print(f"\nPeer ID : {args.id}")
    print(f"Address : {args.host}:{args.port}")
    print(f"Tracker : {args.tracker_host}:{args.tracker_port}\n")

    if args.mode == "seed":
        node.seed(args.torrent, args.file)
    else:
        result = node.download(args.torrent)
        if result:
            print(f"\n✓ Đã tải về: {result}")
        else:
            print("\n✗ Tải thất bại!")
            sys.exit(1)

if __name__ == "__main__":
    main()
