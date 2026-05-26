"""
create_torrent.py
=================
Tool tạo file .torrent từ command line.

Dùng:
    python create_torrent.py <file> [--tracker 127.0.0.1:6969] [--chunk 512]
"""

import argparse, sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common.torrent_parser import create_torrent, load_torrent, print_torrent_info

def main():
    parser = argparse.ArgumentParser(description="Tạo file .torrent")
    parser.add_argument("file",    help="File muốn chia sẻ")
    parser.add_argument("--tracker", default="127.0.0.1:6969",
                        help="Địa chỉ tracker (mặc định: 127.0.0.1:6969)")
    parser.add_argument("--chunk", type=int, default=512,
                        help="Kích thước chunk (KB, mặc định: 512)")
    parser.add_argument("--out",   default=".",
                        help="Thư mục lưu file .torrent")
    args = parser.parse_args()

    if not os.path.exists(args.file):
        print(f"Lỗi: File '{args.file}' không tồn tại!")
        sys.exit(1)

    torrent_path = create_torrent(
        filepath=args.file,
        tracker_url=args.tracker,
        chunk_size=args.chunk * 1024,
        output_dir=args.out
    )
    info = load_torrent(torrent_path)
    print_torrent_info(info)
    print(f"\n✓ File .torrent đã tạo: {torrent_path}")

if __name__ == "__main__":
    main()
