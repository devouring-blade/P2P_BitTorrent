"""
torrent_parser.py
=================
Chức năng:
  1. create_torrent() - Tạo file .torrent (metadata dạng JSON)
  2. load_torrent()   - Đọc file .torrent vào dict Python

File .torrent là gì?
- Không chứa nội dung file thật (không phải file nhạc, phim...)
- Chỉ chứa THÔNG TIN VỀ file: tên, kích thước, hash từng chunk
- Peer dùng file này để biết cần tải gì, tải từ ai, verify thế nào

Cấu trúc file .torrent (JSON):
{
    "name":        "video.mp4",
    "file_size":   104857600,
    "chunk_size":  524288,
    "num_chunks":  200,
    "info_hash":   "abc123...",   <- hash của toàn bộ metadata (định danh torrent)
    "created_at":  "2024-01-01T10:00:00",
    "tracker_url": "127.0.0.1:6969",
    "chunks": [
        {"index": 0, "offset": 0,      "size": 524288, "hash": "a3f1..."},
        {"index": 1, "offset": 524288, "size": 524288, "hash": "b2c4..."},
        ...
    ]
}
"""

import json
import os
import hashlib
from datetime import datetime
from common.file_handler import split_file, DEFAULT_CHUNK_SIZE


def create_torrent(
    filepath: str,
    tracker_url: str = "127.0.0.1:6969",
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    output_dir: str = "."
) -> str:
    """
    Tạo file .torrent cho một file bất kỳ.

    Quy trình:
        1. Chia file thành chunks, lấy hash từng chunk
        2. Tạo dict metadata
        3. Tính info_hash (hash của toàn bộ metadata) để định danh torrent
        4. Lưu ra file .torrent (JSON)

    Input:
        filepath    -> đường dẫn file muốn chia sẻ (VD: "video.mp4")
        tracker_url -> địa chỉ tracker server
        chunk_size  -> kích thước chunk
        output_dir  -> thư mục lưu file .torrent

    Output: đường dẫn file .torrent vừa tạo
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File không tồn tại: {filepath}")

    filename  = os.path.basename(filepath)
    file_size = os.path.getsize(filepath)

    # Bước 1: chia file, lấy metadata từng chunk
    print(f"[Torrent] Đang tạo torrent cho: {filename}")
    chunks = split_file(filepath, chunk_size)

    # Bước 2: tạo dict metadata
    torrent_data = {
        "name":        filename,
        "file_size":   file_size,
        "chunk_size":  chunk_size,
        "num_chunks":  len(chunks),
        "tracker_url": tracker_url,
        "created_at":  datetime.now().isoformat(),
        "chunks":      chunks
    }

    # Bước 3: tính info_hash
    # Mục đích: mỗi torrent có 1 ID duy nhất để tracker phân biệt
    # Cách tính: hash của phần metadata (không kể info_hash và created_at)
    stable_data = {
        "name":       filename,
        "file_size":  file_size,
        "chunk_size": chunk_size,
        "chunks":     [{"index": c["index"], "hash": c["hash"]} for c in chunks]
    }
    info_hash = hashlib.sha256(
        json.dumps(stable_data, sort_keys=True).encode()
    ).hexdigest()
    torrent_data["info_hash"] = info_hash

    # Bước 4: lưu file .torrent
    torrent_filename = filename + ".torrent"
    torrent_path = os.path.join(output_dir, torrent_filename)
    os.makedirs(output_dir, exist_ok=True)

    with open(torrent_path, "w", encoding="utf-8") as f:
        json.dump(torrent_data, f, indent=2, ensure_ascii=False)

    print(f"[Torrent] Đã tạo: {torrent_path}")
    print(f"[Torrent] info_hash: {info_hash[:32]}...")
    print(f"[Torrent] Số chunk: {len(chunks)} | File size: {file_size:,} bytes\n")

    return torrent_path


def load_torrent(torrent_path: str) -> dict:
    """
    Đọc file .torrent và trả về dict metadata.

    Input:  đường dẫn file .torrent
    Output: dict chứa toàn bộ thông tin torrent

    Sử dụng:
        info = load_torrent("video.mp4.torrent")
        print(info["num_chunks"])   # 200
        print(info["chunks"][0])    # {"index":0, "hash":"...", ...}
    """
    if not os.path.exists(torrent_path):
        raise FileNotFoundError(f"File .torrent không tồn tại: {torrent_path}")

    with open(torrent_path, "r", encoding="utf-8") as f:
        torrent_data = json.load(f)

    # Validate các trường bắt buộc
    required_fields = ["name", "file_size", "chunk_size", "num_chunks", "info_hash", "chunks"]
    for field in required_fields:
        if field not in torrent_data:
            raise ValueError(f"File .torrent thiếu trường: {field}")

    print(f"[Torrent] Đã load: {torrent_path}")
    print(f"[Torrent] File: {torrent_data['name']} | {torrent_data['file_size']:,} bytes | {torrent_data['num_chunks']} chunks")

    return torrent_data


def print_torrent_info(torrent_data: dict) -> None:
    """In thông tin torrent ra màn hình (tiện debug)."""
    print("=" * 50)
    print(f"  Tên file   : {torrent_data['name']}")
    print(f"  Kích thước : {torrent_data['file_size']:,} bytes ({torrent_data['file_size']/1024/1024:.2f} MB)")
    print(f"  Chunk size : {torrent_data['chunk_size']:,} bytes")
    print(f"  Số chunk   : {torrent_data['num_chunks']}")
    print(f"  Tracker    : {torrent_data['tracker_url']}")
    print(f"  Info hash  : {torrent_data['info_hash'][:32]}...")
    print(f"  Tạo lúc    : {torrent_data.get('created_at', 'N/A')}")
    print("=" * 50)
