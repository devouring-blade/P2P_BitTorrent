"""
file_handler.py
===============
Chức năng:
  1. split_file()  - Chia file lớn thành nhiều chunk nhỏ
  2. merge_chunks() - Ghép các chunk lại thành file hoàn chỉnh

Nguyên lý chia chunk:
  File 10MB, chunk_size = 512KB
  → Có 20 chunk, đánh số 0..19
  → Chunk cuối có thể nhỏ hơn 512KB (phần dư)

  [chunk_0][chunk_1][chunk_2]...[chunk_19]
   512KB    512KB    512KB        ~?KB
"""

import os
import math
from common.hash_utils import hash_chunk

# Kích thước mặc định mỗi chunk: 512 KB
DEFAULT_CHUNK_SIZE = 512 * 1024  # 524288 bytes


def split_file(filepath: str, chunk_size: int = DEFAULT_CHUNK_SIZE) -> list[dict]:
    """
    Chia file thành nhiều chunk và trả về danh sách metadata từng chunk.

    Input:
        filepath   -> đường dẫn file cần chia
        chunk_size -> kích thước mỗi chunk (bytes), mặc định 512KB

    Output: danh sách dict, mỗi dict là 1 chunk:
        [
            {
                "index": 0,          <- thứ tự chunk
                "offset": 0,         <- vị trí bắt đầu trong file (bytes)
                "size": 524288,      <- kích thước thực tế (bytes)
                "hash": "a3f1cc..."  <- SHA256 hash của chunk này
            },
            ...
        ]

    Tại sao lưu offset?
    -> Khi peer yêu cầu chunk thứ N, seeder biết đọc từ vị trí nào trong file.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Không tìm thấy file: {filepath}")

    file_size = os.path.getsize(filepath)
    if file_size == 0:
        raise ValueError("File rỗng, không thể chia chunk")

    # Tính số chunk cần thiết (làm tròn lên)
    num_chunks = math.ceil(file_size / chunk_size)
    chunks = []

    print(f"[FileHandler] Đang chia file: {filepath}")
    print(f"[FileHandler] Kích thước: {file_size:,} bytes | Chunk size: {chunk_size:,} bytes | Số chunk: {num_chunks}")

    with open(filepath, "rb") as f:
        for index in range(num_chunks):
            offset = index * chunk_size       # vị trí bắt đầu đọc
            data = f.read(chunk_size)         # đọc tối đa chunk_size bytes
            actual_size = len(data)           # chunk cuối có thể nhỏ hơn
            chunk_hash = hash_chunk(data)     # tính SHA256

            chunks.append({
                "index":  index,
                "offset": offset,
                "size":   actual_size,
                "hash":   chunk_hash
            })

            print(f"  Chunk {index:03d}: {actual_size:>8,} bytes | hash: {chunk_hash[:16]}...")

    print(f"[FileHandler] Chia xong! Tổng {len(chunks)} chunk.\n")
    return chunks


def read_chunk(filepath: str, chunk_index: int, chunk_size: int = DEFAULT_CHUNK_SIZE) -> bytes:
    """
    Đọc một chunk cụ thể từ file gốc.
    Dùng bởi Uploader khi peer khác request chunk này.

    Input:
        filepath    -> file gốc trên seeder
        chunk_index -> thứ tự chunk cần đọc (bắt đầu từ 0)
        chunk_size  -> kích thước mỗi chunk

    Output: bytes (nội dung chunk)
    """
    offset = chunk_index * chunk_size
    with open(filepath, "rb") as f:
        f.seek(offset)              # nhảy đến đúng vị trí
        data = f.read(chunk_size)   # đọc đúng chunk_size bytes
    return data


def merge_chunks(chunk_dir: str, output_filepath: str, num_chunks: int) -> bool:
    """
    Ghép tất cả chunk lại thành file hoàn chỉnh.

    Input:
        chunk_dir      -> thư mục chứa các file chunk (chunk_0, chunk_1, ...)
        output_filepath-> đường dẫn file kết quả
        num_chunks     -> tổng số chunk cần ghép

    Output:
        True  -> ghép thành công
        False -> thiếu chunk, ghép thất bại

    Cấu trúc thư mục chunk_dir:
        chunks/
            chunk_000
            chunk_001
            chunk_002
            ...
    """
    print(f"[FileHandler] Bắt đầu ghép {num_chunks} chunk → {output_filepath}")

    # Kiểm tra đủ chunk chưa
    missing = []
    for i in range(num_chunks):
        chunk_path = os.path.join(chunk_dir, f"chunk_{i:03d}")
        if not os.path.exists(chunk_path):
            missing.append(i)

    if missing:
        print(f"[FileHandler] Thiếu chunk: {missing}")
        return False

    # Ghép theo đúng thứ tự 0, 1, 2, ...
    os.makedirs(os.path.dirname(output_filepath) or ".", exist_ok=True)
    with open(output_filepath, "wb") as out:
        for i in range(num_chunks):
            chunk_path = os.path.join(chunk_dir, f"chunk_{i:03d}")
            with open(chunk_path, "rb") as chunk_file:
                out.write(chunk_file.read())
            print(f"  Đã ghép chunk {i:03d}")

    print(f"[FileHandler] Ghép xong! File: {output_filepath}\n")
    return True


def save_chunk(chunk_dir: str, chunk_index: int, data: bytes) -> str:
    """
    Lưu một chunk vào thư mục (dùng bởi Downloader sau khi tải về).

    Input:
        chunk_dir   -> thư mục lưu chunk
        chunk_index -> thứ tự chunk
        data        -> nội dung chunk (bytes)

    Output: đường dẫn file chunk đã lưu
    """
    os.makedirs(chunk_dir, exist_ok=True)
    chunk_path = os.path.join(chunk_dir, f"chunk_{chunk_index:03d}")
    with open(chunk_path, "wb") as f:
        f.write(data)
    return chunk_path
