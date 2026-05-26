"""
hash_utils.py
=============
Chức năng: Tính và kiểm tra SHA256 hash cho từng chunk.

Tại sao cần hash?
- Khi tải chunk từ peer, không biết chunk đó có bị lỗi/giả mạo không.
- Dùng SHA256: tính hash chunk tải về → so sánh với hash trong .torrent
- Nếu khớp → chunk hợp lệ. Nếu không → bỏ, tải lại từ peer khác.
"""

import hashlib
import hmac


def hash_chunk(data: bytes) -> str:
    """
    Tính SHA256 hash của một đoạn dữ liệu (chunk).

    Input : data  -> bytes (nội dung chunk)
    Output: string hex 64 ký tự, ví dụ: "a3f1cc..."
    """
    return hashlib.sha256(data).hexdigest()


def verify_chunk(data: bytes, expected_hash: str) -> bool:
    """
    Kiểm tra chunk có đúng không bằng cách so sánh hash.

    Input:
        data          -> bytes (chunk vừa tải về)
        expected_hash -> string (hash lưu trong file .torrent)
    Output:
        True  -> chunk hợp lệ
        False -> chunk bị lỗi, cần tải lại

    Dùng hmac.compare_digest thay vì == để tránh timing attack.
    """
    actual_hash = hash_chunk(data)
    return hmac.compare_digest(actual_hash, expected_hash)


def hash_file(filepath: str) -> str:
    """
    Tính SHA256 hash của toàn bộ file.
    Đọc từng 8KB để không tràn RAM với file lớn.
    """
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            sha256.update(chunk)
    return sha256.hexdigest()
