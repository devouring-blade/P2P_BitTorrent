"""
test_module1.py
===============
Test toàn bộ Module 1: hash_utils, file_handler, torrent_parser

Chạy lệnh:
    cd p2p_torrent
    python -m tests.test_module1

Kết quả mong đợi:
    [OK] hash_chunk
    [OK] verify_chunk - valid
    [OK] verify_chunk - invalid
    [OK] split_file
    [OK] read_chunk
    [OK] save_chunk + merge_chunks
    [OK] create_torrent
    [OK] load_torrent
    [OK] File gốc == File ghép (integrity check)
    ============================================================
    TAT CA TEST PASSED! Module 1 hoat dong chinh xac.
"""

import os
import sys
import shutil
import tempfile

# Thêm thư mục gốc vào Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.hash_utils import hash_chunk, verify_chunk, hash_file
from common.file_handler import split_file, read_chunk, merge_chunks, save_chunk
from common.torrent_parser import create_torrent, load_torrent, print_torrent_info


def create_test_file(path: str, size_kb: int = 2048) -> None:
    """Tạo file test ngẫu nhiên với kích thước cho trước."""
    import random
    with open(path, "wb") as f:
        # Viết từng 1KB ngẫu nhiên
        for _ in range(size_kb):
            f.write(bytes([random.randint(0, 255) for _ in range(1024)]))


def run_test(name: str, func):
    """Chạy 1 test, in kết quả PASS/FAIL."""
    try:
        func()
        print(f"  [OK] {name}")
        return True
    except Exception as e:
        print(f"  [FAIL] {name}: {e}")
        return False


def test_hash_chunk():
    data = b"Hello BitTorrent!"
    h = hash_chunk(data)
    assert len(h) == 64, "SHA256 phải là 64 ký tự hex"
    assert hash_chunk(data) == h, "Cùng data phải cho cùng hash"


def test_verify_chunk_valid():
    data = b"chunk data test"
    h = hash_chunk(data)
    assert verify_chunk(data, h) == True, "Chunk hợp lệ phải return True"


def test_verify_chunk_invalid():
    data = b"chunk data test"
    h = hash_chunk(data)
    corrupted = b"chunk data XXXX"   # data bị sửa
    assert verify_chunk(corrupted, h) == False, "Chunk lỗi phải return False"


def test_split_file(tmp_dir):
    test_file = os.path.join(tmp_dir, "test_input.bin")
    create_test_file(test_file, size_kb=1500)   # file 1.5MB

    chunk_size = 512 * 1024   # 512KB
    chunks = split_file(test_file, chunk_size)

    file_size = os.path.getsize(test_file)
    import math
    expected_num = math.ceil(file_size / chunk_size)

    assert len(chunks) == expected_num, f"Số chunk phải là {expected_num}"
    assert chunks[0]["index"] == 0, "Chunk đầu phải có index=0"
    assert chunks[-1]["index"] == len(chunks) - 1

    # Tổng size các chunk phải bằng file gốc
    total = sum(c["size"] for c in chunks)
    assert total == file_size, f"Tổng size chunk ({total}) != file size ({file_size})"


def test_read_chunk(tmp_dir):
    test_file = os.path.join(tmp_dir, "test_read.bin")
    create_test_file(test_file, size_kb=600)

    chunk_size = 512 * 1024
    # Đọc chunk 0
    data0 = read_chunk(test_file, 0, chunk_size)
    assert len(data0) == chunk_size, f"Chunk 0 phải đúng {chunk_size} bytes"

    # Đọc chunk 1 (chunk cuối, nhỏ hơn chunk_size)
    data1 = read_chunk(test_file, 1, chunk_size)
    file_size = os.path.getsize(test_file)
    expected_size1 = file_size - chunk_size
    assert len(data1) == expected_size1, f"Chunk 1 phải là {expected_size1} bytes"


def test_save_and_merge(tmp_dir):
    test_file = os.path.join(tmp_dir, "test_merge.bin")
    create_test_file(test_file, size_kb=1200)

    chunk_size = 512 * 1024
    chunks = split_file(test_file, chunk_size)

    # Giả lập download: đọc từng chunk, lưu vào chunk_dir
    chunk_dir = os.path.join(tmp_dir, "chunks")
    with open(test_file, "rb") as f:
        for c in chunks:
            f.seek(c["offset"])
            data = f.read(c["size"])
            # Verify trước khi lưu
            assert verify_chunk(data, c["hash"]), f"Chunk {c['index']} hash sai!"
            save_chunk(chunk_dir, c["index"], data)

    # Ghép lại
    output_file = os.path.join(tmp_dir, "test_output.bin")
    result = merge_chunks(chunk_dir, output_file, len(chunks))
    assert result == True, "Merge phải thành công"

    # So sánh file gốc và file ghép
    hash_original = hash_file(test_file)
    hash_output   = hash_file(output_file)
    assert hash_original == hash_output, "File ghép phải giống hệt file gốc!"


def test_create_torrent(tmp_dir):
    test_file = os.path.join(tmp_dir, "movie.mp4")
    create_test_file(test_file, size_kb=2000)

    torrent_path = create_torrent(
        filepath=test_file,
        tracker_url="127.0.0.1:6969",
        chunk_size=512*1024,
        output_dir=tmp_dir
    )

    assert os.path.exists(torrent_path), "File .torrent phải được tạo"
    assert torrent_path.endswith(".torrent"), "Phải có đuôi .torrent"


def test_load_torrent(tmp_dir):
    test_file = os.path.join(tmp_dir, "audio.mp3")
    create_test_file(test_file, size_kb=800)

    torrent_path = create_torrent(test_file, output_dir=tmp_dir)
    info = load_torrent(torrent_path)

    assert info["name"] == "audio.mp3"
    assert info["num_chunks"] == len(info["chunks"])
    assert len(info["info_hash"]) == 64, "info_hash phải là SHA256 (64 hex)"
    assert info["tracker_url"] == "127.0.0.1:6969"


def test_full_integrity(tmp_dir):
    """Test quan trọng nhất: tạo torrent → chia chunk → ghép → file giống hệt gốc."""
    original = os.path.join(tmp_dir, "original.bin")
    create_test_file(original, size_kb=3000)  # 3MB

    # Tạo torrent
    torrent_path = create_torrent(original, output_dir=tmp_dir)
    info = load_torrent(torrent_path)

    # Đọc + verify từng chunk
    chunk_dir = os.path.join(tmp_dir, "downloaded_chunks")
    with open(original, "rb") as f:
        for c in info["chunks"]:
            f.seek(c["offset"])
            data = f.read(c["size"])
            ok = verify_chunk(data, c["hash"])
            assert ok, f"Chunk {c['index']} verify thất bại"
            save_chunk(chunk_dir, c["index"], data)

    # Ghép
    restored = os.path.join(tmp_dir, "restored.bin")
    merge_chunks(chunk_dir, restored, info["num_chunks"])

    # So sánh
    assert hash_file(original) == hash_file(restored), \
        "File gốc và file ghép phải hoàn toàn giống nhau!"


# ===== MAIN =====
if __name__ == "__main__":
    tmp = tempfile.mkdtemp(prefix="p2p_test_")
    print(f"\nThư mục test tạm: {tmp}\n")

    tests = [
        ("hash_chunk",                   test_hash_chunk),
        ("verify_chunk - valid",          test_verify_chunk_valid),
        ("verify_chunk - invalid",        test_verify_chunk_invalid),
        ("split_file",                    lambda: test_split_file(tmp)),
        ("read_chunk",                    lambda: test_read_chunk(tmp)),
        ("save_chunk + merge_chunks",     lambda: test_save_and_merge(tmp)),
        ("create_torrent",               lambda: test_create_torrent(tmp)),
        ("load_torrent",                 lambda: test_load_torrent(tmp)),
        ("File goc == File ghep (integrity)", lambda: test_full_integrity(tmp)),
    ]

    passed = sum(run_test(name, fn) for name, fn in tests)
    total  = len(tests)

    print("\n" + "=" * 60)
    if passed == total:
        print(f"  TAT CA {total}/{total} TEST PASSED! Module 1 hoat dong chinh xac.")
    else:
        print(f"  {passed}/{total} passed. Kiem tra lai cac test bi FAIL.")
    print("=" * 60)

    shutil.rmtree(tmp)
