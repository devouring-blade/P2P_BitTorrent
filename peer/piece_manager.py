"""
piece_manager.py
================
Theo dõi trạng thái từng chunk của peer.

Mỗi peer cần biết:
  - Tổng có bao nhiêu chunk (từ file .torrent)
  - Mình đang có chunk nào (đã tải xong)
  - Chunk nào đang tải dở (đang pending)
  - Chunk nào chưa có (cần tải)

Dữ liệu lưu dạng "bitfield" — mảng True/False:
  bitfield = [True, True, False, False, True]
             chunk0  1     2      3      4
  → peer đang có chunk 0, 1, 4

Tại sao cần PieceManager riêng?
  - Downloader và Uploader đều cần truy vấn trạng thái chunk
  - Tránh race condition khi nhiều thread cùng cập nhật
  - Tập trung logic "nên tải chunk nào tiếp theo" vào 1 chỗ
"""

import threading


class PieceManager:
    def __init__(self, num_chunks: int, have_chunks: list[int] = None):
        """
        num_chunks  → tổng số chunk của file (từ .torrent)
        have_chunks → danh sách chunk đã có sẵn (seeder truyền vào)
        """
        self.num_chunks = num_chunks
        self._lock      = threading.Lock()

        # bitfield[i] = True  → đã có chunk i
        self._bitfield  = [False] * num_chunks

        # pending = chunk đang được 1 thread tải, chưa verify xong
        # Tránh 2 thread cùng tải 1 chunk
        self._pending   = set()

        # Khởi tạo chunk có sẵn (seeder)
        if have_chunks:
            for idx in have_chunks:
                if 0 <= idx < num_chunks:
                    self._bitfield[idx] = True

    # ── Truy vấn ──────────────────────────────────────────────
    def have(self, chunk_index: int) -> bool:
        """Peer có chunk này chưa?"""
        with self._lock:
            return self._bitfield[chunk_index]

    def get_bitfield(self) -> list[bool]:
        """Trả về bản sao bitfield (để gửi cho peer khác)."""
        with self._lock:
            return list(self._bitfield)

    def get_have_list(self) -> list[int]:
        """Danh sách index các chunk đang có."""
        with self._lock:
            return [i for i, v in enumerate(self._bitfield) if v]

    def is_complete(self) -> bool:
        """Đã có đủ tất cả chunk chưa?"""
        with self._lock:
            return all(self._bitfield)

    def num_have(self) -> int:
        """Số chunk đã có."""
        with self._lock:
            return sum(self._bitfield)

    def progress(self) -> float:
        """Tiến độ tải (0.0 → 1.0)."""
        with self._lock:
            return sum(self._bitfield) / self.num_chunks

    # ── Lấy chunk cần tải tiếp theo ───────────────────────────
    def next_needed(self, peer_has: list[int] = None) -> int | None:
        """
        Trả về index chunk nên tải tiếp theo.

        Logic:
          1. Lọc chunk peer_has có mà mình chưa có và chưa pending
          2. Chọn chunk đầu tiên (sequential) — Module 5 sẽ nâng lên rarest-first
          3. Đánh dấu pending để thread khác không tải trùng
          4. Return index, hoặc None nếu không còn chunk nào cần

        peer_has → danh sách chunk mà peer kia đang có
                   Nếu None → lấy bất kỳ chunk nào mình chưa có
        """
        with self._lock:
            candidates = range(self.num_chunks)

            # Lọc: chưa có + chưa pending
            needed = [
                i for i in candidates
                if not self._bitfield[i] and i not in self._pending
            ]

            if not needed:
                return None

            # Nếu biết peer có gì → chỉ lấy chunk peer đó có
            if peer_has is not None:
                peer_set = set(peer_has)
                needed = [i for i in needed if i in peer_set]
                if not needed:
                    return None

            chosen = needed[0]              # sequential — đơn giản, đủ dùng
            self._pending.add(chosen)       # đánh dấu đang tải
            return chosen

    def next_needed_rarest_first(self, peers_bitfields: list[list[int]]) -> int | None:
        """
        Nâng cao (Module 5): chọn chunk HIẾM NHẤT để tải trước.

        Chunk hiếm = ít peer có → ưu tiên tải để tăng availability cho swarm.
        peers_bitfields → danh sách have_list của từng peer đang kết nối.
        """
        with self._lock:
            # Đếm số peer có từng chunk
            count = [0] * self.num_chunks
            for peer_chunks in peers_bitfields:
                for idx in peer_chunks:
                    if 0 <= idx < self.num_chunks:
                        count[idx] += 1

            # Lọc chunk chưa có + chưa pending + ít nhất 1 peer có
            candidates = [
                (count[i], i) for i in range(self.num_chunks)
                if not self._bitfield[i]
                and i not in self._pending
                and count[i] > 0
            ]

            if not candidates:
                return None

            # Sắp xếp: ít peer có → ưu tiên cao hơn
            candidates.sort()
            chosen = candidates[0][1]
            self._pending.add(chosen)
            return chosen

    # ── Cập nhật trạng thái ───────────────────────────────────
    def mark_complete(self, chunk_index: int) -> None:
        """
        Đánh dấu chunk đã tải và verify xong.
        Xóa khỏi pending, set bitfield = True.
        """
        with self._lock:
            self._bitfield[chunk_index] = True
            self._pending.discard(chunk_index)

    def mark_failed(self, chunk_index: int) -> None:
        """
        Chunk tải thất bại (hash sai, peer rớt...).
        Xóa khỏi pending để thread khác có thể thử lại.
        """
        with self._lock:
            self._pending.discard(chunk_index)

    def get_missing(self) -> list[int]:
        """Danh sách chunk còn thiếu (chưa có và không pending)."""
        with self._lock:
            return [
                i for i in range(self.num_chunks)
                if not self._bitfield[i] and i not in self._pending
            ]

    def __str__(self) -> str:
        have = self.num_have()
        return (f"PieceManager({have}/{self.num_chunks} chunks, "
                f"{self.progress()*100:.1f}% complete)")
