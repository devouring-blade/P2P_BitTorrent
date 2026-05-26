"""
protocol.py
===========
Định nghĩa giao thức giao tiếp giữa Peer <-> Tracker và Peer <-> Peer.

Tại sao cần protocol riêng?
- Mọi message đều có cùng format → dễ parse, dễ debug
- Thay đổi giao thức → chỉ sửa 1 file này

Format message: JSON + newline
  {"type": "REGISTER", "data": {...}}\n

Tại sao dùng JSON thay vì binary?
- Dễ đọc khi debug (dùng Wireshark, print)
- Đủ nhanh cho đồ án
- Binary (như BitTorrent thật) phức tạp hơn, không cần thiết

=== TRACKER PROTOCOL ===
  Client → Tracker:
    REGISTER  : peer báo "tôi đang chia sẻ torrent X, tôi có chunk [0,1,2,...]"
    GET_PEERS : peer hỏi "ai đang có torrent X?"
    HAVE      : peer báo "tôi vừa tải xong chunk N"
    HEARTBEAT : peer báo "tôi vẫn còn sống"
    UNREGISTER: peer báo "tôi rời mạng"

  Tracker → Client:
    OK        : thành công
    PEER_LIST : danh sách peer đang có torrent
    ERROR     : lỗi gì đó

=== PEER PROTOCOL (Module 4) ===
  REQUEST   : "cho tôi chunk N"
  PIECE     : "đây là chunk N, data = ..."
  BITFIELD  : "tôi đang có các chunk [0,2,5,...]"
  CHOKE     : "tôi tạm dừng gửi cho bạn"
  UNCHOKE   : "tôi gửi lại cho bạn"
"""

import json


# ── Tracker message types ──────────────────────────────────────
MSG_REGISTER   = "REGISTER"
MSG_GET_PEERS  = "GET_PEERS"
MSG_HAVE       = "HAVE"
MSG_HEARTBEAT  = "HEARTBEAT"
MSG_UNREGISTER = "UNREGISTER"

# ── Tracker response types ─────────────────────────────────────
MSG_OK         = "OK"
MSG_PEER_LIST  = "PEER_LIST"
MSG_ERROR      = "ERROR"

# ── Peer-to-Peer message types (dùng ở Module 4) ──────────────
MSG_REQUEST    = "REQUEST"
MSG_PIECE      = "PIECE"
MSG_BITFIELD   = "BITFIELD"
MSG_CHOKE      = "CHOKE"
MSG_UNCHOKE    = "UNCHOKE"
MSG_BYE        = "BYE"


def encode_message(msg_type: str, data: dict = None) -> bytes:
    """
    Đóng gói message thành bytes để gửi qua socket.

    Format: {"type": "...", "data": {...}}\n
    Thêm \n ở cuối để receiver biết message kết thúc chỗ nào.

    Input:
        msg_type -> loại message (VD: "REGISTER")
        data     -> dict payload (có thể None)
    Output: bytes
    """
    msg = {"type": msg_type, "data": data or {}}
    return (json.dumps(msg) + "\n").encode("utf-8")


def decode_message(raw: str) -> dict:
    """
    Giải mã message nhận được từ socket.

    Input:  chuỗi JSON (có thể có \n ở cuối)
    Output: dict {"type": ..., "data": ...}
    Raises: ValueError nếu JSON không hợp lệ
    """
    try:
        return json.loads(raw.strip())
    except json.JSONDecodeError as e:
        raise ValueError(f"Message không hợp lệ: {raw!r} | Lỗi: {e}")


def recv_message(sock) -> dict | None:
    """
    Nhận và decode 1 message từ socket.

    Đọc từng byte cho đến khi gặp \n (kết thúc message).
    Return None nếu kết nối bị đóng.
    """
    data = b""
    while True:
        try:
            chunk = sock.recv(1)           # đọc từng byte
            if not chunk:                  # kết nối đóng
                return None
            if chunk == b"\n":             # kết thúc message
                break
            data += chunk
        except OSError:
            return None

    if not data:
        return None

    return decode_message(data.decode("utf-8"))


def send_message(sock, msg_type: str, data: dict = None) -> None:
    """Gửi 1 message qua socket."""
    sock.sendall(encode_message(msg_type, data))
