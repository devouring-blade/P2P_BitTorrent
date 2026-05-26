"""
test_module3.py
===============
Test Module 3: PieceManager + PeerConnection

Nhóm A — Unit test PieceManager  (không cần socket)
Nhóm B — Integration test PeerConnection (socket thật, 2 thread)

Chạy:
    cd p2p_torrent
    python -m tests.test_module3
"""

import os, sys, time, socket, threading, json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from peer.piece_manager  import PieceManager
from peer.peer_connection import PeerConnection
from common               import protocol as P


# ════════════════════════════════════════════════════════════════
# NHÓM A — Unit tests PieceManager
# ════════════════════════════════════════════════════════════════

def test_pm_initial_empty():
    pm = PieceManager(10)
    assert pm.num_have()   == 0
    assert pm.is_complete() == False
    assert pm.progress()    == 0.0
    assert pm.get_missing() == list(range(10))


def test_pm_initial_with_chunks():
    pm = PieceManager(5, have_chunks=[0, 2, 4])
    assert pm.num_have()    == 3
    assert pm.have(0)        == True
    assert pm.have(1)        == False
    assert pm.get_have_list() == [0, 2, 4]


def test_pm_mark_complete():
    pm = PieceManager(4)
    # Giả lập: next_needed pending chunk 0 rồi mark complete
    idx = pm.next_needed()
    assert idx == 0
    pm.mark_complete(0)
    assert pm.have(0) == True
    assert 0 not in pm.get_missing()


def test_pm_mark_failed_retry():
    """Chunk tải thất bại → xóa pending → có thể lấy lại."""
    pm  = PieceManager(4)
    idx = pm.next_needed()   # = 0, đánh dấu pending
    pm.mark_failed(idx)      # thất bại → xóa pending
    idx2 = pm.next_needed()  # phải lấy lại được chunk 0
    assert idx2 == 0


def test_pm_no_duplicate_pending():
    """2 lần gọi next_needed phải trả về chunk KHÁC NHAU."""
    pm = PieceManager(4)
    i1 = pm.next_needed()
    i2 = pm.next_needed()
    assert i1 != i2, "Hai thread không được tải cùng 1 chunk"


def test_pm_complete():
    pm = PieceManager(3)
    for i in range(3):
        pm.next_needed()
        pm.mark_complete(i)
    assert pm.is_complete() == True
    assert pm.progress()    == 1.0


def test_pm_progress():
    pm = PieceManager(10)
    for i in range(5):
        pm.next_needed()
        pm.mark_complete(i)
    assert pm.progress() == 0.5


def test_pm_next_needed_with_peer_filter():
    """next_needed chỉ trả về chunk peer kia đang có."""
    pm       = PieceManager(6)
    peer_has = [2, 4]
    idx      = pm.next_needed(peer_has=peer_has)
    assert idx in peer_has


def test_pm_rarest_first():
    """Rarest-first chọn chunk ít peer có nhất."""
    pm = PieceManager(4)
    # Peer A có chunk 0,1,2; Peer B có chunk 0,2 ; Peer C có chunk 0
    # → chunk 3 không ai có → bỏ
    # → chunk 1: 1 peer; chunk 2: 2 peer; chunk 0: 3 peer
    # → rarest = chunk 1
    peers = [[0, 1, 2], [0, 2], [0]]
    idx   = pm.next_needed_rarest_first(peers)
    assert idx == 1, f"Rarest là chunk 1, nhận được {idx}"


def test_pm_thread_safe():
    """
    50 thread cùng gọi next_needed → không có duplicate.
    Mỗi chunk chỉ được 1 thread lấy.
    """
    pm      = PieceManager(50)
    results = []
    lock    = threading.Lock()

    def grab():
        idx = pm.next_needed()
        if idx is not None:
            with lock:
                results.append(idx)

    threads = [threading.Thread(target=grab) for _ in range(50)]
    for t in threads: t.start()
    for t in threads: t.join()

    # Không có duplicate
    assert len(results) == len(set(results)), \
        f"Có duplicate trong kết quả: {sorted(results)}"


# ════════════════════════════════════════════════════════════════
# NHÓM B — Integration tests PeerConnection
# ════════════════════════════════════════════════════════════════

def make_connected_pair(port: int):
    """
    Tạo cặp (server_conn, client_conn) đã kết nối qua socket thật.
    server_conn → PeerConnection từ phía server (accept)
    client_conn → PeerConnection từ phía client (connect)
    """
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("127.0.0.1", port))
    server_sock.listen(1)

    accepted = [None]

    def accept_thread():
        conn, addr = server_sock.accept()
        accepted[0] = PeerConnection("server_peer", addr[0], addr[1], sock=conn)

    t = threading.Thread(target=accept_thread, daemon=True)
    t.start()

    client_conn = PeerConnection("client_peer", "127.0.0.1", port)
    client_conn.connect()
    t.join(timeout=3)
    server_sock.close()

    return accepted[0], client_conn


_port_base = [18000]
def next_port():
    p = _port_base[0]
    _port_base[0] += 1
    return p


def test_conn_basic_connect():
    port   = next_port()
    server_conn, client_conn = make_connected_pair(port)
    assert client_conn.is_connected
    assert server_conn.is_connected
    client_conn.disconnect()
    server_conn.disconnect()


def test_conn_send_recv_msg():
    """Client gửi JSON message → server nhận đúng."""
    port   = next_port()
    server_conn, client_conn = make_connected_pair(port)

    received = [None]
    def server_recv():
        received[0] = server_conn.recv_msg(timeout=3)

    t = threading.Thread(target=server_recv)
    t.start()
    client_conn.send_msg(P.MSG_REQUEST, {"chunk_index": 7})
    t.join(timeout=5)

    assert received[0] is not None
    assert received[0]["type"] == P.MSG_REQUEST
    assert received[0]["data"]["chunk_index"] == 7

    client_conn.disconnect()
    server_conn.disconnect()


def test_conn_send_recv_piece():
    """Server gửi chunk binary → client nhận đúng data và index."""
    port   = next_port()
    server_conn, client_conn = make_connected_pair(port)

    chunk_data = bytes(range(256)) * 512    # 128KB dữ liệu test
    received   = [None]

    def client_recv():
        received[0] = client_conn.recv_piece(timeout=5)

    t = threading.Thread(target=client_recv)
    t.start()
    server_conn.send_piece(5, chunk_data)
    t.join(timeout=5)

    assert received[0] is not None
    idx, data = received[0]
    assert idx  == 5
    assert data == chunk_data, "Data nhận phải khớp data gửi"

    client_conn.disconnect()
    server_conn.disconnect()


def test_conn_large_chunk():
    """Gửi chunk 512KB (kích thước thực tế) — test recv loop."""
    port      = next_port()
    server_conn, client_conn = make_connected_pair(port)
    big_data  = os.urandom(512 * 1024)   # 512KB random bytes
    received  = [None]

    def recv_thread():
        received[0] = client_conn.recv_piece(timeout=10)

    t = threading.Thread(target=recv_thread)
    t.start()
    server_conn.send_piece(0, big_data)
    t.join(timeout=10)

    assert received[0] is not None
    _, data = received[0]
    assert len(data) == 512 * 1024
    assert data == big_data

    client_conn.disconnect()
    server_conn.disconnect()


def test_conn_handshake():
    """Cả 2 phía thực hiện handshake, trao đổi bitfield."""
    port = next_port()
    server_conn, client_conn = make_connected_pair(port)

    server_ok = [False]
    client_ok = [False]

    def server_side():
        server_ok[0] = server_conn.do_handshake_receiver(
            my_peer_id="server01",
            info_hash="testhash",
            my_chunks=[0, 1, 2, 3]
        )

    def client_side():
        client_ok[0] = client_conn.do_handshake_initiator(
            my_peer_id="client01",
            info_hash="testhash",
            my_chunks=[4, 5]
        )

    ts = threading.Thread(target=server_side)
    tc = threading.Thread(target=client_side)
    ts.start(); tc.start()
    ts.join(3); tc.join(3)

    assert server_ok[0], "Server handshake thất bại"
    assert client_ok[0], "Client handshake thất bại"

    # Sau handshake, mỗi bên biết chunk của bên kia
    assert set(server_conn.remote_bitfield) == {4, 5}
    assert set(client_conn.remote_bitfield) == {0, 1, 2, 3}

    client_conn.disconnect()
    server_conn.disconnect()


def test_conn_full_flow():
    """
    Luồng đầy đủ:
      client → REQUEST(chunk 2)
      server → PIECE(2, data)
      client → verify nhận đúng
    """
    port       = next_port()
    server_conn, client_conn = make_connected_pair(port)
    chunk_data = os.urandom(512)    # 512 bytes
    received   = [None]
    error      = [None]

    def server_side():
        try:
            msg = server_conn.recv_msg(timeout=5)
            assert msg["type"] == P.MSG_REQUEST
            idx = msg["data"]["chunk_index"]
            server_conn.send_piece(idx, chunk_data)
        except Exception as e:
            error[0] = e

    def client_side():
        try:
            client_conn.send_msg(P.MSG_REQUEST, {"chunk_index": 2})
            received[0] = client_conn.recv_piece(timeout=5)
        except Exception as e:
            error[0] = e

    ts = threading.Thread(target=server_side)
    tc = threading.Thread(target=client_side)
    ts.start(); tc.start()
    ts.join(5); tc.join(5)

    assert error[0] is None, f"Có lỗi: {error[0]}"
    assert received[0] is not None
    idx, data = received[0]
    assert idx  == 2
    assert data == chunk_data

    client_conn.disconnect()
    server_conn.disconnect()


def test_conn_disconnect_graceful():
    """Peer ngắt kết nối → recv trả về None, không crash."""
    port = next_port()
    server_conn, client_conn = make_connected_pair(port)

    result = [None]
    def server_recv():
        result[0] = server_conn.recv_msg(timeout=3)

    t = threading.Thread(target=server_recv)
    t.start()

    time.sleep(0.1)
    client_conn.disconnect()   # ngắt kết nối phía client
    t.join(timeout=5)

    assert result[0] is None, "Sau khi peer disconnect phải nhận None"
    server_conn.disconnect()


def test_conn_stats():
    """Thống kê bytes_sent sau khi gửi."""
    port = next_port()
    server_conn, client_conn = make_connected_pair(port)
    received = [None]

    def recv():
        received[0] = server_conn.recv_msg(timeout=3)

    t = threading.Thread(target=recv)
    t.start()
    client_conn.send_msg(P.MSG_HEARTBEAT, {"info_hash": "x", "peer_id": "p1"})
    t.join(3)

    stats = client_conn.get_stats()
    assert stats["bytes_sent"] > 0
    assert stats["connected"] == True

    client_conn.disconnect()
    server_conn.disconnect()


# ════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════

def run_test(name, fn):
    try:
        fn()
        print(f"  [OK] {name}")
        return True
    except Exception as e:
        print(f"  [FAIL] {name}: {e}")
        import traceback; traceback.print_exc()
        return False


if __name__ == "__main__":
    unit_tests = [
        ("PieceManager: khởi tạo rỗng",          test_pm_initial_empty),
        ("PieceManager: khởi tạo có chunks",      test_pm_initial_with_chunks),
        ("PieceManager: mark_complete",           test_pm_mark_complete),
        ("PieceManager: mark_failed → retry",     test_pm_mark_failed_retry),
        ("PieceManager: không duplicate pending", test_pm_no_duplicate_pending),
        ("PieceManager: complete khi đủ chunk",   test_pm_complete),
        ("PieceManager: progress 50%",            test_pm_progress),
        ("PieceManager: filter theo peer",        test_pm_next_needed_with_peer_filter),
        ("PieceManager: rarest-first",            test_pm_rarest_first),
        ("PieceManager: thread-safe 50 thread",   test_pm_thread_safe),
    ]

    integration_tests = [
        ("PeerConnection: kết nối cơ bản",        test_conn_basic_connect),
        ("PeerConnection: gửi/nhận JSON msg",     test_conn_send_recv_msg),
        ("PeerConnection: gửi/nhận chunk binary", test_conn_send_recv_piece),
        ("PeerConnection: chunk 512KB",           test_conn_large_chunk),
        ("PeerConnection: handshake đầy đủ",      test_conn_handshake),
        ("PeerConnection: luồng REQUEST→PIECE",   test_conn_full_flow),
        ("PeerConnection: disconnect graceful",   test_conn_disconnect_graceful),
        ("PeerConnection: thống kê bytes",        test_conn_stats),
    ]

    print("\n── UNIT TESTS — PieceManager ──────────────────────")
    p1 = sum(run_test(n, f) for n, f in unit_tests)

    print("\n── INTEGRATION TESTS — PeerConnection ─────────────")
    p2 = sum(run_test(n, f) for n, f in integration_tests)

    total  = len(unit_tests) + len(integration_tests)
    passed = p1 + p2

    print("\n" + "=" * 60)
    if passed == total:
        print(f"  TAT CA {total}/{total} TEST PASSED! Module 3 hoat dong chinh xac.")
    else:
        print(f"  {passed}/{total} passed.")
    print("=" * 60)
