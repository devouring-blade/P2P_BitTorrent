"""
test_module2.py
===============
Test Module 2: Protocol + PeerManager + TrackerAPI + TrackerServer

Gồm 2 nhóm test:
  A. Unit test (không cần socket): protocol, peer_manager, tracker_api
  B. Integration test             : chạy server thật, kết nối socket thật

Chạy:
    cd p2p_torrent
    python -m tests.test_module2
"""

import os, sys, time, socket, threading, json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common              import protocol as P
from common.protocol     import encode_message, decode_message
from tracker.peer_manager import PeerManager
from tracker.tracker_api  import TrackerAPI
from tracker.tracker_server import TrackerServer


# ════════════════════════════════════════════════════════════════
# NHÓM A — Unit tests (không cần socket)
# ════════════════════════════════════════════════════════════════

def test_protocol_encode_decode():
    """encode → decode phải cho lại đúng message gốc."""
    raw = encode_message("REGISTER", {"peer_id": "abc", "port": 6881})
    assert raw.endswith(b"\n"), "Phải kết thúc bằng newline"
    msg = decode_message(raw.decode())
    assert msg["type"] == "REGISTER"
    assert msg["data"]["peer_id"] == "abc"


def test_protocol_empty_data():
    """Message không có data vẫn encode/decode được."""
    raw = encode_message("HEARTBEAT")
    msg = decode_message(raw.decode())
    assert msg["type"] == "HEARTBEAT"
    assert msg["data"] == {}


def test_peer_manager_register():
    pm = PeerManager()
    pm.register("hash1", "peer1", "192.168.1.2", 6881, [0, 1])
    peers = pm.get_peers("hash1")
    assert len(peers) == 1
    assert peers[0]["ip"]   == "192.168.1.2"
    assert peers[0]["port"] == 6881
    assert 0 in peers[0]["chunks"]


def test_peer_manager_exclude_self():
    """get_peers phải loại trừ chính peer đang hỏi."""
    pm = PeerManager()
    pm.register("hash1", "peer1", "1.1.1.1", 6881, [0])
    pm.register("hash1", "peer2", "2.2.2.2", 6882, [1])

    peers = pm.get_peers("hash1", exclude_peer_id="peer1")
    assert len(peers) == 1
    assert peers[0]["peer_id"] == "peer2"


def test_peer_manager_update_chunks():
    pm = PeerManager()
    pm.register("hash1", "peer1", "1.1.1.1", 6881, [])
    pm.update_chunks("hash1", "peer1", 3)
    pm.update_chunks("hash1", "peer1", 7)

    peers = pm.get_peers("hash1")
    assert 3 in peers[0]["chunks"]
    assert 7 in peers[0]["chunks"]


def test_peer_manager_unregister():
    pm = PeerManager()
    pm.register("hash1", "peer1", "1.1.1.1", 6881, [0])
    pm.unregister("hash1", "peer1")
    peers = pm.get_peers("hash1")
    assert len(peers) == 0


def test_peer_manager_timeout():
    """Peer không heartbeat phải bị xóa sau PEER_TIMEOUT."""
    from tracker.peer_manager import PEER_TIMEOUT
    pm = PeerManager()
    pm.register("hash1", "peer1", "1.1.1.1", 6881, [0])

    # Giả lập peer đã timeout bằng cách set last_seen về quá khứ
    with pm._lock:
        pm._db["hash1"]["peer1"]["last_seen"] = time.time() - PEER_TIMEOUT - 1

    peers = pm.get_peers("hash1")   # trigger _remove_dead_peers
    assert len(peers) == 0, "Peer timeout phải bị xóa"


def test_peer_manager_multiple_torrents():
    pm = PeerManager()
    pm.register("hashA", "peer1", "1.1.1.1", 6881, [0, 1])
    pm.register("hashB", "peer2", "2.2.2.2", 6882, [0])

    peersA = pm.get_peers("hashA")
    peersB = pm.get_peers("hashB")
    assert len(peersA) == 1
    assert len(peersB) == 1
    assert peersA[0]["peer_id"] == "peer1"
    assert peersB[0]["peer_id"] == "peer2"


def test_tracker_api_register():
    api = TrackerAPI(PeerManager())
    msg = {"type": P.MSG_REGISTER, "data": {
        "info_hash": "abc123", "peer_id": "p1",
        "port": 6881, "chunks": [0, 1, 2]
    }}
    resp = api.handle(msg, "127.0.0.1")
    assert resp["type"] == P.MSG_OK


def test_tracker_api_get_peers():
    pm  = PeerManager()
    api = TrackerAPI(pm)

    # Đăng ký 2 peer
    api.handle({"type": P.MSG_REGISTER, "data": {
        "info_hash": "abc", "peer_id": "pA", "port": 6881, "chunks": [0]
    }}, "1.1.1.1")
    api.handle({"type": P.MSG_REGISTER, "data": {
        "info_hash": "abc", "peer_id": "pB", "port": 6882, "chunks": [1]
    }}, "2.2.2.2")

    # pA hỏi danh sách peer, chỉ nhận được pB
    resp = api.handle({"type": P.MSG_GET_PEERS, "data": {
        "info_hash": "abc", "peer_id": "pA"
    }}, "1.1.1.1")
    assert resp["type"] == P.MSG_PEER_LIST
    assert resp["data"]["count"] == 1
    assert resp["data"]["peers"][0]["peer_id"] == "pB"


def test_tracker_api_have():
    pm  = PeerManager()
    api = TrackerAPI(pm)
    api.handle({"type": P.MSG_REGISTER, "data": {
        "info_hash": "abc", "peer_id": "p1", "port": 6881, "chunks": []
    }}, "1.1.1.1")

    resp = api.handle({"type": P.MSG_HAVE, "data": {
        "info_hash": "abc", "peer_id": "p1", "chunk_index": 5
    }}, "1.1.1.1")
    assert resp["type"] == P.MSG_OK

    peers = pm.get_peers("abc")
    assert 5 in peers[0]["chunks"]


def test_tracker_api_unknown_message():
    api  = TrackerAPI(PeerManager())
    resp = api.handle({"type": "INVALID_MSG", "data": {}}, "1.1.1.1")
    assert resp["type"] == P.MSG_ERROR


def test_tracker_api_missing_field():
    """Thiếu field bắt buộc → ERROR, không crash."""
    api  = TrackerAPI(PeerManager())
    resp = api.handle({"type": P.MSG_REGISTER, "data": {
        "peer_id": "p1"   # thiếu info_hash, port
    }}, "1.1.1.1")
    assert resp["type"] == P.MSG_ERROR


# ════════════════════════════════════════════════════════════════
# NHÓM B — Integration test (socket thật)
# ════════════════════════════════════════════════════════════════

BASE_PORT = 16970
TEST_PORT = 16969   # dùng port khác để không xung đột


_port_counter = [17000]

def start_test_tracker():
    """Khởi động tracker trong thread riêng, trả về (TrackerServer, port)."""
    port = _port_counter[0]
    _port_counter[0] += 1
    tracker = TrackerServer("127.0.0.1", port)
    t = threading.Thread(target=tracker.start, daemon=True)
    t.start()
    time.sleep(0.3)
    return tracker, port


def make_client(port):
    """Tạo socket client kết nối đến test tracker."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect(("127.0.0.1", port))
    return s


def send_recv(sock, msg_type, data=None):
    """Gửi message và nhận response từ tracker."""
    sock.sendall(encode_message(msg_type, data))
    raw = b""
    while True:
        chunk = sock.recv(1)
        if not chunk or chunk == b"\n":
            break
        raw += chunk
    return json.loads(raw.decode())


def test_integration_register_and_get_peers():
    """
    Luồng thực tế:
      seeder  → REGISTER (có chunk 0,1,2)
      leecher → REGISTER (chưa có chunk)
      leecher → GET_PEERS → nhận được seeder
    """
    tracker, port = start_test_tracker()
    seeder  = make_client(port)
    leecher = make_client(port)

    try:
        # Seeder đăng ký
        r1 = send_recv(seeder, P.MSG_REGISTER, {
            "info_hash": "torrent_xyz", "peer_id": "seeder_01",
            "port": 7001, "chunks": [0, 1, 2]
        })
        assert r1["type"] == P.MSG_OK, f"Seeder register thất bại: {r1}"

        # Leecher đăng ký
        r2 = send_recv(leecher, P.MSG_REGISTER, {
            "info_hash": "torrent_xyz", "peer_id": "leecher_01",
            "port": 7002, "chunks": []
        })
        assert r2["type"] == P.MSG_OK, f"Leecher register thất bại: {r2}"

        # Leecher hỏi ai có file → nhận được seeder
        r3 = send_recv(leecher, P.MSG_GET_PEERS, {
            "info_hash": "torrent_xyz", "peer_id": "leecher_01"
        })
        assert r3["type"] == P.MSG_PEER_LIST, f"Expected PEER_LIST: {r3}"
        assert r3["data"]["count"] == 1
        assert r3["data"]["peers"][0]["peer_id"] == "seeder_01"
        assert 0 in r3["data"]["peers"][0]["chunks"]

    finally:
        seeder.close()
        leecher.close()
        tracker.stop()


def test_integration_have_updates_peer_list():
    """
    Leecher tải xong chunk 0 → gửi HAVE →
    GET_PEERS phải thấy leecher có chunk 0.
    """
    tracker, port = start_test_tracker()
    seeder  = make_client(port)
    leecher = make_client(port)
    observer = make_client(port)   # peer thứ 3 để quan sát

    try:
        send_recv(seeder, P.MSG_REGISTER, {
            "info_hash": "hash_have", "peer_id": "s1",
            "port": 7001, "chunks": [0, 1, 2]
        })
        send_recv(leecher, P.MSG_REGISTER, {
            "info_hash": "hash_have", "peer_id": "l1",
            "port": 7002, "chunks": []
        })
        send_recv(observer, P.MSG_REGISTER, {
            "info_hash": "hash_have", "peer_id": "obs1",
            "port": 7003, "chunks": []
        })

        # Leecher vừa tải xong chunk 0
        r = send_recv(leecher, P.MSG_HAVE, {
            "info_hash": "hash_have", "peer_id": "l1", "chunk_index": 0
        })
        assert r["type"] == P.MSG_OK

        # Observer hỏi peer list → thấy leecher có chunk 0
        r2 = send_recv(observer, P.MSG_GET_PEERS, {
            "info_hash": "hash_have", "peer_id": "obs1"
        })
        peer_map = {p["peer_id"]: p for p in r2["data"]["peers"]}
        assert "l1" in peer_map
        assert 0 in peer_map["l1"]["chunks"]

    finally:
        seeder.close(); leecher.close(); observer.close()
        tracker.stop()


def test_integration_heartbeat():
    """Heartbeat gửi được, tracker trả OK."""
    tracker, port = start_test_tracker()
    peer    = make_client(port)

    try:
        send_recv(peer, P.MSG_REGISTER, {
            "info_hash": "hb_test", "peer_id": "p1",
            "port": 7001, "chunks": [0]
        })
        r = send_recv(peer, P.MSG_HEARTBEAT, {
            "info_hash": "hb_test", "peer_id": "p1"
        })
        assert r["type"] == P.MSG_OK

    finally:
        peer.close()
        tracker.stop()


def test_integration_unregister():
    """Sau khi UNREGISTER, GET_PEERS không trả về peer đó nữa."""
    tracker, port = start_test_tracker()
    peer_a  = make_client(port)
    peer_b  = make_client(port)

    try:
        send_recv(peer_a, P.MSG_REGISTER, {
            "info_hash": "unreg_test", "peer_id": "pA",
            "port": 7001, "chunks": [0, 1]
        })
        send_recv(peer_b, P.MSG_REGISTER, {
            "info_hash": "unreg_test", "peer_id": "pB",
            "port": 7002, "chunks": []
        })

        # pA rời mạng
        send_recv(peer_a, P.MSG_UNREGISTER, {
            "info_hash": "unreg_test", "peer_id": "pA"
        })

        # pB hỏi → không thấy pA nữa
        r = send_recv(peer_b, P.MSG_GET_PEERS, {
            "info_hash": "unreg_test", "peer_id": "pB"
        })
        peer_ids = [p["peer_id"] for p in r["data"]["peers"]]
        assert "pA" not in peer_ids, "pA đã unregister, không được xuất hiện"

    finally:
        peer_a.close(); peer_b.close()
        tracker.stop()


def test_integration_multi_peer_concurrent():
    """
    5 peer đăng ký cùng lúc (concurrent) → không bị race condition.
    """
    import uuid
    tracker, port = start_test_tracker()
    results = []
    lock    = threading.Lock()

    def register_peer(i):
        try:
            s = make_client(port)
            r = send_recv(s, P.MSG_REGISTER, {
                "info_hash": "concurrent_test",
                "peer_id":   f"peer_{i}",
                "port":      7000 + i,
                "chunks":    [i]
            })
            with lock:
                results.append(r["type"] == P.MSG_OK)
            s.close()
        except Exception as e:
            with lock:
                results.append(False)

    threads = [threading.Thread(target=register_peer, args=(i,)) for i in range(5)]
    for t in threads: t.start()
    for t in threads: t.join()

    tracker.stop()
    assert all(results), f"Concurrent register thất bại: {results}"


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
        ("Protocol encode/decode",           test_protocol_encode_decode),
        ("Protocol empty data",              test_protocol_empty_data),
        ("PeerManager register",             test_peer_manager_register),
        ("PeerManager exclude self",         test_peer_manager_exclude_self),
        ("PeerManager update chunks",        test_peer_manager_update_chunks),
        ("PeerManager unregister",           test_peer_manager_unregister),
        ("PeerManager timeout",              test_peer_manager_timeout),
        ("PeerManager multiple torrents",    test_peer_manager_multiple_torrents),
        ("TrackerAPI register",              test_tracker_api_register),
        ("TrackerAPI get_peers",             test_tracker_api_get_peers),
        ("TrackerAPI have",                  test_tracker_api_have),
        ("TrackerAPI unknown message",       test_tracker_api_unknown_message),
        ("TrackerAPI missing field",         test_tracker_api_missing_field),
    ]

    integration_tests = [
        ("Integration: register + get_peers",     test_integration_register_and_get_peers),
        ("Integration: HAVE updates peer list",   test_integration_have_updates_peer_list),
        ("Integration: heartbeat",                test_integration_heartbeat),
        ("Integration: unregister",               test_integration_unregister),
        ("Integration: 5 peer concurrent",        test_integration_multi_peer_concurrent),
    ]

    print("\n── UNIT TESTS ─────────────────────────────────────")
    p1 = sum(run_test(n, f) for n, f in unit_tests)

    print("\n── INTEGRATION TESTS ──────────────────────────────")
    p2 = sum(run_test(n, f) for n, f in integration_tests)

    total  = len(unit_tests) + len(integration_tests)
    passed = p1 + p2

    print("\n" + "=" * 60)
    if passed == total:
        print(f"  TAT CA {total}/{total} TEST PASSED! Module 2 hoat dong chinh xac.")
    else:
        print(f"  {passed}/{total} passed. Kiem tra lai cac test FAIL.")
    print("=" * 60)
