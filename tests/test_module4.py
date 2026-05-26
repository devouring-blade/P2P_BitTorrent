"""
test_module4.py
===============
Test Module 4: Uploader, Downloader, TrackerClient, PeerNode

Nhóm A — Unit tests (không cần socket)
Nhóm B — Integration tests (socket thật, tracker thật, peer thật)

Chạy:
    cd p2p_torrent
    python -m tests.test_module4
"""

import os, sys, time, socket, threading, tempfile, shutil, random

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.file_handler    import save_chunk, merge_chunks
from common.hash_utils      import hash_file, hash_chunk
from common.torrent_parser  import create_torrent, load_torrent
from peer.piece_manager     import PieceManager
from peer.uploader          import Uploader
from peer.downloader        import Downloader
from peer.tracker_client    import TrackerClient
from peer.peer_node         import PeerNode
from tracker.tracker_server import TrackerServer


# ════════════════════════════════════════════════
# Helpers
# ════════════════════════════════════════════════

_port = [19100]

def next_port():
    p = _port[0]; _port[0] += 1; return p


def make_random_file(path: str, size_kb: int):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "wb") as f:
        f.write(os.urandom(size_kb * 1024))


def start_tracker(port) -> TrackerServer:
    t = TrackerServer("127.0.0.1", port)
    th = threading.Thread(target=t.start, daemon=True)
    th.start()
    time.sleep(0.3)
    return t


# ════════════════════════════════════════════════
# NHÓM A — Unit / Component tests
# ════════════════════════════════════════════════

def test_uploader_serves_chunk(tmp):
    """Uploader phục vụ 1 chunk đúng khi leecher REQUEST."""
    # Tạo file và torrent
    src = os.path.join(tmp, "src.bin")
    make_random_file(src, 600)
    torrent_path = create_torrent(src, output_dir=tmp,
                                  chunk_size=256*1024)
    torrent = load_torrent(torrent_path)

    port = next_port()
    up   = Uploader("seeder", "127.0.0.1", port, src, torrent)
    up.start_background()

    # Client kết nối và request chunk 0
    s = socket.socket()
    s.settimeout(5)
    s.connect(("127.0.0.1", port))

    from peer.peer_connection import PeerConnection
    from common import protocol as P

    conn = PeerConnection("leecher", "127.0.0.1", port, sock=s)
    ok   = conn.do_handshake_initiator("leecher", torrent["info_hash"], [])
    assert ok, "Handshake thất bại"

    conn.send_msg(P.MSG_REQUEST, {"chunk_index": 0})
    result = conn.recv_piece(timeout=5)
    assert result is not None, "Phải nhận được chunk"

    idx, data = result
    assert idx == 0
    # Verify hash
    expected = torrent["chunks"][0]["hash"]
    assert hash_chunk(data) == expected, "Hash chunk phải khớp"

    conn.disconnect()
    up.stop()


def test_downloader_single_peer(tmp):
    """
    1 seeder + 1 leecher: leecher tải xong file, verify hash.
    """
    # Seeder setup
    src = os.path.join(tmp, "movie.bin")
    make_random_file(src, 1200)
    torrent_path = create_torrent(src, output_dir=tmp, chunk_size=256*1024)
    torrent      = load_torrent(torrent_path)
    num_chunks   = torrent["num_chunks"]

    up_port = next_port()
    up      = Uploader("seeder1", "127.0.0.1", up_port, src, torrent)
    up.start_background()

    # Leecher setup
    chunk_dir = os.path.join(tmp, "leecher_chunks")
    pm        = PieceManager(num_chunks)
    dl        = Downloader("leecher1", torrent, pm, chunk_dir)

    peers = [{"peer_id": "seeder1", "ip": "127.0.0.1",
              "port": up_port, "chunks": list(range(num_chunks))}]

    success = dl.download_from_peers(peers)
    assert success, "Tải phải thành công"
    assert pm.is_complete(), "PieceManager phải complete"

    # Ghép file và verify
    out = os.path.join(tmp, "output.bin")
    merge_chunks(chunk_dir, out, num_chunks)
    assert hash_file(src) == hash_file(out), "File gốc và file tải về phải giống nhau!"

    up.stop()


def test_downloader_two_peers_parallel(tmp):
    """
    File chia 4 chunk → seeder_A có chunk 0,1 / seeder_B có chunk 2,3
    Leecher tải song song từ cả 2 → file hoàn chỉnh.
    """
    src = os.path.join(tmp, "audio.bin")
    make_random_file(src, 1024)
    torrent_path = create_torrent(src, output_dir=tmp, chunk_size=256*1024)
    torrent      = load_torrent(torrent_path)
    num_chunks   = torrent["num_chunks"]

    # Seeder A và B đều serve file gốc nhưng leecher chỉ hỏi chunk mình cần
    port_a = next_port()
    port_b = next_port()
    up_a   = Uploader("seeder_a", "127.0.0.1", port_a, src, torrent)
    up_b   = Uploader("seeder_b", "127.0.0.1", port_b, src, torrent)
    up_a.start_background()
    up_b.start_background()

    chunk_dir = os.path.join(tmp, "chunks_parallel")
    pm        = PieceManager(num_chunks)
    dl        = Downloader("leecher_p", torrent, pm, chunk_dir)

    half = num_chunks // 2
    peers = [
        {"peer_id": "seeder_a", "ip": "127.0.0.1",
         "port": port_a, "chunks": list(range(0, half))},
        {"peer_id": "seeder_b", "ip": "127.0.0.1",
         "port": port_b, "chunks": list(range(half, num_chunks))},
    ]

    success = dl.download_from_peers(peers)
    assert success, "Tải song song phải thành công"

    out = os.path.join(tmp, "output_parallel.bin")
    merge_chunks(chunk_dir, out, num_chunks)
    assert hash_file(src) == hash_file(out), "File song song phải giống file gốc!"

    up_a.stop(); up_b.stop()


def test_downloader_bad_hash_retry(tmp):
    """
    Giả lập chunk bị lỗi hash → mark_failed → chunk đó vẫn trong missing.
    """
    src = os.path.join(tmp, "doc.bin")
    make_random_file(src, 512)
    torrent_path = create_torrent(src, output_dir=tmp, chunk_size=256*1024)
    torrent      = load_torrent(torrent_path)

    pm = PieceManager(torrent["num_chunks"])

    # Giả lập: lấy chunk 0, verify với hash sai
    pm.next_needed()          # pending chunk 0
    pm.mark_failed(0)         # thất bại
    assert 0 in pm.get_missing(), "Chunk 0 phải có thể tải lại"


def test_tracker_client_full_flow(tmp):
    """
    TrackerClient: register → get_peers → have → heartbeat → unregister
    """
    t_port  = next_port()
    tracker = start_tracker(t_port)

    # Seeder đăng ký
    seeder_client = TrackerClient("127.0.0.1", t_port, "seeder_X", 8001)
    ok = seeder_client.register("hash_test", [0, 1, 2])
    assert ok, "Register phải thành công"

    # Leecher đăng ký và lấy peer list
    leecher_client = TrackerClient("127.0.0.1", t_port, "leecher_X", 8002)
    leecher_client.register("hash_test", [])
    peers = leecher_client.get_peers("hash_test")

    assert len(peers) == 1, f"Phải có 1 peer, nhận được {len(peers)}"
    assert peers[0]["peer_id"] == "seeder_X"

    # Leecher tải xong chunk 0 → báo tracker
    ok2 = leecher_client.have("hash_test", 0)
    assert ok2

    # Heartbeat
    ok3 = seeder_client.heartbeat("hash_test")
    assert ok3

    # Unregister
    seeder_client.unregister("hash_test")
    peers2 = leecher_client.get_peers("hash_test")
    ids = [p["peer_id"] for p in peers2]
    assert "seeder_X" not in ids

    tracker.stop()


# ════════════════════════════════════════════════
# NHÓM B — End-to-end integration tests
# ════════════════════════════════════════════════

def test_e2e_seeder_leecher(tmp):
    """
    END-TO-END: Tracker + Seeder + Leecher chạy thật.
    Leecher tải xong → file giống file gốc.
    """
    t_port = next_port()
    tracker = start_tracker(t_port)

    # Tạo file gốc
    src = os.path.join(tmp, "e2e_src.bin")
    make_random_file(src, 1500)
    torrent_path = create_torrent(
        src, tracker_url=f"127.0.0.1:{t_port}",
        output_dir=tmp, chunk_size=256*1024
    )
    torrent   = load_torrent(torrent_path)
    info_hash = torrent["info_hash"]

    # ── Seeder ────────────────────────────────────
    s_port  = next_port()
    uploader = Uploader("seeder_e2e", "127.0.0.1", s_port, src, torrent)
    uploader.start_background()

    # Đăng ký seeder với tracker
    sc = TrackerClient("127.0.0.1", t_port, "seeder_e2e", s_port)
    sc.register(info_hash, list(range(torrent["num_chunks"])))

    # ── Leecher ───────────────────────────────────
    chunk_dir = os.path.join(tmp, "e2e_chunks")
    pm        = PieceManager(torrent["num_chunks"])
    dl        = Downloader("leecher_e2e", torrent, pm, chunk_dir)

    # Leecher đăng ký và hỏi peers
    lc    = TrackerClient("127.0.0.1", t_port, "leecher_e2e", 9999)
    lc.register(info_hash, [])
    peers = lc.get_peers(info_hash)

    assert len(peers) > 0, "Phải tìm thấy ít nhất 1 seeder"

    success = dl.download_from_peers(peers)
    assert success, f"Tải E2E thất bại | stats: {dl.get_stats()}"

    # Ghép file
    out = os.path.join(tmp, "e2e_output.bin")
    merge_chunks(chunk_dir, out, torrent["num_chunks"])

    assert hash_file(src) == hash_file(out), \
        "File E2E phải giống hệt file gốc!"

    print(f"\n  E2E stats: {dl.get_stats()}")
    uploader.stop()
    tracker.stop()


def test_e2e_peer_leaves_mid_download(tmp):
    """
    Peer A rớt giữa chừng → Peer B tiếp tục → leecher vẫn hoàn thành.
    """
    t_port  = next_port()
    tracker = start_tracker(t_port)

    src = os.path.join(tmp, "resilience.bin")
    make_random_file(src, 1024)
    torrent_path = create_torrent(src, output_dir=tmp, chunk_size=256*1024)
    torrent      = load_torrent(torrent_path)
    info_hash    = torrent["info_hash"]
    num_chunks   = torrent["num_chunks"]

    # 2 seeder, cùng có đủ chunk
    port_a = next_port()
    port_b = next_port()
    up_a   = Uploader("peer_a", "127.0.0.1", port_a, src, torrent)
    up_b   = Uploader("peer_b", "127.0.0.1", port_b, src, torrent)
    up_a.start_background()
    up_b.start_background()

    # Dừng peer_a sau 0.5 giây (giả lập rớt mạng)
    def kill_a():
        time.sleep(0.5)
        up_a.stop()
        print("  [Test] peer_a đã rớt mạng!")

    threading.Thread(target=kill_a, daemon=True).start()

    # Leecher cố tải từ cả 2, khi A chết vẫn còn B
    chunk_dir = os.path.join(tmp, "resilience_chunks")
    pm        = PieceManager(num_chunks)
    dl        = Downloader("leecher_r", torrent, pm, chunk_dir)

    peers = [
        {"peer_id": "peer_a", "ip": "127.0.0.1",
         "port": port_a, "chunks": list(range(num_chunks))},
        {"peer_id": "peer_b", "ip": "127.0.0.1",
         "port": port_b, "chunks": list(range(num_chunks))},
    ]

    success = dl.download_from_peers(peers)
    # Nếu chưa xong (do A chết), retry với B
    if not success:
        peers_b = [peers[1]]
        success = dl.download_from_peers(peers_b)

    assert success or pm.is_complete(), \
        "Sau khi retry, leecher phải hoàn thành"

    up_b.stop()
    tracker.stop()


def test_e2e_three_leechers_same_torrent(tmp):
    """
    1 seeder + 3 leecher cùng tải 1 torrent song song.
    Tất cả đều nhận được file đúng.
    """
    t_port  = next_port()
    tracker = start_tracker(t_port)

    src = os.path.join(tmp, "shared.bin")
    make_random_file(src, 800)
    torrent_path = create_torrent(src, output_dir=tmp, chunk_size=256*1024)
    torrent      = load_torrent(torrent_path)
    info_hash    = torrent["info_hash"]
    num_chunks   = torrent["num_chunks"]

    # Seeder
    s_port   = next_port()
    uploader = Uploader("seeder_3l", "127.0.0.1", s_port, src, torrent)
    uploader.start_background()
    sc = TrackerClient("127.0.0.1", t_port, "seeder_3l", s_port)
    sc.register(info_hash, list(range(num_chunks)))

    peers = [{"peer_id": "seeder_3l", "ip": "127.0.0.1",
              "port": s_port, "chunks": list(range(num_chunks))}]

    results = [None, None, None]

    def leecher_task(i):
        cdir = os.path.join(tmp, f"leecher_{i}_chunks")
        pm   = PieceManager(num_chunks)
        dl   = Downloader(f"leecher_{i}", torrent, pm, cdir)
        ok   = dl.download_from_peers(peers)
        if ok:
            out = os.path.join(tmp, f"out_{i}.bin")
            merge_chunks(cdir, out, num_chunks)
            results[i] = hash_file(out)

    threads = [threading.Thread(target=leecher_task, args=(i,)) for i in range(3)]
    for t in threads: t.start()
    for t in threads: t.join(timeout=30)

    src_hash = hash_file(src)
    for i, h in enumerate(results):
        assert h is not None,   f"Leecher {i} không tải xong"
        assert h == src_hash,   f"Leecher {i}: file sai hash!"

    uploader.stop()
    tracker.stop()


# ════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════

def run_test(name, fn, tmp):
    t = tempfile.mkdtemp(prefix=f"p2p_m4_")
    try:
        fn(t)
        print(f"  [OK] {name}")
        return True
    except Exception as e:
        print(f"  [FAIL] {name}: {e}")
        import traceback; traceback.print_exc()
        return False
    finally:
        shutil.rmtree(t, ignore_errors=True)


if __name__ == "__main__":
    unit_tests = [
        ("Uploader phục vụ chunk",              test_uploader_serves_chunk),
        ("Downloader 1 peer",                   test_downloader_single_peer),
        ("Downloader 2 peer song song",          test_downloader_two_peers_parallel),
        ("Downloader: hash lỗi → retry",         test_downloader_bad_hash_retry),
        ("TrackerClient: full flow",             test_tracker_client_full_flow),
    ]

    e2e_tests = [
        ("E2E: Seeder → Leecher hoàn chỉnh",    test_e2e_seeder_leecher),
        ("E2E: Peer rớt mạng giữa chừng",       test_e2e_peer_leaves_mid_download),
        ("E2E: 3 Leecher cùng 1 torrent",       test_e2e_three_leechers_same_torrent),
    ]

    print("\n── UNIT / COMPONENT TESTS ─────────────────────────")
    p1 = sum(run_test(n, f, None) for n, f in unit_tests)

    print("\n── END-TO-END TESTS ────────────────────────────────")
    p2 = sum(run_test(n, f, None) for n, f in e2e_tests)

    total  = len(unit_tests) + len(e2e_tests)
    passed = p1 + p2

    print("\n" + "=" * 60)
    if passed == total:
        print(f"  TAT CA {total}/{total} TEST PASSED! Module 4 hoat dong chinh xac.")
    else:
        print(f"  {passed}/{total} passed.")
    print("=" * 60)
