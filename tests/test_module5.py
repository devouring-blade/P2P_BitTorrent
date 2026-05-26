"""
test_module5.py - Test Module 5: Integration tổng thể + Mô phỏng
"""

import os, sys, time, threading, tempfile, shutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.torrent_parser  import create_torrent, load_torrent
from common.file_handler    import merge_chunks
from common.hash_utils      import hash_file
from peer.piece_manager     import PieceManager
from peer.uploader          import Uploader
from peer.downloader        import Downloader
from peer.tracker_client    import TrackerClient
from tracker.tracker_server import TrackerServer

_port = [20200]
def next_port():
    p = _port[0]; _port[0] += 1; return p

def make_file(path, kb):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "wb") as f: f.write(os.urandom(kb * 1024))

def start_tracker(port):
    t = TrackerServer("127.0.0.1", port)
    threading.Thread(target=t.start, daemon=True).start()
    time.sleep(0.3)
    return t


def test_rarest_first_algorithm(tmp):
    """Rarest-first chọn đúng chunk hiếm nhất."""
    pm = PieceManager(6)
    # peer_A có 0,1,2,3 / peer_B có 0,1 / peer_C có 0
    # Rarest: chunk 3 (1 peer) hoặc 2 (1 peer) trước chunk 0 (3 peer)
    peers = [[0,1,2,3], [0,1], [0]]
    idx = pm.next_needed_rarest_first(peers)
    assert idx in [2, 3], f"Rarest phải là chunk 2 hoặc 3, nhận {idx}"


def test_full_pipeline_large_file(tmp):
    """Pipeline đầy đủ với file 3MB, 12 chunks."""
    src = os.path.join(tmp, "large.bin")
    make_file(src, 3072)
    t_port  = next_port()
    tracker = start_tracker(t_port)
    torrent_path = create_torrent(src, tracker_url=f"127.0.0.1:{t_port}",
                                  chunk_size=256*1024, output_dir=tmp)
    torrent  = load_torrent(torrent_path)
    num_c    = torrent["num_chunks"]
    assert num_c == 12, f"3MB / 256KB = 12 chunks, nhận {num_c}"

    s_port   = next_port()
    uploader = Uploader("seeder_L", "127.0.0.1", s_port, src, torrent)
    uploader.start_background()
    sc = TrackerClient("127.0.0.1", t_port, "seeder_L", s_port)
    sc.register(torrent["info_hash"], list(range(num_c)))

    cdir = os.path.join(tmp, "chunks")
    pm   = PieceManager(num_c)
    dl   = Downloader("leecher_L", torrent, pm, cdir)

    lc    = TrackerClient("127.0.0.1", t_port, "leecher_L", 9000)
    lc.register(torrent["info_hash"], [])
    peers = lc.get_peers(torrent["info_hash"])

    ok  = dl.download_from_peers(peers, max_connections=3)
    assert ok, "Tải file 3MB phải thành công"

    out = os.path.join(tmp, "output_large.bin")
    merge_chunks(cdir, out, num_c)
    assert hash_file(src) == hash_file(out), "File 3MB phải giống file gốc"

    stats = dl.get_stats()
    assert stats["total_downloaded_mb"] >= 2.9
    assert stats["failed_chunks"] == 0

    uploader.stop(); tracker.stop()


def test_leecher_becomes_seeder(tmp):
    """Leecher tải xong → đăng ký lại tracker với đủ chunk → tracker thấy đủ chunk."""
    src     = os.path.join(tmp, "chain.bin")
    make_file(src, 512)
    t_port  = next_port()
    tracker = start_tracker(t_port)
    torrent_path = create_torrent(src, output_dir=tmp, chunk_size=256*1024)
    torrent  = load_torrent(torrent_path)
    num_c    = torrent["num_chunks"]

    # Seeder ban đầu
    s_port = next_port()
    up1    = Uploader("seeder_chain", "127.0.0.1", s_port, src, torrent)
    up1.start_background()
    sc1 = TrackerClient("127.0.0.1", t_port, "seeder_chain", s_port)
    sc1.register(torrent["info_hash"], list(range(num_c)))

    # Leecher 1 tải từ seeder gốc
    c1_dir = os.path.join(tmp, "l1_chunks")
    pm1    = PieceManager(num_c)
    dl1    = Downloader("leecher_1", torrent, pm1, c1_dir)
    lc1    = TrackerClient("127.0.0.1", t_port, "leecher_1", next_port())
    lc1.register(torrent["info_hash"], [])
    peers1 = lc1.get_peers(torrent["info_hash"])
    dl1.download_from_peers(peers1)
    assert pm1.is_complete(), "Leecher 1 phải tải xong"
    assert pm1.num_have() == num_c, f"Phải có đủ {num_c} chunk"

    # Leecher_1 báo tracker đã có đủ chunk (chuyển thành seeder)
    lc1.register(torrent["info_hash"], list(range(num_c)))

    # Observer kiểm tra tracker thấy leecher_1 có đủ chunk
    obs = TrackerClient("127.0.0.1", t_port, "observer", next_port())
    obs.register(torrent["info_hash"], [])
    all_peers = obs.get_peers(torrent["info_hash"])
    l1_peer   = next((p for p in all_peers if p["peer_id"] == "leecher_1"), None)

    assert l1_peer is not None, "Tracker phải thấy leecher_1"
    assert len(l1_peer["chunks"]) == num_c,         f"Tracker phải thấy leecher_1 có {num_c} chunks"

    # Leecher_1 giờ là seeder — tải tiếp từ nó
    l1_port = next_port()
    up2     = Uploader("leecher_1", "127.0.0.1", l1_port, src, torrent)
    up2.start_background()

    c2_dir = os.path.join(tmp, "l2_chunks")
    pm2    = PieceManager(num_c)
    dl2    = Downloader("leecher_2", torrent, pm2, c2_dir)
    ok     = dl2.download_from_peers([{
        "peer_id": "leecher_1", "ip": "127.0.0.1",
        "port": l1_port, "chunks": list(range(num_c))
    }])
    assert ok, "Leecher 2 phải tải được từ leecher_1"
    out = os.path.join(tmp, "chain_out.bin")
    merge_chunks(c2_dir, out, num_c)
    assert hash_file(src) == hash_file(out)

    up1.stop(); up2.stop(); tracker.stop()


def test_heartbeat_keeps_peer_alive(tmp):
    """Peer gửi heartbeat → không bị tracker xóa sau timeout."""
    t_port  = next_port()
    tracker = start_tracker(t_port)

    src = os.path.join(tmp, "hb.bin")
    make_file(src, 256)
    torrent_path = create_torrent(src, output_dir=tmp)
    torrent  = load_torrent(torrent_path)

    tc = TrackerClient("127.0.0.1", t_port, "hb_peer", next_port())
    tc.register(torrent["info_hash"], [0, 1])
    tc._hb_interval = 2   # heartbeat mỗi 2 giây (test nhanh)
    tc.start_heartbeat(torrent["info_hash"])

    time.sleep(5)  # chờ qua PEER_TIMEOUT sẽ bị xóa nếu không có heartbeat

    peers = tc.get_peers(torrent["info_hash"])
    # Peer phải vẫn còn (heartbeat giữ alive)
    # Note: get_peers exclude chính nó, nên kiểm tra qua peer khác
    tc2 = TrackerClient("127.0.0.1", t_port, "observer", next_port())
    tc2.register(torrent["info_hash"], [])
    peers2 = tc2.get_peers(torrent["info_hash"])
    ids = [p["peer_id"] for p in peers2]
    assert "hb_peer" in ids, "Peer gửi heartbeat không được bị xóa"

    tc.stop_heartbeat()
    tracker.stop()


def test_simulation_script(tmp):
    """Chạy simulation script đầy đủ, kiểm tra kết quả."""
    # Import và chạy trực tiếp logic simulation (không qua subprocess)
    from simulate_peers import run_simulation
    # Thay đổi port để không xung đột
    import simulate_peers as sim
    orig_port = sim.TRACKER_PORT
    orig_base = sim.BASE_PORT
    sim.TRACKER_PORT  = next_port()
    sim.BASE_PORT     = next_port()
    sim.NUM_LEECHERS  = 2       # giảm để test nhanh
    sim.FILE_SIZE_KB  = 1024    # 1MB

    try:
        result = run_simulation()
        assert result, "Simulation phải thành công"
    finally:
        sim.TRACKER_PORT = orig_port
        sim.BASE_PORT    = orig_base


def run_test(name, fn):
    tmp = tempfile.mkdtemp(prefix="p2p_m5_")
    try:
        fn(tmp)
        print(f"  [OK] {name}")
        return True
    except Exception as e:
        print(f"  [FAIL] {name}: {e}")
        import traceback; traceback.print_exc()
        return False
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    tests = [
        ("Rarest-first algorithm",           test_rarest_first_algorithm),
        ("Pipeline đầy đủ file 3MB",         test_full_pipeline_large_file),
        ("Leecher → Seeder chain",           test_leecher_becomes_seeder),
        ("Heartbeat giữ peer alive",         test_heartbeat_keeps_peer_alive),
        ("Simulation script đầy đủ",         test_simulation_script),
    ]

    print("\n── MODULE 5 TESTS ──────────────────────────────────")
    passed = sum(run_test(n, f) for n, f in tests)
    total  = len(tests)

    print("\n" + "=" * 60)
    if passed == total:
        print(f"  TAT CA {total}/{total} TEST PASSED! Module 5 hoat dong chinh xac.")
    else:
        print(f"  {passed}/{total} passed.")
    print("=" * 60)
