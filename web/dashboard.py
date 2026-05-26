"""
dashboard.py
============
Web dashboard để monitor hệ thống P2P real-time.

Endpoint:
  GET /              → trang dashboard chính
  GET /api/stats     → JSON thống kê (tracker + peers)
  POST /api/simulate → chạy mô phỏng và stream kết quả
  GET /api/health    → health check

Dùng:
    python web/dashboard.py
    → Mở http://localhost:5000
"""

import sys, os, json, threading, time, queue
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask, render_template_string, jsonify, Response, request
from tracker.tracker_server import TrackerServer
from tracker.peer_manager   import PeerManager

app = Flask(__name__)

# Tracker chạy trong background
_tracker    = None
_tracker_pm = None
_sim_log_q  = queue.Queue()

TRACKER_PORT = 6969

# ── HTML Dashboard ─────────────────────────────────────────────
HTML = """
<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>P2P BitTorrent Dashboard</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Segoe UI', sans-serif; background: #0f1117; color: #e2e8f0; }

  .header {
    background: linear-gradient(135deg, #1e3a5f, #0f2744);
    padding: 20px 32px; display: flex; align-items: center; gap: 12px;
    border-bottom: 1px solid #2d3748;
  }
  .header h1 { font-size: 1.4rem; font-weight: 700; }
  .header .badge {
    background: #22c55e; color: #000; font-size: .7rem;
    padding: 2px 8px; border-radius: 999px; font-weight: 700;
  }

  .grid { display: grid; grid-template-columns: repeat(auto-fit,minmax(280px,1fr));
          gap: 16px; padding: 24px; }

  .card {
    background: #1a1f2e; border: 1px solid #2d3748;
    border-radius: 12px; padding: 20px;
  }
  .card h2 { font-size: .75rem; color: #94a3b8; text-transform: uppercase;
             letter-spacing: .08em; margin-bottom: 12px; }
  .stat-val { font-size: 2rem; font-weight: 700; color: #60a5fa; }
  .stat-label { font-size: .8rem; color: #64748b; margin-top: 2px; }

  .tracker-status { display: flex; align-items: center; gap: 8px; }
  .dot { width: 10px; height: 10px; border-radius: 50%; background: #22c55e;
         animation: pulse 2s infinite; }
  @keyframes pulse {
    0%,100% { opacity: 1; } 50% { opacity: .4; }
  }

  .peer-table { width: 100%; border-collapse: collapse; font-size: .85rem; }
  .peer-table th { color: #64748b; font-weight: 500; text-align: left;
                   padding: 6px 8px; border-bottom: 1px solid #2d3748; }
  .peer-table td { padding: 8px; border-bottom: 1px solid #1e2533; }
  .peer-table tr:last-child td { border: none; }

  .progress-wrap { background: #2d3748; border-radius: 999px;
                   height: 8px; overflow: hidden; margin-top: 4px; }
  .progress-bar  { height: 100%; border-radius: 999px;
                   background: linear-gradient(90deg,#3b82f6,#22c55e);
                   transition: width .5s ease; }

  .log-box {
    background: #0d1117; border: 1px solid #2d3748; border-radius: 8px;
    height: 200px; overflow-y: auto; padding: 12px; font-family: monospace;
    font-size: .78rem; color: #7dd3fc;
  }
  .log-box .entry { margin-bottom: 4px; }
  .log-box .ok    { color: #22c55e; }
  .log-box .err   { color: #f87171; }
  .log-box .info  { color: #94a3b8; }

  .btn {
    background: #3b82f6; color: #fff; border: none; border-radius: 8px;
    padding: 10px 20px; font-size: .9rem; cursor: pointer; font-weight: 600;
    transition: background .2s;
  }
  .btn:hover { background: #2563eb; }
  .btn:disabled { background: #374151; cursor: not-allowed; }
  .btn-red { background: #ef4444; }
  .btn-red:hover { background: #dc2626; }

  .full-width { grid-column: 1 / -1; }
  .actions { display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 12px; }

  .tag { display: inline-block; font-size: .7rem; padding: 2px 8px;
         border-radius: 4px; font-weight: 600; }
  .tag-seeder  { background: #1d4ed8; color: #bfdbfe; }
  .tag-leecher { background: #065f46; color: #a7f3d0; }
</style>
</head>
<body>
<div class="header">
  <div class="dot"></div>
  <h1>🌐 P2P BitTorrent Dashboard</h1>
  <span class="badge">LIVE</span>
</div>

<div class="grid">
  <!-- Tracker Status -->
  <div class="card">
    <h2>Tracker</h2>
    <div class="tracker-status">
      <div class="dot" id="tracker-dot"></div>
      <span id="tracker-status-text">Đang kết nối...</span>
    </div>
    <div style="margin-top:12px">
      <div class="stat-val" id="total-torrents">0</div>
      <div class="stat-label">Torrents đang active</div>
    </div>
  </div>

  <!-- Peer Count -->
  <div class="card">
    <h2>Peers Online</h2>
    <div class="stat-val" id="total-peers">0</div>
    <div class="stat-label">Peers đang kết nối</div>
  </div>

  <!-- Total Transfer -->
  <div class="card">
    <h2>Trạng thái</h2>
    <div class="stat-val" id="sim-status">Sẵn sàng</div>
    <div class="stat-label" id="sim-detail">Chờ chạy mô phỏng</div>
  </div>

  <!-- Peers Table -->
  <div class="card full-width">
    <h2>Danh sách Peers</h2>
    <table class="peer-table">
      <thead>
        <tr>
          <th>Peer ID</th>
          <th>Địa chỉ</th>
          <th>Vai trò</th>
          <th>Chunks</th>
          <th>Tiến độ</th>
        </tr>
      </thead>
      <tbody id="peer-tbody">
        <tr><td colspan="5" style="color:#64748b;text-align:center">Chưa có peer nào</td></tr>
      </tbody>
    </table>
  </div>

  <!-- Simulation Controls + Log -->
  <div class="card full-width">
    <h2>Mô phỏng & Log</h2>
    <div class="actions">
      <button class="btn" id="btn-sim" onclick="runSimulation()">▶ Chạy mô phỏng</button>
      <button class="btn" style="background:#374151" onclick="clearLog()">🗑 Xóa log</button>
    </div>
    <div class="log-box" id="log-box">
      <div class="entry info">Dashboard sẵn sàng. Nhấn "Chạy mô phỏng" để bắt đầu.</div>
    </div>
  </div>
</div>

<script>
let simRunning = false;

// Cập nhật stats mỗi 2 giây
async function fetchStats() {
  try {
    const r = await fetch('/api/stats');
    const d = await r.json();
    updateUI(d);
  } catch(e) {}
}

function updateUI(d) {
  document.getElementById('tracker-status-text').textContent =
    d.tracker_online ? `Online — port ${d.tracker_port}` : 'Offline';
  document.getElementById('total-torrents').textContent = d.total_torrents;
  document.getElementById('total-peers').textContent    = d.total_peers;

  // Peer table
  const tbody = document.getElementById('peer-tbody');
  if (!d.peers || d.peers.length === 0) {
    tbody.innerHTML = '<tr><td colspan="5" style="color:#64748b;text-align:center">Chưa có peer nào</td></tr>';
    return;
  }

  tbody.innerHTML = d.peers.map(p => {
    const role = p.chunk_count >= d.total_chunks
      ? '<span class="tag tag-seeder">Seeder</span>'
      : '<span class="tag tag-leecher">Leecher</span>';
    const pct  = d.total_chunks > 0
      ? Math.round(p.chunk_count / d.total_chunks * 100) : 0;
    return `<tr>
      <td>${p.peer_id}</td>
      <td>${p.ip}:${p.port}</td>
      <td>${role}</td>
      <td>${p.chunk_count}/${d.total_chunks}</td>
      <td>
        <div style="font-size:.75rem;color:#94a3b8">${pct}%</div>
        <div class="progress-wrap"><div class="progress-bar" style="width:${pct}%"></div></div>
      </td>
    </tr>`;
  }).join('');
}

async function runSimulation() {
  if (simRunning) return;
  simRunning = true;
  document.getElementById('btn-sim').disabled = true;
  document.getElementById('sim-status').textContent = 'Đang chạy...';
  clearLog();
  addLog('Bắt đầu mô phỏng...', 'info');

  const r = await fetch('/api/simulate', { method: 'POST' });
  const reader = r.body.getReader();
  const dec    = new TextDecoder();

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    const lines = dec.decode(value).split('\\n').filter(Boolean);
    for (const line of lines) {
      if (line.startsWith('data: ')) {
        const msg = line.slice(6);
        const cls = msg.includes('✓') ? 'ok'
                  : msg.includes('✗') ? 'err' : 'info';
        addLog(msg, cls);
      }
    }
  }

  simRunning = false;
  document.getElementById('btn-sim').disabled = false;
  document.getElementById('sim-status').textContent = 'Hoàn thành';
  addLog('Mô phỏng kết thúc.', 'ok');
}

function addLog(msg, cls='info') {
  const box  = document.getElementById('log-box');
  const div  = document.createElement('div');
  div.className = `entry ${cls}`;
  div.textContent = `[${new Date().toLocaleTimeString()}] ${msg}`;
  box.appendChild(div);
  box.scrollTop = box.scrollHeight;
}

function clearLog() {
  document.getElementById('log-box').innerHTML = '';
}

fetchStats();
setInterval(fetchStats, 2000);
</script>
</body>
</html>
"""

# ── Flask routes ───────────────────────────────────────────────

@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/api/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/api/stats")
def stats():
    """Trả về thống kê tracker real-time."""
    if _tracker_pm is None:
        return jsonify({
            "tracker_online": False, "tracker_port": TRACKER_PORT,
            "total_torrents": 0, "total_peers": 0,
            "total_chunks": 0, "peers": []
        })

    raw       = _tracker_pm.get_stats()
    all_peers = []
    total_chunks = 0

    for torrent_key, info in raw.items():
        for p in info.get("peers", []):
            all_peers.append({
                "peer_id":     p["id"],
                "ip":          p["ip"],
                "port":        p["port"],
                "chunk_count": p["chunks"],
                "torrent":     torrent_key
            })
        # Đoán total_chunks từ peer có nhiều nhất
        if info["peers"]:
            mx = max(p["chunks"] for p in info["peers"])
            total_chunks = max(total_chunks, mx)

    return jsonify({
        "tracker_online":  True,
        "tracker_port":    TRACKER_PORT,
        "total_torrents":  len(raw),
        "total_peers":     len(all_peers),
        "total_chunks":    total_chunks,
        "peers":           all_peers
    })


@app.route("/api/simulate", methods=["POST"])
def simulate():
    """Chạy mô phỏng và stream log về dashboard."""
    def generate():
        import tempfile, shutil
        from common.torrent_parser import create_torrent, load_torrent
        from common.file_handler   import merge_chunks
        from common.hash_utils     import hash_file
        from peer.piece_manager    import PieceManager
        from peer.uploader         import Uploader
        from peer.downloader       import Downloader
        from peer.tracker_client   import TrackerClient

        def send(msg):
            yield f"data: {msg}\n\n"

        tmp = tempfile.mkdtemp(prefix="p2p_sim_")
        try:
            yield from send("🚀 Khởi động mô phỏng...")

            # Tạo file test
            src = os.path.join(tmp, "demo.bin")
            with open(src, "wb") as f:
                f.write(os.urandom(1024 * 1024))  # 1MB
            yield from send(f"📄 Tạo file test: 1MB")

            # Torrent
            torrent_path = create_torrent(
                src, tracker_url=f"127.0.0.1:{TRACKER_PORT}",
                chunk_size=256*1024, output_dir=tmp
            )
            torrent = load_torrent(torrent_path)
            num_c   = torrent["num_chunks"]
            yield from send(f"📋 Tạo .torrent: {num_c} chunks x 256KB")

            # Seeder
            s_port   = 8100
            uploader = Uploader("seeder", "127.0.0.1", s_port, src, torrent)
            uploader.start_background()
            sc = TrackerClient("127.0.0.1", TRACKER_PORT, "seeder", s_port)
            sc.register(torrent["info_hash"], list(range(num_c)))
            yield from send("🌱 Seeder online và đăng ký với tracker")
            time.sleep(0.5)

            # 3 leecher
            results = []
            lock    = threading.Lock()

            def run_leecher(i):
                cdir = os.path.join(tmp, f"l{i}_chunks")
                pm   = PieceManager(num_c)
                dl   = Downloader(f"leecher_{i}", torrent, pm, cdir)
                lc   = TrackerClient(
                    "127.0.0.1", TRACKER_PORT, f"leecher_{i}", 8200+i
                )
                lc.register(torrent["info_hash"], [])
                peers = lc.get_peers(torrent["info_hash"])
                ok    = dl.download_from_peers(peers) if peers else False
                out   = os.path.join(tmp, f"out_{i}.bin")
                if ok:
                    merge_chunks(cdir, out, num_c)
                    file_ok = os.path.exists(out) and hash_file(out) == hash_file(src)
                with lock:
                    results.append((i, ok, file_ok if ok else False,
                                    dl.get_stats()))

            yield from send("⬇  3 Leecher bắt đầu tải song song...")
            threads = [threading.Thread(target=run_leecher, args=(i,))
                       for i in range(3)]
            for t in threads: t.start()
            for t in threads: t.join(timeout=30)

            yield from send("─" * 40)
            all_ok = True
            for i, ok, file_ok, stats in sorted(results):
                icon = "✓" if (ok and file_ok) else "✗"
                yield from send(
                    f"{icon} leecher_{i}: "
                    f"{stats['avg_speed_kbps']:.0f} KB/s | "
                    f"file {'đúng ✓' if file_ok else 'sai ✗'}"
                )
                if not file_ok: all_ok = False

            uploader.stop()
            yield from send("─" * 40)
            yield from send(
                "✓ Mô phỏng hoàn thành — tất cả file đúng!" if all_ok
                else "✗ Có lỗi trong mô phỏng."
            )

        except Exception as e:
            yield f"data: ✗ Lỗi: {e}\n\n"
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache",
                             "X-Accel-Buffering": "no"})


def start_background_tracker():
    """Khởi động tracker trong background khi dashboard start."""
    global _tracker, _tracker_pm
    _tracker    = TrackerServer("127.0.0.1", TRACKER_PORT)
    _tracker_pm = _tracker.peer_manager
    t = threading.Thread(target=_tracker.start, daemon=True)
    t.start()
    time.sleep(0.3)
    print(f"[Dashboard] Tracker started on port {TRACKER_PORT}")


if __name__ == "__main__":
    print("=" * 55)
    print("  P2P BitTorrent Dashboard")
    print("  Mở trình duyệt: http://localhost:5000")
    print("=" * 55)
    start_background_tracker()
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
