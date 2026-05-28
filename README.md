# P2P BitTorrent — Đồ án Hệ thống Phân tán
## Cấu trúc project
```
p2p_torrent/
├── common/            # Thư viện dùng chung
│   ├── hash_utils.py      SHA256 hash & verify
│   ├── file_handler.py    Chia/ghép file, đọc chunk
│   ├── torrent_parser.py  Tạo/đọc file .torrent
│   └── protocol.py        Định nghĩa giao thức
├── tracker/           # Tracker server
│   ├── tracker_server.py  TCP server đa luồng
│   ├── tracker_api.py     Xử lý request
│   └── peer_manager.py    Quản lý peer trong RAM
├── peer/              # Peer node
│   ├── peer_node.py       Điều phối chính
│   ├── uploader.py        Upload chunk
│   ├── downloader.py      Download chunk song song
│   ├── piece_manager.py   Theo dõi trạng thái chunk
│   ├── peer_connection.py Kết nối TCP peer-to-peer
│   └── tracker_client.py  Giao tiếp với tracker
├── web/               # Web dashboard
│   └── dashboard.py       Flask web UI
├── tests/             # Test suite
│   ├── test_module1.py    ( 9 tests)
│   ├── test_module2.py    (18 tests)
│   ├── test_module3.py    (18 tests)
│   ├── test_module4.py    ( 8 tests)
│   └── test_module5.py    ( 5 tests)
├── main_tracker.py    # Chạy tracker
├── main_peer.py       # Chạy peer
├── create_torrent.py  # Tạo file .torrent
└── simulate_peers.py  # Mô phỏng nhiều peer
```

## Cài đặt
```bash
pip install flask
```

## Cách chạy

### 1. Tạo file .torrent
```bash
python create_torrent.py video.mp4 --tracker 127.0.0.1:6969
```

### 2. Chạy Tracker
```bash
python main_tracker.py --port 6969
```

### 3. Chạy Seeder (Terminal 2)
```bash
python main_peer.py seed --id peer1 --port 7001 --file video.mp4 --torrent video.mp4.torrent
```

### 4. Chạy Leecher (Terminal 3)
```bash
python main_peer.py download --id peer2 --port 7002 --torrent video.mp4.torrent
```

### 5. Web Dashboard
```bash
python web/dashboard.py
# Mở http://localhost:5000
```

### 6. Chạy mô phỏng tự động
```bash
python simulate_peers.py
```

### 7. chạy mô phỏng xử lý song song (scenario 1) và xử lý peer ngắt giữa chừng (scenario 2)
```bash
python demo_advanced.py --scenario 1
python demo_advanced.py --scenario 2
```

## Tính năng
- Chia file thành chunks + SHA256 hash
- Tracker server TCP đa luồng
- Tải song song từ nhiều peer
- Verify hash từng chunk
- Xử lý peer rớt mạng
- Rarest-first algorithm
- Heartbeat tự động
- Leecher → Seeder sau khi tải xong
- Web dashboard real-time
