"""
main_tracker.py
===============
Chạy Tracker Server.

Dùng:
    python main_tracker.py [--host 0.0.0.0] [--port 6969]
"""

import argparse, sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from tracker.tracker_server import TrackerServer

def main():
    parser = argparse.ArgumentParser(description="P2P Tracker Server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=6969)
    args = parser.parse_args()

    print("=" * 50)
    print("   P2P BitTorrent Tracker")
    print("=" * 50)

    tracker = TrackerServer(args.host, args.port)
    try:
        tracker.start()
    except KeyboardInterrupt:
        print("\nĐang dừng tracker...")
        tracker.stop()

if __name__ == "__main__":
    main()
