"""``python -m inventory_trace [--db inventory.db] [--host 127.0.0.1] [--port 8080]``。"""
from __future__ import annotations

import argparse

from .api import serve


def main() -> None:
    parser = argparse.ArgumentParser(description="库存批次全链追溯服务")
    parser.add_argument("--db", default="inventory.db")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    serve(args.db, args.host, args.port)


if __name__ == "__main__":
    main()
