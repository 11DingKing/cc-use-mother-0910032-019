"""不可变事件存储：SQLite 只追加台账。

并发策略：每次写入使用 ``BEGIN IMMEDIATE`` 立即获取 RESERVED 写锁，
事务内完成"重放最新余额 -> 守恒校验 -> 落库"，多写者/多线程串行化，
从根本上杜绝并发领料导致的超发（负库存）。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path

from .models import Event, EventType, Leg, new_id, now_iso

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id         TEXT NOT NULL UNIQUE,
    event_type       TEXT NOT NULL,
    actor            TEXT NOT NULL,
    occurred_at      TEXT NOT NULL,
    payload          TEXT NOT NULL,
    legs             TEXT NOT NULL,
    idempotency_key  TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_idem
    ON events(idempotency_key) WHERE idempotency_key IS NOT NULL;
"""


class EventStore:
    """事件台账。默认文件持久化；``path=":memory:"`` 用于测试。"""

    def __init__(self, path: str | Path = ":memory:"):
        self.path = str(path)
        # check_same_thread=False：配合自有的写锁跨线程使用
        self._conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        with self._conn:
            self._conn.executescript(SCHEMA)
        # 进程内写锁：保证"读余额-校验-写"在同一线程视角下原子
        self._wlock = threading.RLock()

    def close(self) -> None:
        self._conn.close()

    # ---------------------------------------------------------- 读取

    def load_all(self) -> list[Event]:
        rows = self._conn.execute(
            "SELECT * FROM events ORDER BY seq ASC"
        ).fetchall()
        return [self._row_to_event(row) for row in rows]

    def get(self, event_id: str) -> Event | None:
        row = self._conn.execute(
            "SELECT * FROM events WHERE event_id=?", (event_id,)
        ).fetchone()
        return self._row_to_event(row) if row else None

    def get_by_seq(self, seq: int) -> Event | None:
        row = self._conn.execute(
            "SELECT * FROM events WHERE seq=?", (seq,)
        ).fetchone()
        return self._row_to_event(row) if row else None

    def find_idempotent(self, key: str) -> Event | None:
        row = self._conn.execute(
            "SELECT * FROM events WHERE idempotency_key=?", (key,)
        ).fetchone()
        return self._row_to_event(row) if row else None

    def head_seq(self) -> int:
        value = self._conn.execute("SELECT MAX(seq) AS m FROM events").fetchone()["m"]
        return int(value or 0)

    # ---------------------------------------------------------- 写入

    def append_with_check(self, build, *, idempotency_key: str | None = None) -> Event:
        """原子化"读取-校验-追加"。

        ``build(events)`` 在 IMMEDIATE 事务内被调用，接收当前已提交事件，
        返回 ``(EventType, actor, payload, legs[, occurred_at])``。
        legs 为 Leg 可迭代对象。
        """
        with self._wlock:
            tx = self._conn.execute("BEGIN IMMEDIATE")
            try:
                if idempotency_key is not None:
                    existing = self._conn.execute(
                        "SELECT * FROM events WHERE idempotency_key=?",
                        (idempotency_key,),
                    ).fetchone()
                    if existing:
                        # 幂等命中：原事件已提交，直接返回，不重复落库
                        self._conn.execute("COMMIT")
                        return self._row_to_event(existing)

                rows = self._conn.execute("SELECT * FROM events ORDER BY seq ASC").fetchall()
                events = [self._row_to_event(row) for row in rows]

                result = build(events)
                event_type, actor, payload, raw_legs = result[:4]
                occurred_at = result[4] if len(result) > 4 and result[4] else now_iso()
                legs = tuple(raw_legs)

                event_id = new_id()
                self._conn.execute(
                    "INSERT INTO events(event_id,event_type,actor,occurred_at,payload,legs,idempotency_key)"
                    " VALUES(?,?,?,?,?,?,?)",
                    (
                        event_id,
                        event_type.value,
                        actor,
                        occurred_at,
                        json.dumps(payload, ensure_ascii=False, sort_keys=True),
                        json.dumps([leg.to_json() for leg in legs], ensure_ascii=False),
                        idempotency_key,
                    ),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

        return self.get(event_id)  # type: ignore[return-value]

    # ---------------------------------------------------------- 序列化

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> Event:
        seq = row["seq"]
        return Event(
            seq=seq,
            event_id=row["event_id"],
            event_type=EventType(row["event_type"]),
            actor=row["actor"],
            occurred_at=row["occurred_at"],
            payload=json.loads(row["payload"]),
            legs=tuple(Leg.from_json(x) for x in json.loads(row["legs"])),
            idempotency_key=row["idempotency_key"],
        )
