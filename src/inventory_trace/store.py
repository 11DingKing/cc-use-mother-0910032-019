"""事件存储：SQLite 仅追加日志 + 派生投影。

设计要点：

* ``events`` / ``event_lines`` 是唯一事实来源，触发器禁止 UPDATE/DELETE；
* 每条事件包含前一条事件的 SHA-256，形成哈希链，任何外部篡改都会在
  一致性检查中暴露；
* ``batch_projection`` 只是可重建的读模型，一致性检查时从空状态重放
  全量事件并逐行比对。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from pathlib import Path
from typing import Any, Iterable

from .errors import CorruptedLedgerError
from .models import D, BatchState, Event, Line

GENESIS_HASH = "0" * 64

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq           INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id      TEXT NOT NULL UNIQUE,
    event_type    TEXT NOT NULL,
    occurred_at   TEXT NOT NULL,
    actor         TEXT NOT NULL,
    payload       TEXT NOT NULL DEFAULT '{}',
    supersedes    INTEGER REFERENCES events(seq),
    correction_of INTEGER REFERENCES events(seq),
    prev_hash     TEXT NOT NULL,
    event_hash    TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS event_lines (
    seq          INTEGER NOT NULL REFERENCES events(seq),
    side         TEXT NOT NULL CHECK (side IN ('in', 'out')),
    idx          INTEGER NOT NULL,
    batch_id     TEXT NOT NULL,
    quantity     TEXT NOT NULL,
    location     TEXT,
    external_ref TEXT,
    PRIMARY KEY (seq, side, idx)
);

CREATE TABLE IF NOT EXISTS batch_projection (
    batch_id     TEXT PRIMARY KEY,
    sku          TEXT NOT NULL,
    supplier_lot TEXT,
    quantity     TEXT NOT NULL,
    location     TEXT NOT NULL,
    consumed     INTEGER NOT NULL DEFAULT 0,
    created_seq  INTEGER NOT NULL,
    last_seq     INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS events_no_update BEFORE UPDATE ON events
BEGIN
    SELECT RAISE(ABORT, 'events 表不可变：禁止 UPDATE');
END;

CREATE TRIGGER IF NOT EXISTS events_no_delete BEFORE DELETE ON events
BEGIN
    SELECT RAISE(ABORT, 'events 表不可变：禁止 DELETE');
END;

CREATE TRIGGER IF NOT EXISTS lines_no_update BEFORE UPDATE ON event_lines
BEGIN
    SELECT RAISE(ABORT, 'event_lines 表不可变：禁止 UPDATE');
END;

CREATE TRIGGER IF NOT EXISTS lines_no_delete BEFORE DELETE ON event_lines
BEGIN
    SELECT RAISE(ABORT, 'event_lines 表不可变：禁止 DELETE');
END;
"""


def connect(path: str | Path, *, busy_timeout_ms: int = 10_000) -> sqlite3.Connection:
    """打开一个配置好的数据库连接（外键、WAL、行工厂）。

    ``:memory:`` 使用带唯一名字的共享缓存内存库，使同一 EventStore 的
    多个连接看到同一份数据（需要 EventStore 持有守护连接保活）。
    """
    if str(path) == ":memory:":
        raise ValueError("请使用 EventStore 的 memory_uri，而非直接连接 :memory:")
    conn = sqlite3.connect(str(path), timeout=busy_timeout_ms / 1000, isolation_level=None)
    _configure(conn, busy_timeout_ms)
    return conn


def connect_memory(uri: str, *, busy_timeout_ms: int = 10_000) -> sqlite3.Connection:
    conn = sqlite3.connect(uri, timeout=busy_timeout_ms / 1000,
                           isolation_level=None, uri=True)
    _configure(conn, busy_timeout_ms)
    return conn


def _configure(conn: sqlite3.Connection, busy_timeout_ms: int) -> None:
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={busy_timeout_ms}")
    conn.execute("PRAGMA foreign_keys=ON")
    # 共享缓存内存库不支持 WAL，忽略该报错
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except sqlite3.DatabaseError:
        pass
    conn.execute("PRAGMA synchronous=NORMAL")


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


# --------------------------------------------------------------------------- #
# 哈希链
# --------------------------------------------------------------------------- #

def _canonical_event(
    *,
    event_id: str,
    event_type: str,
    occurred_at: str,
    actor: str,
    payload: dict[str, Any],
    supersedes: int | None,
    correction_of: int | None,
    inputs: Iterable[Line],
    outputs: Iterable[Line],
) -> str:
    body = {
        "event_id": event_id,
        "event_type": event_type,
        "occurred_at": occurred_at,
        "actor": actor,
        "payload": payload,
        "supersedes": supersedes,
        "correction_of": correction_of,
        "inputs": [line.to_row() for line in inputs],
        "outputs": [line.to_row() for line in outputs],
    }
    return json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def compute_hash(prev_hash: str, canonical: str) -> str:
    return hashlib.sha256((prev_hash + ":" + canonical).encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
# 投影器（写入时增量更新 / 重放时从零重建，共用同一套规则）
# --------------------------------------------------------------------------- #

EXTERNAL = "*"


class Projection:
    """批次当前状态的内存投影，键为 batch_id。"""

    def __init__(self) -> None:
        self.batches: dict[str, BatchState] = {}

    def apply(self, event: Event) -> None:
        meta = event.payload.get("batch_meta", {}) if event.payload else {}
        patches = event.payload.get("meta_patch", {}) if event.payload else {}
        for line in event.inputs:
            if line.batch_id == EXTERNAL:
                continue
            state = self.batches.get(line.batch_id)
            if state is None:
                raise CorruptedLedgerError(
                    f"事件 #{event.seq} 扣减了从未入库的批次 {line.batch_id}"
                )
            new_qty = state.quantity - line.quantity
            if new_qty < 0:
                raise CorruptedLedgerError(
                    f"事件 #{event.seq} 导致批次 {line.batch_id} 负库存："
                    f"{state.quantity} - {line.quantity}"
                )
            self.batches[line.batch_id] = BatchState(
                batch_id=state.batch_id,
                sku=state.sku,
                supplier_lot=state.supplier_lot,
                quantity=new_qty,
                location=state.location,
                consumed=1 if (event.event_type == "consume" and new_qty == 0) else 0,
                created_seq=state.created_seq,
                last_seq=event.seq,
            )

        for line in event.outputs:
            if line.batch_id == EXTERNAL:
                continue
            state = self.batches.get(line.batch_id)
            if state is None:
                info = meta.get(line.batch_id)
                if not info or "sku" not in info:
                    raise CorruptedLedgerError(
                        f"事件 #{event.seq} 新建批次 {line.batch_id} 缺少 sku 元数据"
                    )
                if not line.location:
                    raise CorruptedLedgerError(
                        f"事件 #{event.seq} 新建批次 {line.batch_id} 缺少库位"
                    )
                self.batches[line.batch_id] = BatchState(
                    batch_id=line.batch_id,
                    sku=info["sku"],
                    supplier_lot=info.get("supplier_lot"),
                    quantity=line.quantity,
                    location=line.location,
                    consumed=0,
                    created_seq=event.seq,
                    last_seq=event.seq,
                )
            else:
                patch = patches.get(line.batch_id, {})
                self.batches[line.batch_id] = BatchState(
                    batch_id=state.batch_id,
                    sku=patch.get("sku", state.sku),
                    supplier_lot=patch.get("supplier_lot", state.supplier_lot),
                    quantity=state.quantity + line.quantity,
                    location=line.location or state.location,
                    consumed=0,  # 重新有库存即恢复可用
                    created_seq=state.created_seq,
                    last_seq=event.seq,
                )

        # 无数量行的纯元数据更正（correction/reason=adjust_meta）
        if not event.inputs and not event.outputs:
            for bid, patch in patches.items():
                state = self.batches.get(bid)
                if state is None:
                    raise CorruptedLedgerError(
                        f"事件 #{event.seq} 试图更正不存在的批次 {bid}"
                    )
                self.batches[bid] = BatchState(
                    batch_id=state.batch_id,
                    sku=patch.get("sku", state.sku),
                    supplier_lot=patch.get("supplier_lot", state.supplier_lot),
                    quantity=state.quantity,
                    location=state.location,
                    consumed=state.consumed,
                    created_seq=state.created_seq,
                    last_seq=event.seq,
                )


# --------------------------------------------------------------------------- #
# EventStore
# --------------------------------------------------------------------------- #

class EventStore:
    def __init__(self, path: str | Path) -> None:
        self.is_memory = str(path) == ":memory:"
        self._guardian: sqlite3.Connection | None = None
        if self.is_memory:
            # 每个实例独立命名，避免测试间互相串数据
            self.memory_uri = f"file:memtrace-{uuid.uuid4().hex}?mode=memory&cache=shared"
            self.path = self.memory_uri
            self._guardian = connect_memory(self.memory_uri)
            conn = self._guardian
        else:
            self.path = str(path)
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
            conn = connect(self.path)
        try:
            init_schema(conn)
        finally:
            if not self.is_memory:
                conn.close()

    def open(self) -> sqlite3.Connection:
        """打开一个指向本账本的新连接。"""
        if self.is_memory:
            return connect_memory(self.memory_uri)
        return connect(self.path)

    def close(self) -> None:
        if self._guardian is not None:
            self._guardian.close()
            self._guardian = None

    # -- 基础读取 ---------------------------------------------------------- #

    @staticmethod
    def head_hash(conn: sqlite3.Connection) -> str:
        row = conn.execute("SELECT event_hash FROM events ORDER BY seq DESC LIMIT 1").fetchone()
        return row["event_hash"] if row else GENESIS_HASH

    def get_by_event_id(self, conn: sqlite3.Connection, event_id: str | None) -> Event | None:
        if not event_id:
            return None
        row = conn.execute("SELECT * FROM events WHERE event_id = ?", (event_id,)).fetchone()
        if row is None:
            return None
        line_rows = conn.execute(
            "SELECT * FROM event_lines WHERE seq = ? ORDER BY side, idx", (row["seq"],)
        ).fetchall()
        lines_by_seq: dict[int, dict[str, list[Line]]] = {}
        for lr in line_rows:
            lines_by_seq.setdefault(lr["seq"], {}).setdefault(lr["side"], []).append(
                Line(lr["batch_id"], D(lr["quantity"]), lr["location"], lr["external_ref"])
            )
        return self._row_to_event(row, lines_by_seq)

    @staticmethod
    def _row_to_event(row: sqlite3.Row, lines_by_seq: dict[int, dict[str, list[Line]]]) -> Event:
        seq = row["seq"]
        sides = lines_by_seq.get(seq, {})
        return Event(
            seq=seq,
            event_id=row["event_id"],
            event_type=row["event_type"],
            occurred_at=row["occurred_at"],
            actor=row["actor"],
            inputs=tuple(sides.get("in", [])),
            outputs=tuple(sides.get("out", [])),
            payload=json.loads(row["payload"]),
            supersedes=row["supersedes"],
            correction_of=row["correction_of"],
        )

    def load_events(self, conn: sqlite3.Connection) -> list[Event]:
        line_rows = conn.execute(
            "SELECT * FROM event_lines ORDER BY seq, side, idx"
        ).fetchall()
        lines_by_seq: dict[int, dict[str, list[Line]]] = {}
        for lr in line_rows:
            lines_by_seq.setdefault(lr["seq"], {}).setdefault(lr["side"], []).append(
                Line(
                    batch_id=lr["batch_id"],
                    quantity=D(lr["quantity"]),
                    location=lr["location"],
                    external_ref=lr["external_ref"],
                )
            )
        rows = conn.execute("SELECT * FROM events ORDER BY seq").fetchall()
        events = [self._row_to_event(row, lines_by_seq) for row in rows]
        # 回填 reversed_by
        by_seq = {e.seq: e for e in events}
        for e in events:
            if e.supersedes is not None:
                target = by_seq.get(e.supersedes)
                if target is not None:
                    object.__setattr__(target, "reversed_by", e.seq)
        return events

    def read_projection(self, conn: sqlite3.Connection) -> dict[str, BatchState]:
        result: dict[str, BatchState] = {}
        for row in conn.execute("SELECT * FROM batch_projection"):
            result[row["batch_id"]] = BatchState(
                batch_id=row["batch_id"],
                sku=row["sku"],
                supplier_lot=row["supplier_lot"],
                quantity=D(row["quantity"]),
                location=row["location"],
                consumed=bool(row["consumed"]),
                created_seq=row["created_seq"],
                last_seq=row["last_seq"],
            )
        return result

    # -- 追加 -------------------------------------------------------------- #

    def append(
        self,
        conn: sqlite3.Connection,
        *,
        event_type: str,
        actor: str,
        inputs: list[Line],
        outputs: list[Line],
        payload: dict[str, Any],
        occurred_at: str,
        event_id: str,
        supersedes: int | None = None,
        correction_of: int | None = None,
    ) -> Event:
        """在调用方已开启的事务中追加事件，并增量更新投影表。"""
        prev_hash = self.head_hash(conn)
        canonical = _canonical_event(
            event_id=event_id,
            event_type=event_type,
            occurred_at=occurred_at,
            actor=actor,
            payload=payload,
            supersedes=supersedes,
            correction_of=correction_of,
            inputs=inputs,
            outputs=outputs,
        )
        event_hash = compute_hash(prev_hash, canonical)

        cur = conn.execute(
            """
            INSERT INTO events (event_id, event_type, occurred_at, actor, payload,
                                supersedes, correction_of, prev_hash, event_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                event_type,
                occurred_at,
                actor,
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
                supersedes,
                correction_of,
                prev_hash,
                event_hash,
            ),
        )
        seq = cur.lastrowid

        for side, lines in (("in", inputs), ("out", outputs)):
            for idx, line in enumerate(lines):
                conn.execute(
                    """
                    INSERT INTO event_lines (seq, side, idx, batch_id, quantity,
                                             location, external_ref)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (seq, side, idx, line.batch_id, str(line.quantity),
                     line.location, line.external_ref),
                )

        # 在同一事务内增量维护投影（复用 Projection 规则）
        event = Event(
            seq=seq,
            event_id=event_id,
            event_type=event_type,
            occurred_at=occurred_at,
            actor=actor,
            inputs=tuple(inputs),
            outputs=tuple(outputs),
            payload=payload,
            supersedes=supersedes,
            correction_of=correction_of,
        )
        projection = Projection()
        projection.batches = self.read_projection(conn)
        projection.apply(event)
        self._persist_projection(conn, projection.batches)
        return event

    @staticmethod
    def _persist_projection(conn: sqlite3.Connection, batches: dict[str, BatchState]) -> None:
        for state in batches.values():
            conn.execute(
                """
                INSERT INTO batch_projection (batch_id, sku, supplier_lot, quantity,
                                              location, consumed, created_seq, last_seq)
                VALUES (:batch_id, :sku, :supplier_lot, :quantity, :location,
                        :consumed, :created_seq, :last_seq)
                ON CONFLICT(batch_id) DO UPDATE SET
                    sku=excluded.sku,
                    supplier_lot=excluded.supplier_lot,
                    quantity=excluded.quantity,
                    location=excluded.location,
                    consumed=excluded.consumed,
                    last_seq=excluded.last_seq
                """,
                {
                    "batch_id": state.batch_id,
                    "sku": state.sku,
                    "supplier_lot": state.supplier_lot,
                    "quantity": str(state.quantity),
                    "location": state.location,
                    "consumed": 1 if state.consumed else 0,
                    "created_seq": state.created_seq,
                    "last_seq": state.last_seq,
                },
            )

    # -- 校验 -------------------------------------------------------------- #

    def verify_hash_chain(self, conn: sqlite3.Connection) -> list[str]:
        """重算全部哈希，返回异常描述列表（空列表表示通过）。"""
        anomalies: list[str] = []
        prev = GENESIS_HASH
        rows = conn.execute("SELECT * FROM events ORDER BY seq").fetchall()
        lines_by_seq: dict[int, dict[str, list[Line]]] = {}
        for lr in conn.execute("SELECT * FROM event_lines ORDER BY seq, side, idx"):
            lines_by_seq.setdefault(lr["seq"], {}).setdefault(lr["side"], []).append(
                Line(lr["batch_id"], D(lr["quantity"]), lr["location"], lr["external_ref"])
            )
        for row in rows:
            if row["prev_hash"] != prev:
                anomalies.append(
                    f"事件 #{row['seq']} 的 prev_hash 与链上前值不符"
                )
            canonical = _canonical_event(
                event_id=row["event_id"],
                event_type=row["event_type"],
                occurred_at=row["occurred_at"],
                actor=row["actor"],
                payload=json.loads(row["payload"]),
                supersedes=row["supersedes"],
                correction_of=row["correction_of"],
                inputs=lines_by_seq.get(row["seq"], {}).get("in", []),
                outputs=lines_by_seq.get(row["seq"], {}).get("out", []),
            )
            expected = compute_hash(prev, canonical)
            if row["event_hash"] != expected:
                anomalies.append(f"事件 #{row['seq']} 的内容哈希不匹配")
            prev = row["event_hash"]
        return anomalies

    def replay(self, conn: sqlite3.Connection) -> dict[str, BatchState]:
        """从空状态重放全部事件，得到重建投影。"""
        projection = Projection()
        for event in self.load_events(conn):
            projection.apply(event)
        return projection.batches
