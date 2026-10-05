"""库存命令服务：所有写操作的守恒校验与补偿事件。

每个命令都在一个 ``BEGIN IMMEDIATE`` 事务内完成“读取当前投影 → 校验
前置条件与数量守恒 → 追加事件 → 增量更新投影”，因此并发领料会被
SQLite 写锁串行化，后提交者基于最新库存重新判定，从机制上杜绝超领
和负库存。
"""
from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from .errors import ConflictError, NotFoundError, ValidationError
from .models import (
    BALANCED_EVENTS,
    CORRECTION_EVENT,
    REVERSIBLE_EVENTS,
    D,
    Event,
    Line,
)
from .store import EventStore

EXTERNAL = "*"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def new_batch_id() -> str:
    return "B" + uuid.uuid4().hex[:10].upper()


def positive_quantity(value: Any, field: str) -> Decimal:
    if isinstance(value, bool):
        raise ValidationError(f"{field} 必须是正数")
    try:
        qty = D(value)
    except Exception as exc:  # invalid decimal literal
        raise ValidationError(f"{field} 不是合法的数量：{value!r}") from exc
    if not qty.is_finite() or qty <= 0:
        raise ValidationError(f"{field} 必须大于 0")
    return qty


class InventoryService:
    def __init__(self, path: str = ":memory:") -> None:
        self.store = EventStore(path)

    @contextmanager
    def _write_tx(self) -> Iterator[sqlite3.Connection]:
        conn = self.store.open()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    @contextmanager
    def _read_tx(self) -> Iterator[sqlite3.Connection]:
        conn = self.store.open()
        try:
            yield conn
        finally:
            conn.close()

    # ------------------------------------------------------------------ #
    # 命令
    # ------------------------------------------------------------------ #

    def receive(
        self,
        *,
        sku: str,
        quantity: Any,
        location: str,
        supplier_lot: str | None = None,
        actor: str = "仓储管理员",
        batch_id: str | None = None,
        event_id: str | None = None,
        occurred_at: str | None = None,
        note: str | None = None,
    ) -> dict:
        """批次进入仓库（收货）。"""
        sku = _require_text(sku, "sku")
        location = _require_text(location, "location")
        qty = positive_quantity(quantity, "quantity")
        batch_id = batch_id or new_batch_id()
        inputs = [Line(EXTERNAL, qty, None, supplier_lot)]
        outputs = [Line(batch_id, qty, location, None)]
        payload: dict[str, Any] = {
            "sku": sku,
            "supplier_lot": supplier_lot,
            "batch_meta": {batch_id: {"sku": sku, "supplier_lot": supplier_lot}},
        }
        if note:
            payload["note"] = note
        return self._append(
            "receive", actor, inputs, outputs, payload,
            event_id=event_id, occurred_at=occurred_at,
        )

    def move(
        self,
        *,
        batch_id: str,
        quantity: Any,
        to_location: str,
        moved_batch_id: str | None = None,
        actor: str = "仓储管理员",
        event_id: str | None = None,
    ) -> dict:
        """批次在库位间移动。

        全量移动保持批次编号不变；部分移动会生成一个新批次承载移走的数量
        （原批次保留余量），并在血缘图中保留父子边，保证一个批次同一时刻
        只位于一个库位、来源链不断裂。
        """
        to_location = _require_text(to_location, "to_location")
        qty = positive_quantity(quantity, "quantity")
        with self._write_tx() as conn:
            projection = self.store.read_projection(conn)
            state = projection.get(batch_id)
            if state is None:
                raise NotFoundError(f"批次 {batch_id} 不存在")
            self._require_available(state, qty)
            if state.location == to_location:
                raise ValidationError("目标库位与当前库位相同")
            inputs = [Line(batch_id, qty, state.location, None)]
            payload: dict[str, Any] = {}
            if qty == state.quantity:
                outputs = [Line(batch_id, qty, to_location, None)]
            else:
                moved_id = moved_batch_id or new_batch_id()
                if moved_id in projection:
                    raise ValidationError(f"批次编号已存在：{moved_id}")
                outputs = [Line(moved_id, qty, to_location, None)]
                payload = {
                    "partial": True,
                    "moved_batch_id": moved_id,
                    "batch_meta": {moved_id: {
                        "sku": state.sku,
                        "supplier_lot": state.supplier_lot,
                    }},
                }
            event = self._append_locked(
                conn, "move", actor, inputs, outputs, payload,
                event_id=event_id,
            )
        return event.to_dict()

    def split(
        self,
        *,
        batch_id: str,
        children: list[dict],
        actor: str = "仓储管理员",
        event_id: str | None = None,
    ) -> dict:
        """把一个批次拆成多个子批次，子批次数量之和必须等于拆分数量。

        children: [{"quantity": ..., "location": ...(可选), "batch_id": ...(可选)}]
        未指定 location 时子批次留在父批次库位；拆分数量默认是父批次全部数量，
        也可通过 ``quantity`` 做部分拆分（剩余留在父批次）。
        """
        if not children:
            raise ValidationError("拆分至少需要一个子批次")
        child_lines: list[Line] = []
        total = Decimal(0)
        child_meta: dict[str, dict] = {}
        seen: set[str] = set()
        for child in children:
            qty = positive_quantity(child.get("quantity"), "children.quantity")
            cid = child.get("batch_id") or new_batch_id()
            if cid in seen:
                raise ValidationError(f"子批次编号重复：{cid}")
            seen.add(cid)
            total += qty
            child_lines.append(Line(cid, qty, child.get("location"), None))

        with self._write_tx() as conn:
            projection = self.store.read_projection(conn)
            state = projection.get(batch_id)
            if state is None:
                raise NotFoundError(f"批次 {batch_id} 不存在")
            # 部分拆分：子批次数量之和 <= 父批次当前数量，余量留在父批次
            self._require_available(state, total)
            final_lines: list[Line] = []
            for line in child_lines:
                loc = line.location or state.location
                final_lines.append(Line(line.batch_id, line.quantity, loc, None))
                if line.batch_id in projection:
                    raise ValidationError(f"批次编号已存在：{line.batch_id}")
                child_meta[line.batch_id] = {
                    "sku": state.sku,
                    "supplier_lot": state.supplier_lot,
                }
            inputs = [Line(batch_id, total, state.location, None)]
            payload = {
                "parent": batch_id,
                "children": [
                    {"batch_id": l.batch_id, "quantity": str(l.quantity), "location": l.location}
                    for l in final_lines
                ],
                "batch_meta": child_meta,
            }
            event = self._append_locked(
                conn, "split", actor, inputs, final_lines, payload,
                event_id=event_id,
            )
        return event.to_dict()

    def merge(
        self,
        *,
        sources: list[dict],
        location: str,
        batch_id: str | None = None,
        actor: str = "仓储管理员",
        event_id: str | None = None,
    ) -> dict:
        """把多个同 SKU 批次合并为一个新批次。

        sources: [{"batch_id": ..., "quantity": ...(可选，默认全部)}]
        """
        location = _require_text(location, "location")
        if not sources or len(sources) < 2:
            raise ValidationError("合并至少需要两个来源批次")
        with self._write_tx() as conn:
            projection = self.store.read_projection(conn)
            input_lines: list[Line] = []
            sku: str | None = None
            lots: set[str | None] = set()
            total = Decimal(0)
            for src in sources:
                sid = src.get("batch_id")
                if not sid:
                    raise ValidationError("sources.batch_id 不能为空")
                state = projection.get(sid)
                if state is None:
                    raise NotFoundError(f"批次 {sid} 不存在")
                qty = (
                    positive_quantity(src["quantity"], "sources.quantity")
                    if src.get("quantity") is not None
                    else state.quantity
                )
                self._require_available(state, qty)
                if sku is None:
                    sku = state.sku
                elif state.sku != sku:
                    raise ValidationError(
                        f"只能合并且相同 SKU 的批次：{sku} 与 {state.sku}"
                    )
                input_lines.append(Line(sid, qty, state.location, None))
                lots.add(state.supplier_lot)
                total += qty
            new_id = batch_id or new_batch_id()
            if new_id in projection:
                raise ValidationError(f"批次编号已存在：{new_id}")
            outputs = [Line(new_id, total, location, None)]
            # 只有所有来源同供应商批次时才继承批号，否则批号置空（混合批次）
            inherited_lot = next(iter(lots)) if len(lots) == 1 else None
            payload = {
                "batch_id": new_id,
                "sku": sku,
                "batch_meta": {new_id: {"sku": sku, "supplier_lot": inherited_lot}},
            }
            event = self._append_locked(
                conn, "merge", actor, input_lines, outputs, payload,
                event_id=event_id,
            )
        return event.to_dict()

    def consume(
        self,
        *,
        allocations: list[dict],
        work_order: str,
        product: str | None = None,
        actor: str = "仓储管理员",
        event_id: str | None = None,
        occurred_at: str | None = None,
        note: str | None = None,
    ) -> dict:
        """并发安全的领料（批次投入生产消耗）。"""
        work_order = _require_text(work_order, "work_order")
        if not allocations:
            raise ValidationError("allocations 不能为空")
        requested: list[tuple[str, Decimal]] = []
        for item in allocations:
            requested.append(
                (item.get("batch_id"), positive_quantity(item.get("quantity"), "allocations.quantity"))
            )
        with self._write_tx() as conn:
            projection = self.store.read_projection(conn)
            input_lines: list[Line] = []
            total = Decimal(0)
            for bid, qty in requested:
                if not bid:
                    raise ValidationError("allocations.batch_id 不能为空")
                state = projection.get(bid)
                if state is None:
                    raise NotFoundError(f"批次 {bid} 不存在")
                # 关键：负库存保护在写锁内基于最新投影判定
                self._require_available(state, qty)
                input_lines.append(Line(bid, qty, state.location, None))
                total += qty
            outputs = [Line(EXTERNAL, total, None, work_order)]
            payload: dict[str, Any] = {
                "work_order": work_order,
                "product": product,
                "allocations": [
                    {"batch_id": l.batch_id, "quantity": str(l.quantity)}
                    for l in input_lines
                ],
            }
            if note:
                payload["note"] = note
            event = self._append_locked(
                conn, "consume", actor, input_lines, outputs, payload,
                event_id=event_id, occurred_at=occurred_at,
            )
        return event.to_dict()

    def return_stock(
        self,
        *,
        work_order: str,
        sku: str,
        quantity: Any,
        location: str,
        supplier_lot: str | None = None,
        actor: str = "仓储管理员",
        batch_id: str | None = None,
        event_id: str | None = None,
    ) -> dict:
        """生产余料退回，生成与生产工单血缘相连的新批次。"""
        work_order = _require_text(work_order, "work_order")
        sku = _require_text(sku, "sku")
        location = _require_text(location, "location")
        qty = positive_quantity(quantity, "quantity")
        new_id = batch_id or new_batch_id()
        inputs = [Line(EXTERNAL, qty, None, work_order)]
        outputs = [Line(new_id, qty, location, None)]
        payload = {
            "work_order": work_order,
            "sku": sku,
            "batch_meta": {new_id: {"sku": sku, "supplier_lot": supplier_lot}},
        }
        return self._append(
            "return", actor, inputs, outputs, payload, event_id=event_id,
        )

    # ------------------------------------------------------------------ #
    # 补偿事件：撤销与历史更正
    # ------------------------------------------------------------------ #

    def reverse_event(
        self,
        seq: int,
        *,
        actor: str = "质量工程师",
        event_id: str | None = None,
        note: str | None = None,
    ) -> dict:
        """撤销一条历史事件。

        不删除也不修改原事件，而是追加一条镜像的补偿事件（inputs/outputs
        互换）。补偿必须在当前库存状态下可行：若该批次后来被拆分、消耗或
        移动，需要先撤销下游事件，保证数量链条按序回滚。
        """
        with self._write_tx() as conn:
            events = self.store.load_events(conn)
            by_seq = {e.seq: e for e in events}
            target = by_seq.get(seq)
            if target is None:
                raise NotFoundError(f"事件 #{seq} 不存在")
            if target.event_type == "reversal":
                raise ValidationError("补偿事件不能再次被撤销")
            if target.event_type not in REVERSIBLE_EVENTS:
                raise ConflictError(
                    f"{target.event_type} 事件不能镜像撤销，请签发反向 correction"
                )
            if target.supersedes is not None:
                raise ConflictError(f"事件 #{seq} 本身已是补偿事件")
            if target.reversed_by is not None:
                raise ConflictError(
                    f"事件 #{seq} 已被事件 #{target.reversed_by} 撤销",
                    details={"reversed_by": target.reversed_by},
                )
            projection = self.store.read_projection(conn)

            # 镜像原事件的两侧
            new_inputs = [
                Line(l.batch_id, l.quantity, l.location, l.external_ref)
                for l in target.outputs
            ]
            # 恢复侧（outputs）的库位规则：
            # * 同批次移动（move，批次同时出现在原事件两侧）：恢复到原事件
            #   input 侧的旧库位，即撤销移动的本意；
            # * 只在 input 侧的批次（拆分父批次、合并来源、领料批次）：把数量
            #   补回批次当前所在库位，允许事件之后发生过位置无关的变化。
            output_batches = {l.batch_id for l in target.outputs if l.batch_id != EXTERNAL}
            new_outputs: list[Line] = []
            for l in target.inputs:
                if l.batch_id != EXTERNAL and l.batch_id not in output_batches:
                    current = projection.get(l.batch_id)
                    loc = current.location if current is not None else l.location
                    new_outputs.append(Line(l.batch_id, l.quantity, loc, l.external_ref))
                else:
                    new_outputs.append(Line(l.batch_id, l.quantity, l.location, l.external_ref))
            # 撤销的当前状态可行性：数量足够、库位仍吻合
            for line in new_inputs:
                if line.batch_id == EXTERNAL:
                    continue
                state = projection.get(line.batch_id)
                if state is None:
                    raise ConflictError(
                        f"无法撤销事件 #{seq}：批次 {line.batch_id} 当前不存在"
                    )
                if state.quantity < line.quantity:
                    raise ConflictError(
                        f"无法撤销事件 #{seq}：批次 {line.batch_id} 当前库存 "
                        f"{state.quantity} 不足 {line.quantity}，请先撤销其下游事件"
                    )
                if line.location and state.location != line.location:
                    raise ConflictError(
                        f"无法撤销事件 #{seq}：批次 {line.batch_id} 已移动到 "
                        f"{state.location}，请先按相反顺序撤销后续移动"
                    )
            payload: dict[str, Any] = {
                "reversed_seq": seq,
                "reversed_type": target.event_type,
            }
            if note:
                payload["note"] = note
            event = self._append_locked(
                conn, "reversal", actor, new_inputs, new_outputs, payload,
                event_id=event_id, supersedes=seq,
            )
        return event.to_dict()

    def correct(
        self,
        *,
        batch_id: str,
        reason: str,
        quantity: Any = None,
        actor: str = "质量工程师",
        supplier_lot: str | None = None,
        sku: str | None = None,
        event_id: str | None = None,
        note: str | None = None,
    ) -> dict:
        """历史/账面差异更正，一律以补偿事件留痕。

        reason:
        * ``short`` 盘亏：库存调减，必须有足够当前库存；
        * ``gain`` 盘盈：库存调增；
        * ``adjust_meta``：仅更正 SKU / 供应商批次号等元数据，不动数量。
        """
        if reason not in {"short", "gain", "adjust_meta"}:
            raise ValidationError("reason 必须是 short / gain / adjust_meta")
        with self._write_tx() as conn:
            state = self.store.read_projection(conn).get(batch_id)
            if state is None:
                raise NotFoundError(f"批次 {batch_id} 不存在")
            payload: dict[str, Any] = {"reason": reason}
            inputs: list[Line] = []
            outputs: list[Line] = []
            meta_patch: dict[str, dict] = {}
            if reason in {"short", "gain"}:
                qty = positive_quantity(quantity, "quantity")
                if reason == "short":
                    self._require_available(state, qty)
                    inputs = [Line(batch_id, qty, state.location, None)]
                else:
                    outputs = [Line(batch_id, qty, state.location, None)]
                payload["quantity"] = str(qty)
            else:
                patch: dict[str, str] = {}
                previous: dict[str, str | None] = {}
                if sku is not None:
                    patch["sku"] = _require_text(sku, "sku")
                    previous["sku"] = state.sku
                if supplier_lot is not None:
                    patch["supplier_lot"] = _require_text(supplier_lot, "supplier_lot")
                    previous["supplier_lot"] = state.supplier_lot
                if not patch:
                    raise ValidationError("adjust_meta 必须提供 sku 或 supplier_lot")
                meta_patch = {batch_id: patch}
                payload["patch"] = patch
                payload["previous"] = previous
            if note:
                payload["note"] = note
            if meta_patch:
                payload["meta_patch"] = meta_patch
            event = self._append_locked(
                conn, "correction", actor, inputs, outputs, payload,
                event_id=event_id,
            )
        return event.to_dict()

    # ------------------------------------------------------------------ #
    # 查询
    # ------------------------------------------------------------------ #

    def list_events(self, *, batch_id: str | None = None, limit: int = 200) -> list[dict]:
        with self._read_tx() as conn:
            events = self.store.load_events(conn)
        if batch_id:
            events = [
                e for e in events
                if any(l.batch_id == batch_id for l in (*e.inputs, *e.outputs))
            ]
        return [e.to_dict() for e in events[-limit:]]

    def get_event(self, seq: int) -> dict:
        with self._read_tx() as conn:
            for e in self.store.load_events(conn):
                if e.seq == seq:
                    return e.to_dict()
        raise NotFoundError(f"事件 #{seq} 不存在")

    def list_batches(
        self,
        *,
        sku: str | None = None,
        location: str | None = None,
        include_zero: bool = False,
    ) -> list[dict]:
        with self._read_tx() as conn:
            batches = list(self.store.read_projection(conn).values())
        result = []
        for state in sorted(batches, key=lambda b: b.batch_id):
            if not include_zero and state.quantity == 0:
                continue
            if sku and state.sku != sku:
                continue
            if location and state.location != location:
                continue
            result.append(state.to_dict())
        return result

    def get_batch(self, batch_id: str) -> dict:
        with self._read_tx() as conn:
            state = self.store.read_projection(conn).get(batch_id)
        if state is None:
            raise NotFoundError(f"批次 {batch_id} 不存在")
        return state.to_dict()

    # ------------------------------------------------------------------ #
    # 内部
    # ------------------------------------------------------------------ #

    def _append(
        self, event_type: str, actor: str,
        inputs: list[Line], outputs: list[Line], payload: dict[str, Any],
        *, event_id: str | None, occurred_at: str | None = None,
        supersedes: int | None = None, correction_of: int | None = None,
    ) -> dict:
        with self._write_tx() as conn:
            event = self._append_locked(
                conn, event_type, actor, inputs, outputs, payload,
                event_id=event_id, occurred_at=occurred_at,
                supersedes=supersedes, correction_of=correction_of,
            )
        return event.to_dict()

    def _append_locked(
        self, conn: sqlite3.Connection, event_type: str, actor: str,
        inputs: list[Line], outputs: list[Line], payload: dict[str, Any],
        *, event_id: str | None, occurred_at: str | None = None,
        supersedes: int | None = None, correction_of: int | None = None,
    ) -> Event:
        self._validate_conservation(event_type, inputs, outputs)
        try:
            return self.store.append(
                conn,
                event_type=event_type,
                actor=actor,
                inputs=inputs,
                outputs=outputs,
                payload=payload,
                occurred_at=occurred_at or utc_now(),
                event_id=event_id or str(uuid.uuid4()),
                supersedes=supersedes,
                correction_of=correction_of,
            )
        except sqlite3.IntegrityError as exc:
            # 幂等重放：相同 event_id 已存在则原样返回，不重复记账
            existing = self.store.get_by_event_id(conn, event_id)
            if existing is not None:
                return existing
            raise ConflictError(
                "事件违反数据约束", details={"detail": str(exc)}
            ) from exc

    @staticmethod
    def _validate_conservation(
        event_type: str, inputs: list[Line], outputs: list[Line],
    ) -> None:
        """事件级数量守恒：内部流转两侧恒等；消耗只减不增；更正单边。"""
        in_total = sum((l.quantity for l in inputs if l.batch_id != EXTERNAL), Decimal(0))
        out_total = sum((l.quantity for l in outputs if l.batch_id != EXTERNAL), Decimal(0))
        if event_type in BALANCED_EVENTS:
            # 含外部行（receive/return）时按含外部行的两侧总量比较
            raw_in = sum((l.quantity for l in inputs), Decimal(0))
            raw_out = sum((l.quantity for l in outputs), Decimal(0))
            if raw_in != raw_out:
                raise ValidationError(
                    f"数量守恒被破坏：{event_type} 两侧总量 {raw_in} != {raw_out}"
                )
            if event_type in {"move", "split", "merge"} and (in_total == 0 or out_total == 0):
                raise ValidationError(f"{event_type} 必须同时有来源与去向批次")
        elif event_type == "consume":
            if in_total <= 0 or out_total != 0:
                raise ValidationError("consume 只能有库存侧 inputs，不能有库存侧 outputs")
        elif event_type == CORRECTION_EVENT:
            no_lines = not inputs and not outputs
            if not no_lines and (in_total > 0) == (out_total > 0):
                raise ValidationError("correction 必须恰好单侧调整数量，或不带任何行（仅更正元数据）")
        else:  # pragma: no cover - 防御未知类型
            raise ValidationError(f"未知事件类型：{event_type}")
        for line in (*inputs, *outputs):
            if line.quantity <= 0:
                raise ValidationError("事件行数量必须大于 0")

    @staticmethod
    def _require_available(state, qty: Decimal) -> None:
        if state.quantity <= 0:
            raise ConflictError(f"批次 {state.batch_id} 已无可用库存")
        if state.quantity < qty:
            raise ConflictError(
                f"批次 {state.batch_id} 可用库存不足：当前 {state.quantity}，请求 {qty}",
                details={"available": str(state.quantity), "requested": str(qty)},
            )


def _require_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{field} 必须是非空字符串")
    return value.strip()
