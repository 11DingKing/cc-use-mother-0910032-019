"""应用服务：批次全生命周期操作。

所有方法在单个 IMMEDIATE 事务内完成"重放校验 → 构成分录 → 落库"：

- 每条事件分录之和恒为 0（数量守恒）；
- 事件过账后，任何内部账户（stock/stage/fg）余额不得为负；
- 批次身份（拆分/合并新建的 lot）必须与台账一致。

撤销与历史更正不修改、不删除原事件，只追加补偿事件：
- ``reverse``：对原事件做红字反向分录（REVERSAL）；若下游已继续流转
  导致反向过账会出现负库存，则拒绝——必须自下游向上游按序补偿；
- ``correct``：追加显式平衡的 CORRECTION 事件并记录原因。
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Sequence

from .ledger import EventStore
from .models import (
    Event,
    EventType,
    Leg,
    QuantityError,
    TracebackError,
    ValidationError,
    fg_account,
    is_internal,
    new_id,
    qty,
    require_positive,
    stage_account,
    stock_account,
    supplier_account,
)
from .projection import Projection

SCRAP_ACCOUNT = "ext:scrap"
ADJUST_ACCOUNT = "ext:adjustment"


def _balanced(legs: Sequence[Leg]) -> bool:
    return sum((l.delta for l in legs), Decimal()) == 0


class InventoryService:
    def __init__(self, store: EventStore):
        self.store = store

    # ============================================================ 收货

    def receive(
        self,
        *,
        supplier_id: str,
        supplier_lot: str,
        quantity: Any,
        location: str,
        sku: str | None = None,
        lot_id: str | None = None,
        actor: str,
        occurred_at: str | None = None,
        idempotency_key: str | None = None,
    ) -> Event:
        supplier_id = _nonempty(supplier_id, "supplier_id")
        supplier_lot = _nonempty(supplier_lot, "supplier_lot")
        location = _nonempty(location, "location")
        amount = require_positive(qty(quantity), "收货数量")
        lot_id = lot_id or f"LOT-{new_id()[:12].upper()}"

        def build(events: list[Event]):
            proj = Projection(events)
            if lot_id in proj.lot_info:
                raise ValidationError(f"库存批次已存在：{lot_id}")
            legs = (
                Leg(supplier_account(supplier_id, supplier_lot), -amount),
                Leg(stock_account(lot_id, location), amount, lot_id),
            )
            payload = {
                "supplier_id": supplier_id,
                "supplier_lot": supplier_lot,
                "lot_id": lot_id,
                "location": location,
                "sku": sku,
                "quantity": str(amount),
            }
            _posting_checks(proj, legs)
            return EventType.RECEIVE, actor, payload, legs, occurred_at

        return self.store.append_with_check(build, idempotency_key=idempotency_key)

    # ============================================================ 移动

    def move(
        self,
        *,
        lot_id: str,
        quantity: Any,
        from_location: str,
        to_location: str,
        actor: str,
        occurred_at: str | None = None,
        idempotency_key: str | None = None,
    ) -> Event:
        lot_id = _nonempty(lot_id, "lot_id")
        from_location, to_location = _nonempty(from_location, "from_location"), _nonempty(to_location, "to_location")
        if from_location == to_location:
            raise ValidationError("源库位与目标库位相同")
        amount = require_positive(qty(quantity), "移动数量")

        def build(events: list[Event]):
            proj = Projection(events)
            _require_lot(proj, lot_id)
            src = stock_account(lot_id, from_location)
            _require_balance(proj, src, amount, "库位库存")
            legs = (
                Leg(src, -amount, lot_id),
                Leg(stock_account(lot_id, to_location), amount, lot_id),
            )
            payload = {
                "lot_id": lot_id,
                "quantity": str(amount),
                "from_location": from_location,
                "to_location": to_location,
            }
            _posting_checks(proj, legs)
            return EventType.MOVE, actor, payload, legs, occurred_at

        return self.store.append_with_check(build, idempotency_key=idempotency_key)

    # ==================================================== 拆包 / 拆分

    def split(
        self,
        *,
        source_lot: str,
        location: str,
        children: Sequence[dict[str, Any]],
        actor: str,
        occurred_at: str | None = None,
        idempotency_key: str | None = None,
    ) -> Event:
        """把 ``source_lot`` 拆为多个新批次（拆包）。

        children: [{"lot_id": 可选, "quantity": 数量, "location": 可选}]
        子批次总量可以小于父批次（余量保留在父批次）。
        """
        source_lot = _nonempty(source_lot, "source_lot")
        location = _nonempty(location, "location")
        if not children:
            raise ValidationError("至少需要一个子批次")

        normalized: list[dict[str, Any]] = []
        child_ids: set[str] = set()
        for child in children:
            cid = _nonempty(child.get("lot_id") or f"LOT-{new_id()[:12].upper()}", "lot_id")
            if cid in child_ids:
                raise ValidationError(f"子批次重复：{cid}")
            child_ids.add(cid)
            amount = require_positive(qty(child.get("quantity")), "子批次数量")
            cloc = _nonempty(child.get("location") or location, "child.location")
            normalized.append({"lot_id": cid, "quantity": amount, "location": cloc})

        def build(events: list[Event]):
            proj = Projection(events)
            _require_lot(proj, source_lot)
            for c in normalized:
                if c["lot_id"] in proj.lot_info:
                    raise ValidationError(f"库存批次已存在：{c['lot_id']}")
            total = sum((c["quantity"] for c in normalized), Decimal())
            src = stock_account(source_lot, location)
            _require_balance(proj, src, total, "待拆分库存")
            legs: list[Leg] = [Leg(src, -total, source_lot)]
            for c in normalized:
                legs.append(Leg(stock_account(c["lot_id"], c["location"]), c["quantity"], c["lot_id"]))
            payload = {
                "source_lot": source_lot,
                "location": location,
                "total_quantity": str(total),
                "children": [
                    {"lot_id": c["lot_id"], "quantity": str(c["quantity"]), "location": c["location"]}
                    for c in normalized
                ],
            }
            _posting_checks(proj, legs)
            return EventType.SPLIT, actor, payload, legs, occurred_at

        return self.store.append_with_check(build, idempotency_key=idempotency_key)

    # ============================================================ 合并

    def merge(
        self,
        *,
        sources: Sequence[dict[str, Any]],
        target_lot: str | None,
        target_location: str,
        actor: str,
        sku: str | None = None,
        occurred_at: str | None = None,
        idempotency_key: str | None = None,
    ) -> Event:
        """多个库存批次合并为新批次。

        sources: [{"lot_id": ..., "quantity": ..., "location": ...}]
        """
        if not sources:
            raise ValidationError("至少需要一个来源批次")
        target_location = _nonempty(target_location, "target_location")
        target_lot = _nonempty(target_lot or f"LOT-{new_id()[:12].upper()}", "target_lot")
        normalized: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for s in sources:
            sid = _nonempty(s.get("lot_id"), "source.lot_id")
            sloc = _nonempty(s.get("location"), "source.location")
            amount = require_positive(qty(s.get("quantity")), "来源数量")
            key = (sid, sloc)
            if key in seen:
                raise ValidationError(f"来源重复：{sid}@{sloc}")
            seen.add(key)
            normalized.append({"lot_id": sid, "location": sloc, "quantity": amount})

        def build(events: list[Event]):
            proj = Projection(events)
            if target_lot in proj.lot_info:
                raise ValidationError(f"目标批次已存在：{target_lot}（合并必须产生新批次）")
            legs: list[Leg] = []
            total = Decimal()
            for s in normalized:
                _require_lot(proj, s["lot_id"])
                acct = stock_account(s["lot_id"], s["location"])
                _require_balance(proj, acct, s["quantity"], "待合并库存")
                legs.append(Leg(acct, -s["quantity"], s["lot_id"]))
                total += s["quantity"]
            legs.append(Leg(stock_account(target_lot, target_location), total, target_lot))
            payload = {
                "target_lot": target_lot,
                "target_location": target_location,
                "sku": sku,
                "total_quantity": str(total),
                "sources": [
                    {"lot_id": s["lot_id"], "location": s["location"], "quantity": str(s["quantity"])}
                    for s in normalized
                ],
            }
            _posting_checks(proj, legs)
            return EventType.MERGE, actor, payload, legs, occurred_at

        return self.store.append_with_check(build, idempotency_key=idempotency_key)

    # ============================================================ 领料

    def issue(
        self,
        *,
        lot_id: str,
        quantity: Any,
        from_location: str,
        order_id: str,
        actor: str,
        occurred_at: str | None = None,
        idempotency_key: str | None = None,
    ) -> Event:
        """库位 → 生产工单暂存（并发安全：事务内余额校验）。"""
        lot_id = _nonempty(lot_id, "lot_id")
        from_location = _nonempty(from_location, "from_location")
        order_id = _nonempty(order_id, "order_id")
        amount = require_positive(qty(quantity), "领料数量")

        def build(events: list[Event]):
            proj = Projection(events)
            _require_lot(proj, lot_id)
            src = stock_account(lot_id, from_location)
            _require_balance(proj, src, amount, "可领库存")
            legs = (
                Leg(src, -amount, lot_id),
                Leg(stage_account(order_id, lot_id), amount, lot_id),
            )
            payload = {
                "lot_id": lot_id,
                "order_id": order_id,
                "from_location": from_location,
                "quantity": str(amount),
            }
            _posting_checks(proj, legs)
            return EventType.ISSUE, actor, payload, legs, occurred_at

        return self.store.append_with_check(build, idempotency_key=idempotency_key)

    # ================================================ 生产消耗 / 成品使用

    def consume(
        self,
        *,
        order_id: str,
        lot_id: str,
        quantity: Any,
        fg_batch: str,
        fg_quantity: Any | None = None,
        scrap_quantity: Any = 0,
        actor: str,
        occurred_at: str | None = None,
        idempotency_key: str | None = None,
    ) -> Event:
        """工单暂存物料投入成品批次。

        守恒：消耗数量 = 计入成品数量 + 报废数量（同一计量单位）。
        """
        order_id = _nonempty(order_id, "order_id")
        lot_id = _nonempty(lot_id, "lot_id")
        fg_batch = _nonempty(fg_batch, "fg_batch")
        amount = require_positive(qty(quantity), "消耗数量")
        fg_qty = qty(fg_quantity) if fg_quantity is not None else amount
        scrap_qty = qty(scrap_quantity)
        if fg_qty < 0 or scrap_qty < 0:
            raise ValidationError("成品/报废数量不能为负")
        if fg_qty + scrap_qty != amount:
            raise QuantityError(
                f"消耗不守恒：投入 {amount} != 成品 {fg_qty} + 报废 {scrap_qty}"
            )

        def build(events: list[Event]):
            proj = Projection(events)
            src = stage_account(order_id, lot_id)
            _require_balance(proj, src, amount, "工单暂存")
            legs: list[Leg] = [Leg(src, -amount, lot_id)]
            if fg_qty > 0:
                legs.append(Leg(fg_account(fg_batch), fg_qty, lot_id))
            if scrap_qty > 0:
                legs.append(Leg(SCRAP_ACCOUNT, scrap_qty, lot_id))
            payload = {
                "order_id": order_id,
                "lot_id": lot_id,
                "fg_batch": fg_batch,
                "quantity": str(amount),
                "fg_quantity": str(fg_qty),
                "scrap_quantity": str(scrap_qty),
            }
            _posting_checks(proj, legs)
            return EventType.CONSUME, actor, payload, legs, occurred_at

        return self.store.append_with_check(build, idempotency_key=idempotency_key)

    # ============================================================ 退料

    def return_material(
        self,
        *,
        order_id: str,
        lot_id: str,
        quantity: Any,
        to_location: str,
        actor: str,
        occurred_at: str | None = None,
        idempotency_key: str | None = None,
    ) -> Event:
        """工单暂存 → 库位（领用退回，批次身份不变）。"""
        order_id = _nonempty(order_id, "order_id")
        lot_id = _nonempty(lot_id, "lot_id")
        to_location = _nonempty(to_location, "to_location")
        amount = require_positive(qty(quantity), "退料数量")

        def build(events: list[Event]):
            proj = Projection(events)
            _require_lot(proj, lot_id)
            src = stage_account(order_id, lot_id)
            _require_balance(proj, src, amount, "可退暂存")
            legs = (
                Leg(src, -amount, lot_id),
                Leg(stock_account(lot_id, to_location), amount, lot_id),
            )
            payload = {
                "order_id": order_id,
                "lot_id": lot_id,
                "to_location": to_location,
                "quantity": str(amount),
            }
            _posting_checks(proj, legs)
            return EventType.RETURN, actor, payload, legs, occurred_at

        return self.store.append_with_check(build, idempotency_key=idempotency_key)

    # ================================================ 补偿：撤销 / 更正

    def reverse(
        self,
        *,
        event_id: str,
        reason: str,
        actor: str,
        occurred_at: str | None = None,
        idempotency_key: str | None = None,
    ) -> Event:
        """追加红字冲销事件撤销原事件（原事件保留、不可变）。

        若原事件的影响已被下游事件继续使用，冲销会造成负库存，
        必须先冲销其下游事件（沿链自后向前）。
        """
        reason = _nonempty(reason, "reason")

        def build(events: list[Event]):
            target = next((e for e in events if e.event_id == event_id), None)
            if target is None:
                raise TracebackError(f"待撤销事件不存在：{event_id}")
            if target.event_type in (EventType.REVERSAL, EventType.CORRECTION):
                raise ValidationError("补偿事件不能再被撤销，请使用更正（correction）")
            already = {
                e.payload.get("reverses_event_id")
                for e in events
                if e.event_type == EventType.REVERSAL
            }
            if event_id in already:
                raise ValidationError(f"事件已被撤销：{event_id}")

            proj = Projection(events)
            rev_legs = tuple(Leg(l.account, -l.delta, l.lot_id) for l in target.legs)
            _posting_checks(proj, rev_legs)
            payload = {
                "reverses_event_id": event_id,
                "reverses_seq": target.seq,
                "reverses_type": target.event_type.value,
                "reason": reason,
            }
            return EventType.REVERSAL, actor, payload, rev_legs, occurred_at

        return self.store.append_with_check(build, idempotency_key=idempotency_key)

    def correct(
        self,
        *,
        reason: str,
        legs: Sequence[dict[str, Any]],
        actor: str,
        reference: str | None = None,
        occurred_at: str | None = None,
        idempotency_key: str | None = None,
    ) -> Event:
        """历史更正：追加显式平衡分录（如盘盈/盘亏走 ext:adjustment）。

        legs: [{"account": ..., "delta": 带符号数量, "lot_id": 可选}]
        """
        reason = _nonempty(reason, "reason")
        if not legs:
            raise ValidationError("更正事件至少需要一条分录")
        parsed: list[Leg] = []
        for item in legs:
            acct = _nonempty(item.get("account"), "account")
            delta = qty(item.get("delta"))
            if delta == 0:
                raise ValidationError("更正分录数量不能为 0")
            parsed.append(Leg(acct, delta, item.get("lot_id")))
        if not _balanced(parsed):
            raise QuantityError("更正分录不平衡：delta 之和必须为 0")

        def build(events: list[Event]):
            proj = Projection(events)
            _posting_checks(proj, parsed)
            payload = {
                "reason": reason,
                "reference": reference,
                "legs": [l.to_json() for l in parsed],
            }
            return EventType.CORRECTION, actor, payload, tuple(parsed), occurred_at

        return self.store.append_with_check(build, idempotency_key=idempotency_key)


# ================================================================ 校验工具

def _nonempty(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{name} 不能为空")
    return value.strip()


def _require_lot(proj: Projection, lot_id: str) -> None:
    if lot_id not in proj.lot_info:
        raise TracebackError(f"台账中不存在该批次：{lot_id}")


def _require_balance(proj: Projection, account: str, needed: Decimal, what: str) -> None:
    have = proj.balances.get(account, Decimal())
    if have < needed:
        raise QuantityError(
            f"{what}不足：账户 {account} 现有 {have}，请求 {needed}（禁止负库存）"
        )


def _posting_checks(proj: Projection, legs: Sequence[Leg]) -> None:
    """事件过账前的两条硬不变量：分录平衡、内部账户不为负。"""
    if not _balanced(legs):
        raise QuantityError(
            "数量守恒校验失败：分录 delta 之和 = "
            + str(sum((l.delta for l in legs), Decimal()))
        )
    simulated = dict(proj.balances)
    for leg in legs:
        simulated[leg.account] = simulated.get(leg.account, Decimal()) + leg.delta
        if is_internal(leg.account) and simulated[leg.account] < 0:
            raise QuantityError(
                f"负库存保护：账户 {leg.account} 过账后余额 {simulated[leg.account]} < 0"
            )
