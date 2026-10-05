"""双向批次追踪与质量召回定位。

正向（供应商批次 -> 现在在哪 / 进了哪些成品）：
    trace_forward("SUP-A", "LOT-2026-09")
反向（成品批次 / 库存批次 -> 由哪些供应商批次构成）：
    trace_backward(fg_batch="FG-...") / trace_backward(lot_id="...")

召回报告 ``recall_report`` 汇总受影响的收货、拆包、领料、退料、
生产消耗（成品使用）记录及当前位置。
"""
from __future__ import annotations

from decimal import Decimal

from .ledger import EventStore
from .models import (
    Event,
    EventType,
    parse_fg_account,
    parse_stage_account,
    parse_stock_account,
)
from .projection import Projection


class TraceEngine:
    def __init__(self, store: EventStore):
        self.store = store

    # ------------------------------------------------------------ 正向

    def trace_forward(self, supplier_id: str, supplier_lot: str) -> dict:
        events = self.store.load_all()
        proj = Projection(events)
        root = f"{supplier_id}:{supplier_lot}"

        positions: list[dict] = []
        stage: list[dict] = []
        finished_goods: list[dict] = []
        losses: list[dict] = []

        for acct, amount in sorted(proj.root_locations(root).items()):
            if amount <= 0:
                continue
            parsed_stock = parse_stock_account(acct)
            parsed_stage = parse_stage_account(acct)
            parsed_fg = parse_fg_account(acct)
            if parsed_stock:
                lot_id, location = parsed_stock
                positions.append({"lot_id": lot_id, "location": location, "quantity": str(amount)})
            elif parsed_stage:
                order_id, lot_id = parsed_stage
                stage.append({"order_id": order_id, "lot_id": lot_id, "quantity": str(amount)})
            elif parsed_fg is not None:
                finished_goods.append({"fg_batch": parsed_fg, "quantity": str(amount)})
            elif acct.startswith("ext:"):
                losses.append({"sink": acct, "quantity": str(amount)})

        introduced = proj.root_introduced.get(root, Decimal())
        located = sum(
            (Decimal(x["quantity"]) for group in (positions, stage, finished_goods, losses) for x in group),
            Decimal(),
        )
        return {
            "supplier_id": supplier_id,
            "supplier_lot": supplier_lot,
            "introduced_quantity": str(introduced),
            "current_stock": positions,
            "in_production_stage": stage,
            "in_finished_goods": finished_goods,
            "consumed_or_lost": losses,
            "located_quantity": str(located),
            "accounted": introduced == located,
            "affected_events": self._affected_events(events, root, proj),
        }

    # ------------------------------------------------------------ 反向

    def trace_backward(
        self,
        *,
        fg_batch: str | None = None,
        lot_id: str | None = None,
        location: str | None = None,
        order_id: str | None = None,
    ) -> dict:
        events = self.store.load_all()
        proj = Projection(events)

        if fg_batch is not None:
            accounts = [f"fg:{fg_batch}"]
            target = {"fg_batch": fg_batch}
        elif lot_id is not None:
            accounts = []
            for acct in proj.composition:
                parsed_stock = parse_stock_account(acct)
                parsed_stage = parse_stage_account(acct)
                if parsed_stock and parsed_stock[0] == lot_id:
                    if location is None or parsed_stock[1] == location:
                        accounts.append(acct)
                elif parsed_stage and parsed_stage[1] == lot_id:
                    if order_id is None or parsed_stage[0] == order_id:
                        accounts.append(acct)
            target = {"lot_id": lot_id}
        else:
            raise ValueError("必须提供 fg_batch 或 lot_id")

        roots: dict[str, Decimal] = {}
        total = Decimal()
        per_account = []
        for acct in accounts:
            comp = proj.composition_of(acct)
            balance = proj.balances.get(acct, Decimal())
            if balance > 0 or comp:
                per_account.append(
                    {"account": acct, "balance": str(balance), "supplier_lots": {r: str(v) for r, v in comp.items()}}
                )
            for root, value in comp.items():
                roots[root] = roots.get(root, Decimal()) + value
                total += value

        suppliers = [
            {
                "supplier_id": root.split(":", 1)[0],
                "supplier_lot": root.split(":", 1)[1],
                "quantity": str(value),
            }
            for root, value in sorted(roots.items())
        ]
        return {
            **target,
            "total_traced_quantity": str(total),
            "supplier_origins": suppliers,
            "accounts": per_account,
        }

    # ------------------------------------------------------ 召回报告

    def recall_report(self, supplier_id: str, supplier_lot: str) -> dict:
        """质量召回：定位受影响的收货、拆包、领料、退料、成品使用记录。"""
        events = self.store.load_all()
        proj = Projection(events)
        root = f"{supplier_id}:{supplier_lot}"
        affected = self._affected_events(events, root, proj)

        buckets = {
            "receives": [],
            "splits": [],
            "merges": [],
            "moves": [],
            "issues": [],
            "returns": [],
            "consumptions": [],
            "corrections": [],
        }
        type_map = {
            EventType.RECEIVE: "receives",
            EventType.SPLIT: "splits",
            EventType.MERGE: "merges",
            EventType.MOVE: "moves",
            EventType.ISSUE: "issues",
            EventType.RETURN: "returns",
            EventType.CONSUME: "consumptions",
            EventType.CORRECTION: "corrections",
        }
        for item in affected:
            bucket = type_map.get(item["event_type"])
            if bucket:
                buckets[bucket].append(item)

        forward = self.trace_forward(supplier_id, supplier_lot)
        return {
            "supplier_id": supplier_id,
            "supplier_lot": supplier_lot,
            "introduced_quantity": forward["introduced_quantity"],
            "records": buckets,
            "current_stock": forward["current_stock"],
            "in_production_stage": forward["in_production_stage"],
            "in_finished_goods": forward["in_finished_goods"],
            "consumed_or_lost": forward["consumed_or_lost"],
            "located_quantity": forward["located_quantity"],
            "accounted": forward["accounted"],
        }

    # ------------------------------------------------------------ 内部

    def _affected_events(self, events: list[Event], root: str, proj: Projection) -> list[dict]:
        """找出所有"承载过"该供应商批次成分的事件（含补偿事件）。

        判定：事件任一内部账户在过账前/后含有该 root 成分，
        或事件本身直接引用该供应商批次（收货）。
        """
        live = Projection()
        out: list[dict] = []
        for event in events:
            touched = False
            if event.event_type == EventType.RECEIVE:
                p = event.payload
                if f"{p.get('supplier_id')}:{p.get('supplier_lot')}" == root:
                    touched = True
            if not touched:
                accounts = {leg.account for leg in event.legs}
                for acct in accounts:
                    comp_before = live.composition.get(acct, {})
                    if comp_before.get(root, 0) > 0:
                        touched = True
                        break
            # 补偿事件：若它冲销/引用的目标事件已受影响，则同样受影响
            if not touched and event.event_type == EventType.REVERSAL:
                rev_id = event.payload.get("reverses_event_id")
                if any(e["event_id"] == rev_id for e in out):
                    touched = True
            live.apply(event)
            if touched:
                out.append(_summarize_event(event))
        return out


def _summarize_event(event: Event) -> dict:
    p = dict(event.payload)
    return {
        "seq": event.seq,
        "event_id": event.event_id,
        "event_type": event.event_type,
        "actor": event.actor,
        "occurred_at": event.occurred_at,
        "payload": p,
    }
