"""台账一致性检查。

重放全量事件并输出检查报告，覆盖：

1. 事件结构：分录非空、数量为正约定、分录之和为 0（逐事件守恒）；
2. 负库存：重放到任意序号时内部账户余额不得为负；
3. 全局守恒：边界引入（供应商收货）== 内部现存 + 边界流出（报废/调整等）；
4. 成分闭合：每个内部账户的供应商成分向量之和 == 账户余额；
5. 批次身份：拆分/合并引用的批次必须存在、目标批次不得预先存在；
6. 补偿完整性：REVERSAL 引用的事件存在、未重复冲销；
7. 供应商批次闭合：每个供应商批次引入量 == 可定位量（现存 + 损失）。
"""
from __future__ import annotations

from decimal import Decimal

from .ledger import EventStore
from .models import Event, EventType, is_internal
from .projection import Projection

EPS = Decimal("0.000001")


class ConsistencyChecker:
    def __init__(self, store: EventStore):
        self.store = store

    def check(self) -> dict:
        events = self.store.load_all()
        issues: list[dict] = []

        balances: dict[str, Decimal] = {}
        known_lots: set[str] = set()
        reversed_ids: set[str] = set()
        ids = {e.event_id for e in events}

        for event in events:
            # 1) 逐事件平衡
            total = sum((leg.delta for leg in event.legs), Decimal())
            if total != 0:
                issues.append(_issue(event, "UNBALANCED_EVENT", f"分录之和 {total} != 0"))
            if not event.legs:
                issues.append(_issue(event, "EMPTY_LEGS", "事件没有分录"))

            # 2) 过账 + 负库存
            for leg in event.legs:
                balances[leg.account] = balances.get(leg.account, Decimal()) + leg.delta
                if is_internal(leg.account) and balances[leg.account] < 0:
                    issues.append(
                        _issue(event, "NEGATIVE_BALANCE", f"账户 {leg.account} 余额 {balances[leg.account]} < 0")
                    )

            # 5) 批次身份
            p = event.payload
            if event.event_type == EventType.RECEIVE:
                lot = p.get("lot_id")
                if lot in known_lots:
                    issues.append(_issue(event, "DUPLICATE_LOT", f"批次重复创建：{lot}"))
                known_lots.add(lot)
            elif event.event_type == EventType.SPLIT:
                if p.get("source_lot") not in known_lots:
                    issues.append(_issue(event, "UNKNOWN_SOURCE_LOT", f"拆分源批次不存在：{p.get('source_lot')}"))
                for child in p.get("children", []):
                    cid = child.get("lot_id")
                    if cid in known_lots:
                        issues.append(_issue(event, "DUPLICATE_LOT", f"子批次已存在：{cid}"))
                    known_lots.add(cid)
            elif event.event_type == EventType.MERGE:
                for s in p.get("sources", []):
                    if s.get("lot_id") not in known_lots:
                        issues.append(_issue(event, "UNKNOWN_SOURCE_LOT", f"合并来源批次不存在：{s.get('lot_id')}"))
                t = p.get("target_lot")
                if t in known_lots:
                    issues.append(_issue(event, "DUPLICATE_LOT", f"合并目标批次已存在：{t}"))
                known_lots.add(t)

            # 6) 补偿引用
            if event.event_type == EventType.REVERSAL:
                rev = p.get("reverses_event_id")
                if rev not in ids:
                    issues.append(_issue(event, "DANGLING_REVERSAL", f"冲销事件引用不存在：{rev}"))
                if rev in reversed_ids:
                    issues.append(_issue(event, "DOUBLE_REVERSAL", f"事件被重复冲销：{rev}"))
                reversed_ids.add(rev)

        # 3) 全局边界守恒
        internal_total = sum((b for a, b in balances.items() if is_internal(a)), Decimal())
        boundary_total = sum(
            (b for a, b in balances.items() if not is_internal(a) and not a.startswith("ext:supplier:")),
            Decimal(),
        )
        supplier_total = sum(
            (b for a, b in balances.items() if a.startswith("ext:supplier:")),
            Decimal(),
        )
        # 供应商账户为负（出库方向），其余外部账户为正（流入损耗/调整）
        if -supplier_total != internal_total + boundary_total:
            issues.append(
                {
                    "code": "GLOBAL_CONSERVATION_BROKEN",
                    "message": (
                        f"全局不守恒：供应商引入 {-supplier_total} != "
                        f"内部现存 {internal_total} + 边界流出 {boundary_total}"
                    ),
                }
            )

        proj = Projection(events)

        # 4) 成分向量闭合
        for acct, balance in proj.balances.items():
            if not is_internal(acct) or balance <= 0:
                continue
            comp_sum = sum(proj.composition.get(acct, {}).values(), Decimal())
            if abs(comp_sum - balance) > EPS:
                issues.append(
                    {
                        "code": "COMPOSITION_MISMATCH",
                        "account": acct,
                        "message": f"成分之和 {comp_sum} != 余额 {balance}",
                    }
                )

        # 7) 供应商批次级闭合
        lot_closure = []
        for root, introduced in sorted(proj.root_introduced.items()):
            located = sum(
                (v for acct, comp in proj.composition.items() for r, v in comp.items() if r == root),
                Decimal(),
            )
            # 流出到外部（报废等）的成分也记录在 composition 的 ext 账户上
            ok = abs(located - introduced) <= EPS
            if not ok:
                issues.append(
                    {
                        "code": "SUPPLIER_LOT_NOT_CLOSED",
                        "supplier_lot": root,
                        "message": f"引入 {introduced}，可定位 {located}",
                    }
                )
            lot_closure.append(
                {"supplier_lot": root, "introduced": str(introduced), "located": str(located), "closed": ok}
            )

        return {
            "event_count": len(events),
            "head_seq": proj._events[-1].seq if proj._events else 0,
            "consistent": not issues,
            "issue_count": len(issues),
            "issues": issues,
            "totals": {
                "introduced_from_suppliers": str(-supplier_total),
                "internal_on_hand": str(internal_total),
                "boundary_outflows": str(boundary_total),
            },
            "supplier_lot_closure": lot_closure,
        }


def _issue(event: Event, code: str, message: str) -> dict:
    return {"code": code, "seq": event.seq, "event_id": event.event_id, "message": message}
