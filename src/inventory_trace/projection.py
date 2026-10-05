"""事件重放投影。

重放不可变事件台账，得到三类只读视图：

1. ``balances``：账户当前余额（内部账户即实际库存/在制/成品数量）；
2. ``composition``：每个账户的"供应商批次成分向量"——
   按比例（proportional allocation）在拆分/合并/移动间传播，
   任意账户的成分向量之和恒等于其余额；
3. ``edges``：批次来源链有向边（账户 -> 账户，带事件与数量），
   用于正向/反向遍历批次流转路径。

成分在"多入多出"事件上按数量比例混合，是物料混合后的标准记账约定；
一成一变（1:1 移动、拆包）时结果精确无损。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from .models import (
    Event,
    EventType,
    QUANT,
    account_lot,
    is_internal,
    parse_fg_account,
    parse_stage_account,
    parse_stock_account,
)

UNKNOWN_ROOT = "external:unknown"


def root_of_external(account: str) -> str:
    """外部账户（供应商/调整/损耗）对应的成分根。"""
    prefix = "ext:supplier:"
    if account.startswith(prefix):
        return account[len(prefix) :]
    return f"external:{account[4:]}" if account.startswith("ext:") else f"external:{account}"


@dataclass
class LotInfo:
    lot_id: str
    sku: Optional[str]
    created_seq: int
    created_event_id: str
    origins: list[str] = field(default_factory=list)  # 创建时的供应商批次根
    merged: bool = False


@dataclass
class FlowEdge:
    seq: int
    event_id: str
    event_type: str
    source: str
    target: str
    lot_from: Optional[str]
    lot_to: Optional[str]
    qty: Decimal


@dataclass
class EdgeRef:
    seq: int
    event_id: str
    event_type: str
    qty: Decimal


class Projection:
    def __init__(self, events: list[Event] | None = None):
        self.balances: dict[str, Decimal] = {}
        self.composition: dict[str, dict[str, Decimal]] = {}
        self.lot_info: dict[str, LotInfo] = {}
        self.edges: list[FlowEdge] = []
        self.root_introduced: dict[str, Decimal] = {}
        self._events: list[Event] = []
        for event in events or []:
            self.apply(event)

    # ------------------------------------------------------------ 重放

    def apply(self, event: Event) -> None:
        # 1) 余额过账
        before: dict[str, Decimal] = {}
        for leg in event.legs:
            before[leg.account] = self.balances.get(leg.account, Decimal())
            self.balances[leg.account] = before[leg.account] + leg.delta

        sources = [l for l in event.legs if l.delta < 0]
        dests = [l for l in event.legs if l.delta > 0]
        total_out = -sum((l.delta for l in sources), Decimal())

        # 2) 汇总"源"的成分向量（部分出库按余额比例取出）
        src_roots: dict[str, Decimal] = {}
        for leg in sources:
            amount = -leg.delta
            acct = leg.account
            if is_internal(acct):
                balance_before = before[acct]
                comp = self.composition.get(acct, {})
                if balance_before > 0:
                    ratio = amount / balance_before
                    shares: dict[str, Decimal] = {}
                    for root, value in comp.items():
                        share = (value * ratio).quantize(QUANT)
                        shares[root] = share
                        src_roots[root] = src_roots.get(root, Decimal()) + share
                    # 舍入残差并入最大成分，保证取出量精确等于 amount
                    remainder = amount - sum(shares.values(), Decimal())
                    if remainder:
                        top = max(shares, key=shares.get)
                        shares[top] += remainder
                        src_roots[top] += remainder
                    for root, share in shares.items():
                        comp[root] = comp[root] - share
                    # 清理近零项
                    self.composition[acct] = {
                        r: v.quantize(QUANT) for r, v in comp.items() if v != 0
                    }
            else:
                root = root_of_external(acct)
                src_roots[root] = src_roots.get(root, Decimal()) + amount
                self.root_introduced[root] = self.root_introduced.get(root, Decimal()) + amount

        # 3) 按数量比例分配到各"目的地"（含损耗等外部账户，便于损失核算）
        if total_out > 0:
            for leg in dests:
                amount = leg.delta
                bucket = self.composition.setdefault(leg.account, {})
                allocations: dict[str, Decimal] = {}
                for root, value in src_roots.items():
                    share = (value * (amount / total_out)).quantize(QUANT)
                    allocations[root] = share
                # 舍入残差并入最大成分，保证成分之和精确等于入账量
                remainder = amount - sum(allocations.values(), Decimal())
                if remainder:
                    top = max(allocations, key=allocations.get)
                    allocations[top] += remainder
                for root, share in allocations.items():
                    if share:
                        bucket[root] = bucket.get(root, Decimal()) + share

        # 4) 批次身份登记
        self._register_lots(event, src_roots)

        # 5) 来源链边（源->目的地全连接，按比例赋量）
        for s in sources:
            for d in dests:
                share = (s.delta.copy_abs() * d.delta / total_out).quantize(QUANT) if total_out > 0 else Decimal()
                self.edges.append(
                    FlowEdge(
                        seq=event.seq,
                        event_id=event.event_id,
                        event_type=event.event_type.value,
                        source=s.account,
                        target=d.account,
                        lot_from=account_lot(s.account),
                        lot_to=account_lot(d.account),
                        qty=share,
                    )
                )

        self._events.append(event)

    def _register_lots(self, event: Event, src_roots: dict[str, Decimal]) -> None:
        p = event.payload
        if event.event_type is EventType.RECEIVE:
            for leg in event.legs:
                parsed = parse_stock_account(leg.account)
                if parsed and leg.delta > 0:
                    lot_id, _ = parsed
                    self.lot_info[lot_id] = LotInfo(
                        lot_id=lot_id,
                        sku=p.get("sku"),
                        created_seq=event.seq,
                        created_event_id=event.event_id,
                        origins=[f"{p.get('supplier_id')}:{p.get('supplier_lot')}"],
                    )
        elif event.event_type is EventType.SPLIT:
            parent = p.get("source_lot")
            parent_info = self.lot_info.get(parent) if parent else None
            for child in p.get("children", []):
                cid = child["lot_id"]
                self.lot_info[cid] = LotInfo(
                    lot_id=cid,
                    sku=(child.get("sku") or (parent_info.sku if parent_info else None)),
                    created_seq=event.seq,
                    created_event_id=event.event_id,
                    origins=list(parent_info.origins) if parent_info else list(src_roots),
                )
        elif event.event_type is EventType.MERGE:
            target = p.get("target_lot")
            if target:
                origins = list(src_roots.keys())
                self.lot_info[target] = LotInfo(
                    lot_id=target,
                    sku=p.get("sku"),
                    created_seq=event.seq,
                    created_event_id=event.event_id,
                    origins=origins,
                    merged=True,
                )

    # ------------------------------------------------------------ 查询

    def stock_positions(self) -> list[dict]:
        rows = []
        for acct, balance in self.balances.items():
            parsed = parse_stock_account(acct)
            if parsed and balance > 0:
                lot_id, location = parsed
                rows.append(
                    {
                        "lot_id": lot_id,
                        "location": location,
                        "quantity": str(balance),
                        "sku": (self.lot_info.get(lot_id).sku if lot_id in self.lot_info else None),
                        "composition": {r: str(v) for r, v in self.composition.get(acct, {}).items()},
                    }
                )
        return sorted(rows, key=lambda x: (x["location"], x["lot_id"]))

    def accounts_by_prefix(self, prefix: str) -> dict[str, Decimal]:
        return {a: b for a, b in self.balances.items() if a.startswith(prefix)}

    def root_locations(self, root: str) -> dict[str, Decimal]:
        """该供应商批次成分当前分布在所有账户中的数量。"""
        out: dict[str, Decimal] = {}
        for acct, comp in self.composition.items():
            value = comp.get(root)
            if value and value > 0:
                out[acct] = value
        return out

    def composition_of(self, account: str) -> dict[str, Decimal]:
        return dict(self.composition.get(account, {}))
