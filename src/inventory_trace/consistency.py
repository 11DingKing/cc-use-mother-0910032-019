"""一致性检查：独立重放、恒等式核对与哈希链审计。

检查内容：

1. **哈希链**：重算每条事件的 SHA-256 链，检测外部篡改或删除；
2. **投影一致性**：从空状态重放全部事件，与在线投影表逐行比对；
3. **逐事件守恒**：内部流转事件两侧总量恒等，消耗/更正只允许单侧；
4. **库存恒等式**：
   ``累计收货 + 盘盈 + 退回 = 当前库存 + 累计消耗 + 盘亏``
   （已撤销事件两侧都不计入，等式仍成立）；
5. **非负库存**：重放过程任意批次数量永不为负（投影器强制，再独立核对）；
6. **补偿完整性**：``supersedes`` 必须指向存在、有效且可撤销的事件；
7. **血缘可达性**：每个非空批次都能追溯到至少一个供应商批号或工单。
"""
from __future__ import annotations

from collections import defaultdict
from decimal import Decimal

from .lineage import SUPPLIER_PREFIX, WORK_ORDER_PREFIX, LineageGraph
from .models import BALANCED_EVENTS
from .store import EXTERNAL, EventStore


def run_checks(store: EventStore, conn) -> dict:
    events = store.load_events(conn)
    anomalies: list[str] = []

    # 1. 哈希链
    anomalies.extend(store.verify_hash_chain(conn))

    # 2. 投影一致性 + 5. 非负库存（重放器内部强制）
    try:
        replayed = store.replay(conn)
    except Exception as exc:  # CorruptedLedgerError
        anomalies.append(f"重放失败：{exc}")
        replayed = {}
    online = store.read_projection(conn)
    if set(replayed) != set(online):
        only_online = sorted(set(online) - set(replayed))
        only_replay = sorted(set(replayed) - set(online))
        if only_online:
            anomalies.append(f"投影多出的批次：{only_online}")
        if only_replay:
            anomalies.append(f"投影缺失的批次：{only_replay}")
    for bid in sorted(set(replayed) & set(online)):
        a, b = replayed[bid], online[bid]
        for field in ("sku", "supplier_lot", "quantity", "location", "consumed"):
            if getattr(a, field) != getattr(b, field):
                anomalies.append(
                    f"批次 {bid} 的 {field} 不一致：重放={getattr(a, field)!r} "
                    f"投影={getattr(b, field)!r}"
                )

    # 3. 逐事件守恒 + 6. 补偿完整性
    by_seq = {e.seq: e for e in events}
    active = [e for e in events if e.supersedes is None and e.reversed_by is None]
    for event in events:
        raw_in = sum((l.quantity for l in event.inputs), Decimal(0))
        raw_out = sum((l.quantity for l in event.outputs), Decimal(0))
        if event.event_type in BALANCED_EVENTS and raw_in != raw_out:
            anomalies.append(
                f"事件 #{event.seq} 数量不守恒：{raw_in} != {raw_out}"
            )
        for line in (*event.inputs, *event.outputs):
            if line.quantity <= 0:
                anomalies.append(
                    f"事件 #{event.seq} 存在非正数量行：{line.batch_id} {line.quantity}"
                )
        if event.supersedes is not None:
            target = by_seq.get(event.supersedes)
            if target is None:
                anomalies.append(f"事件 #{event.seq} 指向不存在的撤销目标 #{event.supersedes}")
            elif target.supersedes is not None:
                anomalies.append(f"事件 #{event.seq} 撤销了另一个补偿事件 #{event.supersedes}")
            elif target.reversed_by not in (None, event.seq):
                anomalies.append(f"事件 #{event.seq} 的撤销目标状态异常")

    # 独立重算非负库存（不依赖 Projection）
    balances: dict[str, Decimal] = defaultdict(lambda: Decimal(0))
    for event in active:
        for line in event.inputs:
            if line.batch_id != EXTERNAL:
                balances[line.batch_id] -= line.quantity
        for line in event.outputs:
            if line.batch_id != EXTERNAL:
                balances[line.batch_id] += line.quantity
        for line in event.inputs:
            if line.batch_id != EXTERNAL and balances[line.batch_id] < 0:
                anomalies.append(
                    f"事件 #{event.seq} 后批次 {line.batch_id} 出现负库存"
                )

    # 4. 库存恒等式
    received = returned = gains = Decimal(0)
    consumed = shorts = Decimal(0)
    on_hand = Decimal(0)
    for event in active:
        ext_in = sum(
            (l.quantity for l in event.inputs if l.batch_id == EXTERNAL), Decimal(0)
        )
        ext_out = sum(
            (l.quantity for l in event.outputs if l.batch_id == EXTERNAL), Decimal(0)
        )
        if event.event_type == "receive":
            received += ext_in
        elif event.event_type == "return":
            returned += ext_in
        elif event.event_type == "consume":
            consumed += ext_out
        elif event.event_type == "correction":
            reason = event.payload.get("reason")
            if reason == "gain":
                gains += sum((l.quantity for l in event.outputs), Decimal(0))
            elif reason == "short":
                shorts += sum((l.quantity for l in event.inputs), Decimal(0))
    for qty in balances.values():
        on_hand += qty
    identity_lhs = received + returned + gains
    identity_rhs = on_hand + consumed + shorts
    if identity_lhs != identity_rhs:
        anomalies.append(
            "库存恒等式不成立："
            f"收货{received}+退回{returned}+盘盈{gains}={identity_lhs} "
            f"!= 库存{on_hand}+消耗{consumed}+盘亏{shorts}={identity_rhs}"
        )

    # 7. 血缘可达性
    graph = LineageGraph(events)
    for bid, state in replayed.items():
        if state.quantity > 0 and not _reaches_boundary(graph, bid):
            anomalies.append(f"批次 {bid} 有库存但无法追溯到供应商批次或工单来源")

    return {
        "ok": not anomalies,
        "event_count": len(events),
        "active_event_count": len(active),
        "batch_count": len(online),
        "identity": {
            "received": str(received),
            "returned": str(returned),
            "gains": str(gains),
            "on_hand": str(on_hand),
            "consumed": str(consumed),
            "shorts": str(shorts),
            "balanced": identity_lhs == identity_rhs,
        },
        "anomalies": anomalies,
    }


def _reaches_boundary(graph: LineageGraph, node: str) -> bool:
    """向上游是否能到达供应商/工单边界节点。"""
    seen: set[str] = set()
    stack = [node]
    while stack:
        current = stack.pop()
        for edge in graph.in_edges.get(current, ()):
            src = edge["from"]
            if src.startswith((SUPPLIER_PREFIX, WORK_ORDER_PREFIX)):
                return True
            if src not in seen:
                seen.add(src)
                stack.append(src)
    return False
