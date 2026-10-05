"""批次血缘图与双向追踪。

由不可变事件构建有向血缘图（外部供应商批次 / 生产工单为图的边界节点）：

    供应商批号 --receive--> 批次 --split/merge--> 批次 --consume--> 生产工单

已撤销的事件（``reversed_by`` 非空）及其补偿事件不参与血缘传播，
因此追溯结果与当前有效库存一致。
"""
from __future__ import annotations

import sqlite3
from collections import defaultdict, deque
from decimal import Decimal

from .models import Event
from .store import EXTERNAL, EventStore

SUPPLIER_PREFIX = "supplier:"
WORK_ORDER_PREFIX = "wo:"


def supplier_node(lot: str) -> str:
    return SUPPLIER_PREFIX + lot


def work_order_node(wo: str) -> str:
    return WORK_ORDER_PREFIX + wo


class LineageGraph:
    def __init__(self, events: list[Event]) -> None:
        self.events = events
        self.by_seq = {e.seq: e for e in events}
        # 邻接表：node -> [{to, seq, type, quantity}]
        self.out_edges: dict[str, list[dict]] = defaultdict(list)
        self.in_edges: dict[str, list[dict]] = defaultdict(list)
        self._build()

    def _active(self, event: Event) -> bool:
        """事件未被撤销，且本身不是补偿事件。"""
        return event.reversed_by is None and event.supersedes is None

    def _add_edge(self, src: str, dst: str, event: Event, qty: Decimal) -> None:
        edge = {"from": src, "to": dst, "seq": event.seq,
                "type": event.event_type, "quantity": qty}
        self.out_edges[src].append(edge)
        self.in_edges[dst].append(edge)

    def _build(self) -> None:
        for event in self.events:
            if not self._active(event):
                continue
            etype = event.event_type
            if etype == "receive":
                ext = next((l for l in event.inputs if l.batch_id == EXTERNAL), None)
                out = next((l for l in event.outputs if l.batch_id != EXTERNAL), None)
                if ext and out and ext.external_ref:
                    self._add_edge(supplier_node(ext.external_ref), out.batch_id, event, out.quantity)
            elif etype == "return":
                ext = next((l for l in event.inputs if l.batch_id == EXTERNAL), None)
                out = next((l for l in event.outputs if l.batch_id != EXTERNAL), None)
                if ext and out and ext.external_ref:
                    self._add_edge(work_order_node(ext.external_ref), out.batch_id, event, out.quantity)
            elif etype == "split":
                parent = event.inputs[0].batch_id
                for line in event.outputs:
                    self._add_edge(parent, line.batch_id, event, line.quantity)
            elif etype == "move":
                # 仅部分移动会生成新批次，需要血缘边；全量移动批次不变
                if event.payload.get("partial"):
                    src = event.inputs[0].batch_id
                    for line in event.outputs:
                        self._add_edge(src, line.batch_id, event, line.quantity)
            elif etype == "merge":
                new = event.outputs[0].batch_id
                for line in event.inputs:
                    self._add_edge(line.batch_id, new, event, line.quantity)
            elif etype == "consume":
                ext = next((l for l in event.outputs if l.batch_id == EXTERNAL), None)
                if not ext or not ext.external_ref:
                    continue
                node = work_order_node(ext.external_ref)
                for line in event.inputs:
                    self._add_edge(line.batch_id, node, event, line.quantity)

    # ------------------------------------------------------------------ #
    # 遍历
    # ------------------------------------------------------------------ #

    def forward(self, roots: list[str]) -> set[str]:
        """正向收集全部可达批次节点；可穿透工单节点（覆盖余料退回）。"""
        reached: set[str] = set()
        seen: set[str] = set()
        queue = deque(roots)
        while queue:
            node = queue.popleft()
            for edge in self.out_edges.get(node, ()):
                dst = edge["to"]
                if dst in seen:
                    continue
                seen.add(dst)
                if not dst.startswith((SUPPLIER_PREFIX, WORK_ORDER_PREFIX)):
                    reached.add(dst)
                queue.append(dst)
        return reached

    def backward(self, roots: list[str]) -> set[str]:
        """反向收集全部可达批次节点；可穿透工单节点。"""
        reached: set[str] = set()
        seen: set[str] = set()
        queue = deque(roots)
        while queue:
            node = queue.popleft()
            for edge in self.in_edges.get(node, ()):
                src = edge["from"]
                if src in seen:
                    continue
                seen.add(src)
                if not src.startswith((SUPPLIER_PREFIX, WORK_ORDER_PREFIX)):
                    reached.add(src)
                queue.append(src)
        return reached

    def created_quantity(self, node: str) -> Decimal:
        """节点创建总量：批次为诞生事件并入量，工单节点为累计领入量。"""
        return sum(
            (e["quantity"] for e in self.in_edges.get(node, ())), Decimal(0)
        )

    def forward_shares(self, roots: list[str]) -> dict[str, Decimal]:
        """计算各下游节点中来自 roots 的源头物料数量（按创建量比例分摊）。

        与 :meth:`contributions_to` 对称：沿创建边正向传播，批次的当前库存
        与领料量再按 ``share / 创建总量`` 折算为源头归属量。
        """
        share: dict[str, Decimal] = defaultdict(lambda: Decimal(0))
        spread: dict[str, Decimal] = defaultdict(lambda: Decimal(0))
        queue: deque[str] = deque()
        for root in roots:
            share[root] = self.created_quantity(root)
            queue.append(root)
        while queue:
            node = queue.popleft()
            total = share[node]
            if total <= spread[node]:
                continue
            delta = total - spread[node]
            spread[node] = total
            created = self.created_quantity(node)
            if created <= 0:
                continue
            for edge in self.out_edges.get(node, ()):
                share[edge["to"]] += edge["quantity"] * delta / created
                queue.append(edge["to"])
        return dict(share)

    def contributions_to(self, target: str) -> dict[str, Decimal]:
        """计算边界来源（供应商批号/退回工单）对目标节点的按比例贡献量。

        每个批次只有一个"诞生事件"（收货 1 个来源、拆分 1 个父批次、
        合并 N 个来源、退回 1 个工单），沿创建边反向做增量传播：

        * 拆分：子批次的需求量全额上推给父批次；
        * 合并：按各来源并入数量占比分摊（视为均质混合）；
        * 收货/退回：需求量计入对应边界来源。

        以"已传播增量"驱动，能正确处理菱形血缘（同一祖先经多条路径、
        不同深度可达）。
        """
        contributions: dict[str, Decimal] = defaultdict(lambda: Decimal(0))
        demand: dict[str, Decimal] = defaultdict(lambda: Decimal(0))
        spread: dict[str, Decimal] = defaultdict(lambda: Decimal(0))
        queue: deque[str] = deque()
        for edge in self.in_edges.get(target, ()):
            demand[edge["from"]] += edge["quantity"]
            queue.append(edge["from"])

        while queue:
            node = queue.popleft()
            amount = demand[node]
            if amount <= spread[node]:
                continue
            delta = amount - spread[node]
            spread[node] = amount

            sources = self.in_edges.get(node, ())
            boundaries = [e for e in sources
                          if e["from"].startswith((SUPPLIER_PREFIX, WORK_ORDER_PREFIX))]
            batch_sources = [e for e in sources if e not in boundaries]
            if batch_sources:
                if len(batch_sources) == 1:
                    edge = batch_sources[0]
                    demand[edge["from"]] += min(delta, edge["quantity"])
                    queue.append(edge["from"])
                else:
                    weights = {e["from"]: e["quantity"] for e in batch_sources}
                    total = sum(weights.values(), Decimal(0))
                    for src, weight in weights.items():
                        demand[src] += delta * weight / total
                        queue.append(src)
            else:
                for edge in boundaries:
                    src = edge["from"]
                    wo_inputs = self.in_edges.get(src, ())
                    if src.startswith(WORK_ORDER_PREFIX) and wo_inputs:
                        # 余料来自有领料记录的工单：视为领料混合物，
                        # 按该工单的领料占比继续上溯
                        amount = min(delta, edge["quantity"])
                        total = sum((e["quantity"] for e in wo_inputs), Decimal(0))
                        for e in wo_inputs:
                            demand[e["from"]] += amount * e["quantity"] / total
                            queue.append(e["from"])
                    else:
                        # 供应商批号，或无领料记录的工单退回（外部来源）
                        contributions[src] += min(delta, edge["quantity"])
        return dict(contributions)


class TraceService:
    def __init__(self, store: EventStore) -> None:
        self.store = store

    def _load(self, conn: sqlite3.Connection) -> tuple[list[Event], LineageGraph, dict]:
        events = self.store.load_events(conn)
        projection = self.store.read_projection(conn)
        return events, LineageGraph(events), projection

    # ------------------------------------------------------------------ #
    # 正向召回：供应商批号 -> 当前库位 + 生产消耗
    # ------------------------------------------------------------------ #

    def forward_by_supplier_lot(
        self, supplier_lot: str, *, sku: str | None = None,
    ) -> dict:
        root = supplier_node(supplier_lot)
        conn = connect_readonly(self.store)
        try:
            events, graph, projection = self._load(conn)
            root_batches = [e["to"] for e in graph.out_edges.get(root, ())
                            if not e["to"].startswith(WORK_ORDER_PREFIX)]
            if not root_batches:
                return {
                    "direction": "forward",
                    "supplier_lot": supplier_lot,
                    "found": False,
                    "root_batches": [],
                    "current_locations": [],
                    "depleted_batches": [],
                    "total_on_hand": "0",
                    "total_attributed_on_hand": "0",
                    "total_consumed": "0",
                    "total_attributed_consumed": "0",
                    "consumptions": [],
                    "events": [],
                }
            if sku:
                root_batches = [b for b in root_batches
                                if projection.get(b) and projection[b].sku == sku]
                if not root_batches:
                    return self._forward_report(
                        supplier_lot, [], {}, events, graph, projection)
            shares = graph.forward_shares(root_batches)
            return self._forward_report(supplier_lot, root_batches, shares, events, graph, projection)
        finally:
            conn.close()

    def _forward_report(self, lot, root_batches, shares, events, graph, projection) -> dict:
        # 带权份额：合并后的批次按诞生时各来源占比折算源头物料量
        shares = graph.forward_shares(root_batches)

        def attributed(bid: str, physical: Decimal) -> Decimal:
            created = graph.created_quantity(bid)
            share = shares.get(bid, Decimal(0))
            if created <= 0 or share <= 0:
                return Decimal(0)
            # 按占比折算，并以源头份额为上限（盘盈等无来源增量不归供应商）
            return min(physical * share / created, share)

        on_hand: list[dict] = []
        depleted: list[dict] = []
        for bid in sorted(shares):
            state = projection.get(bid)
            if state is None:
                continue
            physical = state.quantity
            item = {
                "batch_id": bid,
                "sku": state.sku,
                "location": state.location,
                "quantity": _q(physical),
                "attributed_quantity": _q(attributed(bid, physical)),
                "consumed": state.consumed,
            }
            (on_hand if physical > 0 else depleted).append(item)

        consumptions: list[dict] = []
        total_consumed = Decimal(0)
        total_attributed_consumed = Decimal(0)
        for event in events:
            if event.event_type != "consume" or not graph._active(event):
                continue
            touched = [l for l in event.inputs if l.batch_id in shares]
            if not touched:
                continue
            qty = sum((l.quantity for l in touched), Decimal(0))
            attr = sum((attributed(l.batch_id, l.quantity) for l in touched), Decimal(0))
            total_consumed += qty
            total_attributed_consumed += attr
            consumptions.append({
                "seq": event.seq,
                "work_order": event.payload.get("work_order"),
                "product": event.payload.get("product"),
                "occurred_at": event.occurred_at,
                "quantity": _q(qty),
                "attributed_quantity": _q(attr),
                "batches": [l.batch_id for l in touched],
            })

        total_on_hand_attr = sum(
            (Decimal(b["attributed_quantity"]) for b in on_hand), Decimal(0)
        )
        path_events = [
            e for e in events
            if graph._active(e)
            and any(l.batch_id in shares for l in (*e.inputs, *e.outputs))
            and e.event_type in {"receive", "split", "merge", "consume", "return", "move"}
        ]
        return {
            "direction": "forward",
            "found": True,
            "supplier_lot": lot,
            "root_batches": sorted(set(root_batches) & set(shares)) or sorted(root_batches),
            "current_locations": on_hand,
            "depleted_batches": depleted,
            "total_on_hand": _q(sum((Decimal(b["quantity"]) for b in on_hand), Decimal(0))),
            "total_attributed_on_hand": _q(total_on_hand_attr),
            "total_consumed": _q(total_consumed),
            "total_attributed_consumed": _q(total_attributed_consumed),
            "consumptions": consumptions,
            "events": [_event_brief(e) for e in path_events],
        }

    # ------------------------------------------------------------------ #
    # 反向追溯：生产工单/成品 -> 供应商批号
    # ------------------------------------------------------------------ #

    def backward_by_work_order(self, work_order: str) -> dict:
        root = work_order_node(work_order)
        conn = connect_readonly(self.store)
        try:
            events, graph, projection = self._load(conn)
            consumed_batches = [e["from"] for e in graph.in_edges.get(root, ())]
            if not consumed_batches:
                return {
                    "direction": "backward",
                    "work_order": work_order,
                    "found": False,
                    "supplier_lots": [],
                    "events": [],
                }
            reached = graph.backward(consumed_batches) | set(consumed_batches)
            return self._backward_report(work_order, consumed_batches, reached, events, graph, projection)
        finally:
            conn.close()

    def _backward_report(self, work_order, consumed_batches, reached, events, graph, projection) -> dict:
        # 以工单节点为目标做带权贡献量传播：领料多少就向上游分摊多少，
        # 合并按占比分摊，而非简单累加整批收货量
        contributions = graph.contributions_to(work_order_node(work_order))
        supplier_lots: list[dict] = []
        return_sources: list[dict] = []
        for node, qty in sorted(contributions.items()):
            entry = {"quantity": _q(qty)}
            if node.startswith(SUPPLIER_PREFIX):
                supplier_lots.append({"supplier_lot": node[len(SUPPLIER_PREFIX):], **entry})
            elif node.startswith(WORK_ORDER_PREFIX):
                return_sources.append({"work_order": node[len(WORK_ORDER_PREFIX):], **entry})

        path_events = [
            e for e in events
            if graph._active(e)
            and any(l.batch_id in reached for l in (*e.inputs, *e.outputs))
            and e.event_type in {"receive", "split", "merge", "consume", "return", "move"}
        ]
        return {
            "direction": "backward",
            "found": True,
            "work_order": work_order,
            "consumed_batches": sorted(set(consumed_batches)),
            "supplier_lots": supplier_lots,
            "returned_from_work_orders": return_sources,
            "events": [_event_brief(e) for e in path_events],
        }

    def trace_batch(self, batch_id: str) -> dict:
        """以任意批次为中心的双向血缘（召回定位用）。"""
        conn = connect_readonly(self.store)
        try:
            events, graph, projection = self._load(conn)
            if batch_id not in projection and not any(
                l.batch_id == batch_id
                for e in events for l in (*e.inputs, *e.outputs)
            ):
                return {"found": False, "batch_id": batch_id}
            forward_set = graph.forward([batch_id]) | {batch_id}
            backward_set = graph.backward([batch_id]) | {batch_id}

            supplier_lots: set[str] = set()
            work_orders: set[str] = set()
            for bid in backward_set:
                for edge in graph.in_edges.get(bid, ()):
                    if edge["from"].startswith(SUPPLIER_PREFIX):
                        supplier_lots.add(edge["from"][len(SUPPLIER_PREFIX):])
            for bid in forward_set:
                for edge in graph.out_edges.get(bid, ()):
                    if edge["to"].startswith(WORK_ORDER_PREFIX):
                        work_orders.add(edge["to"][len(WORK_ORDER_PREFIX):])

            state = projection.get(batch_id)
            return {
                "found": True,
                "batch_id": batch_id,
                "current": state.to_dict() if state else None,
                "upstream_supplier_lots": sorted(supplier_lots),
                "downstream_work_orders": sorted(work_orders),
                "lineage_batches": sorted((forward_set | backward_set) - {batch_id}),
                "events": [
                    _event_brief(e) for e in events
                    if any(l.batch_id == batch_id for l in (*e.inputs, *e.outputs))
                ],
            }
        finally:
            conn.close()


def connect_readonly(store: EventStore) -> sqlite3.Connection:
    return store.open()


def _q(value: Decimal) -> str:
    """去掉带权分摊产生的无效尾差（保留 9 位小数）。"""
    quantized = value.quantize(Decimal("0.000000001"))
    return format(quantized, "f")


def _event_brief(event: Event) -> dict:
    return {
        "seq": event.seq,
        "type": event.event_type,
        "occurred_at": event.occurred_at,
        "actor": event.actor,
        "inputs": [l.to_row() for l in event.inputs],
        "outputs": [l.to_row() for l in event.outputs],
        "payload": event.payload,
        "reversed_by": event.reversed_by,
    }
