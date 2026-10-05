"""质量召回端到端演示：收货 -> 拆包 -> 合并 -> 领料 -> 双向追溯 -> 一致性检查。

用法：
    python3 tools/recall_demo.py [数据库路径]   # 默认使用内存库
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from inventory_trace.consistency import run_checks
from inventory_trace.lineage import TraceService
from inventory_trace.service import InventoryService


def main() -> int:
    db_path = sys.argv[1] if len(sys.argv) > 1 else ":memory:"
    svc = InventoryService(db_path)

    print("=" * 70)
    print("场景：供应商通知批号 SUP-LOT-2026-09 存在质量缺陷，需要召回")
    print("=" * 70)

    # 1. 收货：两批原料，其中问题批号 200 件
    r1 = svc.receive(sku="RAW-A", quantity="200", location="待检区-01",
                     supplier_lot="SUP-LOT-2026-09", actor="仓储管理员",
                     note="问题批号")
    raw1 = r1["outputs"][0]["batch_id"]
    r2 = svc.receive(sku="RAW-A", quantity="100", location="待检区-02",
                     supplier_lot="SUP-LOT-2026-08")
    raw2 = r2["outputs"][0]["batch_id"]
    print(f"收货：{raw1}（200，问题批号）、{raw2}（100，正常批号）")

    # 2. 拆包：问题批号拆成 120 + 80，分别入原料仓不同库位
    s1 = svc.split(batch_id=raw1, children=[
        {"quantity": "120", "location": "原料仓-A1"},
        {"quantity": "80", "location": "原料仓-A2"},
    ])
    part1, part2 = [c["batch_id"] for c in s1["payload"]["children"]]
    print(f"拆包：{raw1} -> {part1}(120) + {part2}(80)")

    # 3. 合并：80 件问题料与 100 件正常料混合（批次血缘保留双方来源）
    m1 = svc.merge(sources=[
        {"batch_id": part2}, {"batch_id": raw2}], location="配料区-B1")
    mixed = m1["outputs"][0]["batch_id"]
    print(f"合并：{part2}(80) + {raw2}(100) -> {mixed}(180)，血缘保留双来源")

    # 4. 领料：两笔生产工单消耗
    c1 = svc.consume(allocations=[{"batch_id": part1, "quantity": "120"}],
                     work_order="WO-5001", product="成品-P1", actor="仓储管理员")
    c2 = svc.consume(allocations=[{"batch_id": mixed, "quantity": "90"}],
                     work_order="WO-5002", product="成品-P2")
    print(f"领料：WO-5001 消耗 {part1} 120；WO-5002 消耗 {mixed} 90")

    # 5. 余料退回
    ret = svc.return_stock(work_order="WO-5002", sku="RAW-A", quantity="5",
                           location="退料区-C1", supplier_lot=None)
    returned = ret["outputs"][0]["batch_id"]
    print(f"退回：WO-5002 余料 5 -> {returned}")

    print()
    print("-" * 70)
    print("【正向召回追踪】SUP-LOT-2026-09 -> 当前库位 + 生产消耗")
    print("-" * 70)
    tracer = TraceService(svc.store)
    fwd = tracer.forward_by_supplier_lot("SUP-LOT-2026-09")
    print(json.dumps({
        "在库位置": fwd["current_locations"],
        "已耗尽批次": fwd["depleted_batches"],
        "归属在库总量": fwd["total_attributed_on_hand"],
        "生产消耗记录": fwd["consumptions"],
        "归属消耗总量": fwd["total_attributed_consumed"],
    }, ensure_ascii=False, indent=2))

    print("-" * 70)
    print("【反向追踪】WO-5002（成品 P2）-> 供应商批号")
    print("-" * 70)
    bwd = tracer.backward_by_work_order("WO-5002")
    print(json.dumps(bwd["supplier_lots"], ensure_ascii=False, indent=2))
    print("（合并批次按 80:100 占比分摊：90 件消耗中问题批号占 40 件）")

    print("-" * 70)
    print("【一致性检查】")
    print("-" * 70)
    conn = svc.store.open()
    try:
        report = run_checks(svc.store, conn)
    finally:
        conn.close()
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
