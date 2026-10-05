"""质量召回场景端到端演示（可直接运行：python3 tools/demo_recall.py）。

场景：
1. 供应商 SUP-ALPHA 批次 SL-20260928-07 到货 1000kg 铝材（同时到货另一合格批次）；
2. 拆包、移动、与其他批次合并、领料、退料、投入成品 FG-AX-260、部分报废；
3. 质量工程师收到召回通知，一键输出受影响记录与当前位置；
4. 演示撤销（补偿事件）与盘点更正；
5. 输出台账一致性检查结果。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from inventory_trace import (  # noqa: E402
    ConsistencyChecker,
    EventStore,
    InventoryService,
    TraceEngine,
)

BAD_SUPPLIER = "SUP-ALPHA"
BAD_LOT = "SL-20260928-07"


def main() -> None:
    store = EventStore(":memory:")
    svc = InventoryService(store)
    tracer = TraceEngine(store)
    checker = ConsistencyChecker(store)

    # 1) 收货：问题批次 1000，合格批次 500
    bad = svc.receive(supplier_id=BAD_SUPPLIER, supplier_lot=BAD_LOT, quantity=1000,
                      location="RAW-A", sku="ALU-6061", actor="仓储管理员", lot_id="ALU-BAD")
    good = svc.receive(supplier_id="SUP-BETA", supplier_lot="SL-OK-01", quantity=500,
                       location="RAW-A", sku="ALU-6061", actor="仓储管理员", lot_id="ALU-GOOD")
    print(f"[收货] 问题批次 {bad.payload['lot_id']} 1000kg @ RAW-A；合格批次 {good.payload['lot_id']} 500kg")

    # 2) 移动 200kg 到待检区
    svc.move(lot_id="ALU-BAD", quantity=200, from_location="RAW-A", to_location="QC-HOLD",
             actor="仓储管理员")
    # 拆包：RAW-A 余 800 -> 300 + 500
    svc.split(source_lot="ALU-BAD", location="RAW-A", actor="仓储管理员",
              children=[{"lot_id": "ALU-BAD-A", "quantity": 300},
                        {"lot_id": "ALU-BAD-B", "quantity": 500}])
    # 合并：BAD-B 500 + GOOD 400 -> ALU-MIX 900
    svc.merge(sources=[{"lot_id": "ALU-BAD-B", "location": "RAW-A", "quantity": 500},
                       {"lot_id": "ALU-GOOD", "location": "RAW-A", "quantity": 400}],
              target_lot="ALU-MIX", target_location="PREP-1", actor="仓储管理员")
    # 领料：BAD-A 300 全领、MIX 中 540 投工单 WO-9527
    svc.issue(lot_id="ALU-BAD-A", quantity=300, from_location="RAW-A",
              order_id="WO-9527", actor="仓储管理员")
    svc.issue(lot_id="ALU-MIX", quantity=540, from_location="PREP-1",
              order_id="WO-9527", actor="仓储管理员")
    # 退料 20
    svc.return_material(order_id="WO-9527", lot_id="ALU-BAD-A", quantity=20,
                        to_location="RAW-A", actor="仓储管理员")
    # 消耗：BAD-A 280 全部成品；MIX 540 = 成品 530 + 报废 10
    svc.consume(order_id="WO-9527", lot_id="ALU-BAD-A", quantity=280,
                fg_batch="FG-AX-260", actor="生产操作工")
    svc.consume(order_id="WO-9527", lot_id="ALU-MIX", quantity=540,
                fg_batch="FG-AX-260", fg_quantity=530, scrap_quantity=10,
                actor="生产操作工")
    print("[流转] 拆包/合并/领料/退料/消耗完成，产出成品批次 FG-AX-260")

    # 3) 召回：正向定位 + 受影响记录
    print("\n================ 质量召回报告 ================")
    report = tracer.recall_report(BAD_SUPPLIER, BAD_LOT)
    print(f"问题供应商批次：{BAD_SUPPLIER}/{BAD_LOT}，引入 {report['introduced_quantity']}kg")
    print(f"受影响记录：收货 {len(report['records']['receives'])}、移动 {len(report['records']['moves'])}、"
          f"拆包 {len(report['records']['splits'])}、合并 {len(report['records']['merges'])}、"
          f"领料 {len(report['records']['issues'])}、退料 {len(report['records']['returns'])}、"
          f"成品消耗 {len(report['records']['consumptions'])}")
    print("当前库存位置：", json.dumps(report["current_stock"], ensure_ascii=False))
    print("工单在制：", json.dumps(report["in_production_stage"], ensure_ascii=False))
    print("已入成品：", json.dumps(report["in_finished_goods"], ensure_ascii=False))
    print("报废/流失：", json.dumps(report["consumed_or_lost"], ensure_ascii=False))
    print(f"数量闭合：{report['located_quantity']} / {report['introduced_quantity']} -> "
          f"{'通过' if report['accounted'] else '不通过'}")

    # 反向：成品反查供应商批次
    back = tracer.trace_backward(fg_batch="FG-AX-260")
    print("\n成品 FG-AX-260 的供应商批次构成：")
    for item in back["supplier_origins"]:
        print(f"  - {item['supplier_id']}/{item['supplier_lot']}: {item['quantity']}kg")

    # 4) 补偿：撤销对问题批次的一次错误移动（QC-HOLD 的 200 尚未下游流转）
    move_event = report["records"]["moves"][0]
    svc.reverse(event_id=move_event["event_id"], reason="召回冻结，库位记录撤销",
                actor="质量工程师")
    # 盘点更正：RAW-A 的 ALU-BAD 退回件盘亏 1
    svc.correct(reason="召回盘点盘亏 1kg", reference="CC-20261005",
                legs=[{"account": "stock:ALU-BAD-A@RAW-A", "delta": -1},
                      {"account": "ext:adjustment", "delta": 1}],
                actor="质量工程师")
    print("\n[补偿] 已追加 REVERSAL 与 CORRECTION 事件（原事件保留不变）")

    # 5) 一致性检查
    result = checker.check()
    print("\n================ 一致性检查 ================")
    print(json.dumps(result["totals"], ensure_ascii=False, indent=2))
    print(f"供应商批次闭合：{len(result['supplier_lot_closure'])} 个，"
          f"全部闭合={all(x['closed'] for x in result['supplier_lot_closure'])}")
    print(f"事件总数 {result['event_count']}，问题数 {result['issue_count']}，"
          f"结论：{'一致 ✓' if result['consistent'] else '不一致 ✗'}")


if __name__ == "__main__":
    main()
