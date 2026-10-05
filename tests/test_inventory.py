"""库存批次全链追溯后端测试。"""
from __future__ import annotations

import json
import sys
import threading
import unittest
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from inventory_trace import (  # noqa: E402
    ConsistencyChecker,
    EventStore,
    InventoryService,
    TraceEngine,
    create_server,
)
from inventory_trace.models import QuantityError, TracebackError, ValidationError  # noqa: E402


def make_service():
    store = EventStore(":memory:")
    return store, InventoryService(store), TraceEngine(store), ConsistencyChecker(store)


class LifecycleTest(unittest.TestCase):
    def setUp(self):
        self.store, self.svc, self.trace, self.checker = make_service()

    def test_full_lifecycle_and_conservation(self):
        # 收货 100
        r = self.svc.receive(supplier_id="SUP-A", supplier_lot="SL-1", quantity=100,
                             location="WH-RAW", sku="STEEL", actor="仓管员")
        # 移动 30 到待检区
        self.svc.move(lot_id=r.payload["lot_id"], quantity=30, from_location="WH-RAW",
                      to_location="WH-QC", actor="仓管员")
        # 拆包：70 留在原库位 -> 40 + 30 两个子批次
        self.svc.split(source_lot=r.payload["lot_id"], location="WH-RAW",
                       children=[{"lot_id": "C1", "quantity": 40}, {"lot_id": "C2", "quantity": 30}],
                       actor="仓管员")
        # 合并 C2(30) + QC区的原批次(30)
        m = self.svc.merge(sources=[
            {"lot_id": "C2", "location": "WH-RAW", "quantity": 30},
            {"lot_id": r.payload["lot_id"], "location": "WH-QC", "quantity": 30},
        ], target_lot="M1", target_location="WH-PREP", actor="仓管员")
        self.assertEqual(m.payload["total_quantity"], "60.000000")

        # 领料 40（C1 全部）+ 领料 25（M1）
        self.svc.issue(lot_id="C1", quantity=40, from_location="WH-RAW", order_id="WO-1", actor="班组长")
        self.svc.issue(lot_id="M1", quantity=25, from_location="WH-PREP", order_id="WO-1", actor="班组长")
        # 退 5 回库
        self.svc.return_material(order_id="WO-1", lot_id="C1", quantity=5,
                                 to_location="WH-RAW", actor="班组长")
        # 消耗：C1 35 全部成品；M1 25 = 20 成品 + 5 报废
        self.svc.consume(order_id="WO-1", lot_id="C1", quantity=35, fg_batch="FG-1", actor="操作工")
        self.svc.consume(order_id="WO-1", lot_id="M1", quantity=25, fg_batch="FG-1",
                         fg_quantity=20, scrap_quantity=5, actor="操作工")

        report = self.checker.check()
        self.assertTrue(report["consistent"], report["issues"])
        self.assertEqual(report["totals"]["introduced_from_suppliers"], "100.000000")
        # 现存：M1 剩 35 + C1 退回 5 = 40；成品 55；报废 5
        self.assertEqual(report["totals"]["internal_on_hand"], "95.000000")
        self.assertEqual(report["totals"]["boundary_outflows"], "5.000000")

    def test_consume_requires_balance_conservation(self):
        r = self.svc.receive(supplier_id="S", supplier_lot="L", quantity=10,
                             location="WH", actor="a")
        self.svc.issue(lot_id=r.payload["lot_id"], quantity=10, from_location="WH",
                       order_id="WO", actor="a")
        with self.assertRaises(QuantityError):
            # 8 != 7成品+2报废
            self.svc.consume(order_id="WO", lot_id=r.payload["lot_id"], quantity=8,
                             fg_batch="FG", fg_quantity=7, scrap_quantity=2, actor="a")

    def test_events_are_immutable_and_ordered(self):
        self.svc.receive(supplier_id="S", supplier_lot="L", quantity=10, location="WH", actor="a")
        self.svc.receive(supplier_id="S", supplier_lot="L2", quantity=5, location="WH", actor="a")
        events = self.store.load_all()
        self.assertEqual([e.seq for e in events], [1, 2])
        self.assertEqual(events[0].event_type.value, "RECEIVE")


class NegativeStockTest(unittest.TestCase):
    def setUp(self):
        self.store, self.svc, _, _ = make_service()

    def test_cannot_move_more_than_on_hand(self):
        r = self.svc.receive(supplier_id="S", supplier_lot="L", quantity=10,
                             location="WH", actor="a")
        with self.assertRaises(QuantityError):
            self.svc.move(lot_id=r.payload["lot_id"], quantity=11, from_location="WH",
                          to_location="X", actor="a")

    def test_cannot_split_more_than_on_hand(self):
        r = self.svc.receive(supplier_id="S", supplier_lot="L", quantity=10,
                             location="WH", actor="a")
        with self.assertRaises(QuantityError):
            self.svc.split(source_lot=r.payload["lot_id"], location="WH",
                           children=[{"lot_id": "C", "quantity": 10.000001}], actor="a")

    def test_cannot_issue_more_than_on_hand(self):
        r = self.svc.receive(supplier_id="S", supplier_lot="L", quantity=10,
                             location="WH", actor="a")
        with self.assertRaises(QuantityError):
            self.svc.issue(lot_id=r.payload["lot_id"], quantity=10.0001,
                           from_location="WH", order_id="WO", actor="a")

    def test_cannot_consume_more_than_staged(self):
        r = self.svc.receive(supplier_id="S", supplier_lot="L", quantity=10,
                             location="WH", actor="a")
        self.svc.issue(lot_id=r.payload["lot_id"], quantity=4, from_location="WH",
                       order_id="WO", actor="a")
        with self.assertRaises(QuantityError):
            self.svc.consume(order_id="WO", lot_id=r.payload["lot_id"], quantity=5,
                             fg_batch="FG", actor="a")

    def test_unknown_lot_rejected(self):
        with self.assertRaises(TracebackError):
            self.svc.issue(lot_id="GHOST", quantity=1, from_location="WH",
                           order_id="WO", actor="a")


class ConcurrencyTest(unittest.TestCase):
    def setUp(self):
        self.store, self.svc, _, _ = make_service()
        self.lot = self.svc.receive(
            supplier_id="S", supplier_lot="L", quantity=100, location="WH", actor="a"
        ).payload["lot_id"]

    def test_concurrent_issues_never_oversubscribe(self):
        results = {"ok": 0, "fail": 0}
        lock = threading.Lock()

        def worker(i):
            try:
                self.svc.issue(lot_id=self.lot, quantity=15, from_location="WH",
                               order_id=f"WO-{i % 3}", actor=f"w{i}")
                with lock:
                    results["ok"] += 1
            except QuantityError:
                with lock:
                    results["fail"] += 1

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(worker, range(10)))

        self.assertEqual(results["ok"], 6)   # 6*15 = 90
        self.assertEqual(results["fail"], 4)  # 其余被负库存保护拒绝
        report = ConsistencyChecker(self.store).check()
        self.assertTrue(report["consistent"], report["issues"])

    def test_idempotency_key_dedupes(self):
        def call():
            return self.svc.issue(lot_id=self.lot, quantity=10, from_location="WH",
                                  order_id="WO", actor="a", idempotency_key="KEY-1")

        first = call()
        second = call()
        self.assertEqual(first.event_id, second.event_id)
        self.assertEqual(self.store.head_seq(), 2)  # 收货 + 一次领料


class CompensationTest(unittest.TestCase):
    def setUp(self):
        self.store, self.svc, _, self.checker = make_service()
        self.r = self.svc.receive(supplier_id="S", supplier_lot="L", quantity=100,
                                  location="WH", actor="仓管员")
        self.lot = self.r.payload["lot_id"]
        self.iss = self.svc.issue(lot_id=self.lot, quantity=40, from_location="WH",
                                  order_id="WO", actor="班组长")

    def test_reversal_restores_balance(self):
        rev = self.svc.reverse(event_id=self.iss.event_id, reason="误领", actor="质量工程师")
        self.assertEqual(rev.event_type.value, "REVERSAL")
        # 原事件保留
        self.assertIsNotNone(self.store.get(self.iss.event_id))
        from inventory_trace.projection import Projection
        from inventory_trace.models import stock_account
        proj = Projection(self.store.load_all())
        self.assertEqual(proj.balances[stock_account(self.lot, "WH")], Decimal("100"))
        self.assertTrue(self.checker.check()["consistent"])

    def test_cannot_reverse_twice(self):
        self.svc.reverse(event_id=self.iss.event_id, reason="x", actor="a")
        with self.assertRaises(ValidationError):
            self.svc.reverse(event_id=self.iss.event_id, reason="y", actor="a")

    def test_reversal_blocked_when_downstream_consumed(self):
        self.svc.consume(order_id="WO", lot_id=self.lot, quantity=40, fg_batch="FG", actor="a")
        # 必须先撤销消耗，才能撤销领料
        with self.assertRaises(QuantityError):
            self.svc.reverse(event_id=self.iss.event_id, reason="误领", actor="a")

    def test_reverse_in_chain_order_succeeds(self):
        cons = self.svc.consume(order_id="WO", lot_id=self.lot, quantity=40,
                                fg_batch="FG", actor="a")
        self.svc.reverse(event_id=cons.event_id, reason="成品记录有误", actor="质量工程师")
        self.svc.reverse(event_id=self.iss.event_id, reason="误领", actor="质量工程师")
        self.assertTrue(self.checker.check()["consistent"])

    def test_correction_event_for_inventory_adjustment(self):
        # 盘亏 3：stock -3，ext:adjustment +3
        self.svc.correct(
            reason="盘点差异",
            legs=[
                {"account": f"stock:{self.lot}@WH", "delta": -3},
                {"account": "ext:adjustment", "delta": 3},
            ],
            actor="质量工程师",
        )
        report = self.checker.check()
        self.assertTrue(report["consistent"], report["issues"])

    def test_unbalanced_correction_rejected(self):
        with self.assertRaises(QuantityError):
            self.svc.correct(reason="x", legs=[{"account": f"stock:{self.lot}@WH", "delta": -3}],
                             actor="a")


class TraceTest(unittest.TestCase):
    def setUp(self):
        self.store, self.svc, self.trace, self.checker = make_service()
        # 两个供应商批次
        self.ra = self.svc.receive(supplier_id="SUP-A", supplier_lot="SL-1", quantity=100,
                                   location="WH-RAW", actor="仓管员")
        self.rb = self.svc.receive(supplier_id="SUP-B", supplier_lot="SL-9", quantity=50,
                                   location="WH-RAW", actor="仓管员")
        lot_a, lot_b = self.ra.payload["lot_id"], self.rb.payload["lot_id"]
        # A 拆包 60 -> A1
        self.svc.split(source_lot=lot_a, location="WH-RAW",
                       children=[{"lot_id": "A1", "quantity": 60}], actor="仓管员")
        # 合并 A1(60) + B(40) -> MIX
        self.svc.merge(sources=[
            {"lot_id": "A1", "location": "WH-RAW", "quantity": 60},
            {"lot_id": lot_b, "location": "WH-RAW", "quantity": 40},
        ], target_lot="MIX", target_location="WH-PREP", actor="仓管员")
        # 领料 + 消耗 80 -> FG-1；其余在库
        self.svc.issue(lot_id="MIX", quantity=80, from_location="WH-PREP",
                       order_id="WO-7", actor="班组长")
        self.svc.consume(order_id="WO-7", lot_id="MIX", quantity=75, fg_batch="FG-1",
                         scrap_quantity=5, fg_quantity=70, actor="操作工")
        self.lot_a, self.lot_b, self.mix = lot_a, lot_b, "MIX"

    def test_forward_trace_locates_supplier_lot(self):
        out = self.trace.trace_forward("SUP-A", "SL-1")
        self.assertTrue(out["accounted"])
        # 100 = 原批次剩 40 + MIX剩 20*0.6 + 暂存5*0.6 + 成品70*0.6 + 报废5*0.6
        fg = next(x for x in out["in_finished_goods"] if x["fg_batch"] == "FG-1")
        self.assertEqual(Decimal(fg["quantity"]), Decimal("42.000000"))
        # 召回报告含全部受影响记录类型
        report = self.trace.recall_report("SUP-A", "SL-1")
        self.assertEqual(len(report["records"]["receives"]), 1)
        self.assertEqual(len(report["records"]["splits"]), 1)
        self.assertEqual(len(report["records"]["merges"]), 1)
        self.assertEqual(len(report["records"]["issues"]), 1)
        self.assertEqual(len(report["records"]["consumptions"]), 1)
        self.assertTrue(report["accounted"])

    def test_backward_trace_fg_to_supplier_lots(self):
        out = self.trace.trace_backward(fg_batch="FG-1")
        roots = {x["supplier_id"] + ":" + x["supplier_lot"]: Decimal(x["quantity"])
                 for x in out["supplier_origins"]}
        # 成品 70 按 60/40 混合
        self.assertEqual(roots["SUP-A:SL-1"], Decimal("42.000000"))
        self.assertEqual(roots["SUP-B:SL-9"], Decimal("28.000000"))

    def test_backward_trace_stock_lot(self):
        out = self.trace.trace_backward(lot_id="MIX")
        roots = {x["supplier_id"] + ":" + x["supplier_lot"]: Decimal(x["quantity"])
                 for x in out["supplier_origins"]}
        # MIX 在库 20 + 暂存 5 = 25，按 60/40
        self.assertEqual(roots["SUP-A:SL-1"], Decimal("15.000000"))
        self.assertEqual(roots["SUP-B:SL-9"], Decimal("10.000000"))

    def test_trace_after_reversal_still_consistent(self):
        cons = [e for e in self.store.load_all() if e.event_type.value == "CONSUME"][0]
        self.svc.reverse(event_id=cons.event_id, reason="召回冻结", actor="质量工程师")
        report = self.checker.check()
        self.assertTrue(report["consistent"], report["issues"])
        out = self.trace.trace_forward("SUP-A", "SL-1")
        self.assertTrue(out["accounted"])


class ApiTest(unittest.TestCase):
    def setUp(self):
        self.store = EventStore(":memory:")
        self.server = create_server(self.store, "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def _url(self, path):
        return f"http://127.0.0.1:{self.port}{path}"

    def _post(self, path, body, key=None):
        req = urllib.request.Request(
            self._url(path), data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json", **({"Idempotency-Key": key} if key else {})},
            method="POST",
        )
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())

    def _get(self, path):
        with urllib.request.urlopen(self._url(path)) as resp:
            return resp.status, json.loads(resp.read())

    def test_api_end_to_end(self):
        status, body = self._post("/api/events/receive", {
            "actor": "仓管员", "supplier_id": "SUP-A", "supplier_lot": "SL-1",
            "quantity": 100, "location": "WH", "lot_id": "L1",
        })
        self.assertEqual(status, 201)

        status, body = self._post("/api/events/issue", {
            "actor": "班组长", "lot_id": "L1", "quantity": 60,
            "from_location": "WH", "order_id": "WO-1",
        }, key="issue-1")
        self.assertEqual(status, 201)
        # 幂等
        _, again = self._post("/api/events/issue", {
            "actor": "班组长", "lot_id": "L1", "quantity": 60,
            "from_location": "WH", "order_id": "WO-1",
        }, key="issue-1")
        self.assertEqual(body["event"]["event_id"], again["event"]["event_id"])

        # 超领被 400 拒绝
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            self._post("/api/events/issue", {
                "actor": "班组长", "lot_id": "L1", "quantity": 50,
                "from_location": "WH", "order_id": "WO-2",
            })
        self.assertEqual(ctx.exception.code, 400)

        self._post("/api/events/consume", {
            "actor": "操作工", "order_id": "WO-1", "lot_id": "L1",
            "quantity": 60, "fg_batch": "FG-1",
        })
        _, stock = self._get("/api/stock")
        # 收货 100、领料 60 后，WH 库位仍余 40
        self.assertEqual(len(stock["positions"]), 1)
        self.assertEqual(stock["positions"][0]["quantity"], "40.000000")
        self.assertEqual(stock["positions"][0]["location"], "WH")

    def test_api_trace_and_consistency(self):
        self._post("/api/events/receive", {
            "actor": "a", "supplier_id": "SUP-A", "supplier_lot": "SL-1",
            "quantity": 100, "location": "WH", "lot_id": "L1",
        })
        self._post("/api/events/issue", {
            "actor": "a", "lot_id": "L1", "quantity": 60,
            "from_location": "WH", "order_id": "WO-1",
        })
        self._post("/api/events/consume", {
            "actor": "a", "order_id": "WO-1", "lot_id": "L1",
            "quantity": 60, "fg_batch": "FG-1",
        })
        _, stock = self._get("/api/stock")
        self.assertEqual(len(stock["positions"]), 1)
        self.assertEqual(stock["positions"][0]["quantity"], "40.000000")

        _, fwd = self._get("/api/trace/forward?supplier_id=SUP-A&supplier_lot=SL-1")
        self.assertTrue(fwd["accounted"])
        self.assertEqual(fwd["in_finished_goods"][0]["quantity"], "60.000000")

        _, back = self._get("/api/trace/backward?fg_batch=FG-1")
        self.assertEqual(back["supplier_origins"][0]["supplier_lot"], "SL-1")

        _, report = self._get("/api/recall?supplier_id=SUP-A&supplier_lot=SL-1")
        self.assertEqual(len(report["records"]["issues"]), 1)
        self.assertEqual(len(report["records"]["consumptions"]), 1)

        _, cons = self._get("/api/consistency")
        self.assertTrue(cons["consistent"])

        _, events = self._get("/api/events")
        rev_target = next(e["event_id"] for e in events["events"] if e["event_type"] == "CONSUME")
        status, _ = self._post(f"/api/events/{rev_target}/reverse", {"actor": "质量工程师", "reason": "召回"})
        self.assertEqual(status, 201)
        _, cons2 = self._get("/api/consistency")
        self.assertTrue(cons2["consistent"])


if __name__ == "__main__":
    unittest.main()
