"""库存追溯后端的领域回归测试。"""
from __future__ import annotations

import json
import sys
import tempfile
import threading
import unittest
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from inventory_trace.consistency import run_checks
from inventory_trace.errors import ConflictError, NotFoundError, ValidationError
from inventory_trace.lineage import TraceService
from inventory_trace.service import InventoryService


def checks(service: InventoryService) -> dict:
    conn = service.store.open()
    try:
        return run_checks(service.store, conn)
    finally:
        conn.close()


class LifecycleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = InventoryService(":memory:")
        self.tracer = TraceService(self.svc.store)

    def test_receive_move_split_merge_consume_flow(self) -> None:
        r = self.svc.receive(sku="SKU-1", quantity="100", location="A-01",
                             supplier_lot="LOT-A")
        b1 = r["outputs"][0]["batch_id"]
        self.assertEqual(self.svc.get_batch(b1)["quantity"], "100")

        s = self.svc.split(batch_id=b1, children=[
            {"quantity": "60", "location": "A-02"},
            {"quantity": "40"},
        ])
        b2, b3 = [c["batch_id"] for c in s["payload"]["children"]]
        self.assertEqual(self.svc.get_batch(b1)["quantity"], "0")
        self.assertEqual(self.svc.get_batch(b2)["location"], "A-02")
        self.assertEqual(self.svc.get_batch(b3)["location"], "A-01")

        self.svc.move(batch_id=b2, quantity="60", to_location="A-03")
        self.assertEqual(self.svc.get_batch(b2)["location"], "A-03")

        r2 = self.svc.receive(sku="SKU-1", quantity="20", location="A-04",
                              supplier_lot="LOT-B")
        b4 = r2["outputs"][0]["batch_id"]
        m = self.svc.merge(sources=[{"batch_id": b3}, {"batch_id": b4}],
                           location="A-05")
        b5 = m["outputs"][0]["batch_id"]
        self.assertEqual(self.svc.get_batch(b5)["quantity"], "60")

        self.svc.consume(
            allocations=[{"batch_id": b2, "quantity": "60"},
                         {"batch_id": b5, "quantity": "40"}],
            work_order="WO-100", product="P-FIN",
        )
        self.assertEqual(self.svc.get_batch(b2)["consumed"], True)
        report = checks(self.svc)
        self.assertTrue(report["ok"], report["anomalies"])
        self.assertEqual(report["identity"]["received"], "120")
        self.assertEqual(report["identity"]["consumed"], "100")
        self.assertEqual(report["identity"]["on_hand"], "20")

    def test_split_conservation(self) -> None:
        r = self.svc.receive(sku="S", quantity="10", location="L1",
                             supplier_lot="L")
        b = r["outputs"][0]["batch_id"]
        # 单子批次的部分拆分合法（掰出一块，余量留父批次）
        self.svc.split(batch_id=b, children=[{"quantity": "10"}])
        # 空 children 非法
        with self.assertRaises(ValidationError):
            self.svc.split(batch_id=b, children=[])
        # 新父批次已耗尽，再拆必然超量
        with self.assertRaises(ConflictError):
            self.svc.split(batch_id=b, children=[
                {"quantity": "6"}, {"quantity": "5"}])  # 6+5 > 0

    def test_merge_requires_same_sku(self) -> None:
        b1 = self.svc.receive(sku="A", quantity="1", location="L",
                              supplier_lot="x")["outputs"][0]["batch_id"]
        b2 = self.svc.receive(sku="B", quantity="1", location="L",
                              supplier_lot="y")["outputs"][0]["batch_id"]
        with self.assertRaises(ValidationError):
            self.svc.merge(sources=[{"batch_id": b1}, {"batch_id": b2}],
                           location="L2")

    def test_invalid_quantities_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            self.svc.receive(sku="S", quantity="0", location="L")
        with self.assertRaises(ValidationError):
            self.svc.receive(sku="S", quantity="-3", location="L")
        with self.assertRaises(ValidationError):
            self.svc.receive(sku="S", quantity="abc", location="L")

    def test_decimal_precision_not_float(self) -> None:
        r = self.svc.receive(sku="S", quantity="0.3", location="L",
                             supplier_lot="L")
        b = r["outputs"][0]["batch_id"]
        self.svc.split(batch_id=b, children=[
            {"quantity": "0.1"}, {"quantity": "0.2"}])
        self.assertEqual(checks(self.svc)["ok"], True)

    def test_partial_move_creates_new_batch_and_keeps_lineage(self) -> None:
        b = self.svc.receive(sku="S", quantity="100", location="L1",
                             supplier_lot="LOT-X")["outputs"][0]["batch_id"]
        mv = self.svc.move(batch_id=b, quantity="30", to_location="L2")
        moved = mv["payload"]["moved_batch_id"]
        # 原批次保留余量与原库位，新批次承载移走数量
        self.assertEqual(self.svc.get_batch(b)["quantity"], "70")
        self.assertEqual(self.svc.get_batch(b)["location"], "L1")
        self.assertEqual(self.svc.get_batch(moved)["quantity"], "30")
        self.assertEqual(self.svc.get_batch(moved)["location"], "L2")
        self.assertEqual(self.svc.get_batch(moved)["supplier_lot"], "LOT-X")
        # 全量移动保持编号
        self.svc.move(batch_id=moved, quantity="30", to_location="L3")
        self.assertEqual(self.svc.get_batch(moved)["location"], "L3")
        # 召回血缘穿透部分移动产生的新批次
        self.svc.consume(allocations=[{"batch_id": moved, "quantity": "30"}],
                         work_order="WO-PM")
        fwd = self.tracer.forward_by_supplier_lot("LOT-X")
        wos = {c["work_order"] for c in fwd["consumptions"]}
        self.assertIn("WO-PM", wos)
        self.assertTrue(checks(self.svc)["ok"])

    def test_reverse_split_restores_parent_at_its_current_location(self) -> None:
        r = self.svc.receive(sku="S", quantity="100", location="L1",
                             supplier_lot="L")
        parent = r["outputs"][0]["batch_id"]
        # 部分拆分：父批次保留 40
        s = self.svc.split(batch_id=parent, children=[
            {"quantity": "60", "location": "L2"}])
        child = s["payload"]["children"][0]["batch_id"]
        # 父批次余量被全量移动到 L9
        self.svc.move(batch_id=parent, quantity="40", to_location="L9")
        # 子批次未动，撤销拆分：60 应恢复到父批次当前库位 L9
        self.svc.reverse_event(s["seq"])
        state = self.svc.get_batch(parent)
        self.assertEqual(state["quantity"], "100")
        self.assertEqual(state["location"], "L9")
        self.assertTrue(checks(self.svc)["ok"])


class ConcurrencyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.svc = InventoryService(str(Path(self.tmp.name) / "inv.db"))
        r = self.svc.receive(sku="S", quantity="10", location="L1",
                             supplier_lot="LOT")
        self.batch = r["outputs"][0]["batch_id"]

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_concurrent_consume_never_oversells(self) -> None:
        outcomes: list[str] = []
        lock = threading.Lock()

        def worker(i: int) -> None:
            try:
                self.svc.consume(
                    allocations=[{"batch_id": self.batch, "quantity": "1"}],
                    work_order=f"WO-{i}",
                )
                with lock:
                    outcomes.append("ok")
            except ConflictError:
                with lock:
                    outcomes.append("rejected")

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(25)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(outcomes.count("ok"), 10)
        self.assertEqual(outcomes.count("rejected"), 15)
        self.assertEqual(self.svc.get_batch(self.batch)["quantity"], "0")
        self.assertTrue(checks(self.svc)["ok"])


class CompensationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = InventoryService(":memory:")

    def test_reverse_consume_restores_stock_and_cannot_double_reverse(self) -> None:
        b = self.svc.receive(sku="S", quantity="5", location="L",
                             supplier_lot="L")["outputs"][0]["batch_id"]
        c = self.svc.consume(allocations=[{"batch_id": b, "quantity": "2"}],
                             work_order="W1")
        rev = self.svc.reverse_event(c["seq"], note="误领")
        self.assertEqual(rev["event_type"], "reversal")
        self.assertEqual(rev["supersedes"], c["seq"])
        self.assertEqual(self.svc.get_batch(b)["quantity"], "5")
        with self.assertRaises(ConflictError):
            self.svc.reverse_event(c["seq"])
        with self.assertRaises(ValidationError):
            self.svc.reverse_event(rev["seq"])
        self.assertTrue(checks(self.svc)["ok"])

    def test_reverse_must_follow_downstream_order(self) -> None:
        r = self.svc.receive(sku="S", quantity="5", location="L1",
                             supplier_lot="L")
        b = r["outputs"][0]["batch_id"]
        m = self.svc.move(batch_id=b, quantity="5", to_location="L2")
        c = self.svc.consume(allocations=[{"batch_id": b, "quantity": "5"}],
                             work_order="W")
        # 库存已耗尽，直接撤销收货不可行
        with self.assertRaises(ConflictError):
            self.svc.reverse_event(r["seq"])
        # 逆序撤销可以
        for seq in (c["seq"], m["seq"], r["seq"]):
            self.svc.reverse_event(seq)
        report = checks(self.svc)
        self.assertTrue(report["ok"])
        self.assertEqual(report["identity"]["on_hand"], "0")

    def test_corrections_short_gain_and_meta(self) -> None:
        b = self.svc.receive(sku="S", quantity="10", location="L",
                             supplier_lot="OLD")["outputs"][0]["batch_id"]
        self.svc.correct(batch_id=b, reason="short", quantity="3", note="破损")
        self.assertEqual(self.svc.get_batch(b)["quantity"], "7")
        self.svc.correct(batch_id=b, reason="gain", quantity="2")
        self.assertEqual(self.svc.get_batch(b)["quantity"], "9")
        self.svc.correct(batch_id=b, reason="adjust_meta",
                         supplier_lot="NEW", sku="S2")
        state = self.svc.get_batch(b)
        self.assertEqual(state["supplier_lot"], "NEW")
        self.assertEqual(state["sku"], "S2")
        with self.assertRaises(ConflictError):
            self.svc.correct(batch_id=b, reason="short", quantity="99")
        with self.assertRaises(ValidationError):
            self.svc.correct(batch_id=b, reason="bogus")
        self.assertTrue(checks(self.svc)["ok"])

    def test_correction_cannot_be_mirrored_use_reverse_correction(self) -> None:
        b = self.svc.receive(sku="S", quantity="10", location="L",
                             supplier_lot="L")["outputs"][0]["batch_id"]
        cor = self.svc.correct(batch_id=b, reason="short", quantity="2")
        with self.assertRaises(ConflictError):
            self.svc.reverse_event(cor["seq"])
        # 反向更正
        self.svc.correct(batch_id=b, reason="gain", quantity="2")
        self.assertEqual(self.svc.get_batch(b)["quantity"], "10")


class ImmutabilityTest(unittest.TestCase):
    def test_events_are_append_only(self) -> None:
        import sqlite3
        tmp = tempfile.TemporaryDirectory()
        path = str(Path(tmp.name) / "inv.db")
        svc = InventoryService(path)
        svc.receive(sku="S", quantity="1", location="L", supplier_lot="L")
        raw = sqlite3.connect(path)
        for statement in (
            "UPDATE events SET actor='x' WHERE seq=1",
            "DELETE FROM events WHERE seq=1",
            "DELETE FROM event_lines WHERE seq=1",
        ):
            with self.assertRaises(sqlite3.IntegrityError):
                raw.execute(statement)
        raw.close()
        tmp.cleanup()

    def test_idempotent_event_id(self) -> None:
        r1 = self.svc.receive(sku="S", quantity="1", location="L",
                              supplier_lot="L", event_id="idem-1")
        r2 = self.svc.receive(sku="S", quantity="1", location="L",
                              supplier_lot="L", event_id="idem-1")
        self.assertEqual(r1["seq"], r2["seq"])
        self.assertEqual(len(self.svc.list_events()), 1)

    def test_hash_chain_detects_external_tampering(self) -> None:
        import sqlite3
        tmp = tempfile.TemporaryDirectory()
        path = str(Path(tmp.name) / "inv.db")
        svc = InventoryService(path)
        svc.receive(sku="S", quantity="1", location="L", supplier_lot="L")
        # 攻击者直接操作底层文件：删除触发器后插入伪造事件
        raw = sqlite3.connect(path)
        raw.execute("DROP TRIGGER events_no_update")
        raw.execute("UPDATE events SET actor='伪造者' WHERE seq=1")
        raw.commit()
        raw.close()
        report = checks(svc)
        self.assertFalse(report["ok"])
        self.assertTrue(any("哈希" in a for a in report["anomalies"]),
                        report["anomalies"])
        tmp.cleanup()

    def setUp(self) -> None:
        self.svc = InventoryService(":memory:")


class TraceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = InventoryService(":memory:")
        self.tracer = TraceService(self.svc.store)
        b1 = self.svc.receive(sku="SKU-1", quantity="100", location="A-01",
                              supplier_lot="LOT-A")["outputs"][0]["batch_id"]
        s = self.svc.split(batch_id=b1, children=[
            {"quantity": "60", "location": "A-02"}, {"quantity": "40"}])
        self.b2, self.b3 = [c["batch_id"] for c in s["payload"]["children"]]
        self.b4 = self.svc.receive(
            sku="SKU-1", quantity="20", location="A-04",
            supplier_lot="LOT-B")["outputs"][0]["batch_id"]
        self.b5 = self.svc.merge(
            sources=[{"batch_id": self.b3}, {"batch_id": self.b4}],
            location="A-05")["outputs"][0]["batch_id"]
        self.svc.consume(
            allocations=[{"batch_id": self.b2, "quantity": "60"},
                         {"batch_id": self.b5, "quantity": "40"}],
            work_order="WO-100", product="P-FIN")

    def test_forward_recall_attributes_weighted_quantities(self) -> None:
        fwd = self.tracer.forward_by_supplier_lot("LOT-A")
        self.assertTrue(fwd["found"])
        # 合并体 60 中 LOT-A 占 40/60；剩余 20 中归属 LOT-A 为 13.333...
        self.assertEqual(Decimal(fwd["total_attributed_on_hand"]),
                         Decimal("13.333333333"))
        self.assertEqual(Decimal(fwd["total_attributed_consumed"]),
                         Decimal("86.666666667"))
        # 归属量闭环 = 原始收货 100
        self.assertEqual(
            Decimal(fwd["total_attributed_on_hand"])
            + Decimal(fwd["total_attributed_consumed"]),
            Decimal("100.000000000"))
        wos = {c["work_order"] for c in fwd["consumptions"]}
        self.assertEqual(wos, {"WO-100"})

        fwd_b = self.tracer.forward_by_supplier_lot("LOT-B")
        self.assertEqual(
            Decimal(fwd_b["total_attributed_on_hand"])
            + Decimal(fwd_b["total_attributed_consumed"]),
            Decimal("20.000000000"))

    def test_backward_trace_finds_supplier_lots(self) -> None:
        bwd = self.tracer.backward_by_work_order("WO-100")
        lots = {item["supplier_lot"]: Decimal(item["quantity"])
                for item in bwd["supplier_lots"]}
        self.assertEqual(lots["LOT-A"], Decimal("86.666666667"))
        self.assertEqual(lots["LOT-B"], Decimal("13.333333333"))
        self.assertEqual(sum(lots.values(), Decimal(0)), Decimal("100.000000000"))

    def test_trace_batch_bidirectional(self) -> None:
        info = self.tracer.trace_batch(self.b5)
        self.assertTrue(info["found"])
        self.assertEqual(set(info["upstream_supplier_lots"]), {"LOT-A", "LOT-B"})
        self.assertEqual(info["downstream_work_orders"], ["WO-100"])

    def test_unknown_keys_report_not_found(self) -> None:
        self.assertFalse(self.tracer.forward_by_supplier_lot("NOPE")["found"])
        self.assertFalse(self.tracer.backward_by_work_order("NOPE")["found"])
        self.assertFalse(self.tracer.trace_batch("BNOPE")["found"])

    def test_returned_stock_stays_connected_to_work_order(self) -> None:
        ret = self.svc.return_stock(
            work_order="WO-100", sku="SKU-1", quantity="5", location="A-06")
        rb = ret["outputs"][0]["batch_id"]
        self.svc.consume(allocations=[{"batch_id": rb, "quantity": "5"}],
                         work_order="WO-200")
        # 余料批次的源头仍是 WO-100 的消耗批次（可穿透工单节点）
        bwd = self.tracer.backward_by_work_order("WO-200")
        lots = {i["supplier_lot"] for i in bwd["supplier_lots"]}
        self.assertEqual(lots, {"LOT-A", "LOT-B"})


class ReturnTest(unittest.TestCase):
    def test_return_creates_linked_batch(self) -> None:
        svc = InventoryService(":memory:")
        ret = svc.return_stock(work_order="WO-9", sku="S", quantity="3",
                               location="L", supplier_lot="L")
        b = ret["outputs"][0]["batch_id"]
        self.assertEqual(svc.get_batch(b)["quantity"], "3")
        self.assertTrue(checks(svc)["ok"])


if __name__ == "__main__":
    unittest.main()
