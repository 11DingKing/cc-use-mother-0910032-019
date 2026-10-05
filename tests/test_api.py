"""HTTP API 端到端测试（真实监听随机端口）。"""
from __future__ import annotations



import http.client
import json
import sys
import tempfile
import threading
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from inventory_trace.api import build_server


class ApiClient:
    def __init__(self, host: str, port: int) -> None:
        self.host, self.port = host, port

    def request(self, method: str, path: str, body: dict | None = None,
                headers: dict | None = None):
        conn = http.client.HTTPConnection(self.host, self.port, timeout=5)
        data = json.dumps(body).encode() if body is not None else None
        hdr = {"Content-Type": "application/json"}
        if headers:
            hdr.update(headers)
        conn.request(method, path, body=data, headers=hdr)
        resp = conn.getresponse()
        payload = resp.read().decode()
        conn.close()
        return resp.status, json.loads(payload) if payload else None


class ApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.TemporaryDirectory()
        db = str(Path(cls.tmp.name) / "api.db")
        cls.server = build_server("127.0.0.1", 0, db)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.api = ApiClient("127.0.0.1", cls.port)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
        cls.tmp.cleanup()

    def cmd(self, name: str, body: dict, headers: dict | None = None) -> dict:
        status, payload = self.api.request("POST", f"/api/commands/{name}", body, headers)
        self.assertEqual(status, 201, payload)
        return payload

    def test_health_and_empty_consistency(self) -> None:
        status, payload = self.api.request("GET", "/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "ok")
        status, report = self.api.request("GET", "/api/consistency")
        self.assertEqual(status, 200)
        self.assertTrue(report["ok"])

    def test_full_scenario_and_bidirectional_trace(self) -> None:
        r = self.cmd("receive", {
            "sku": "SKU-1", "quantity": "100", "location": "A-01",
            "supplier_lot": "LOT-A"})
        b1 = r["outputs"][0]["batch_id"]
        s = self.cmd("split", {"batch_id": b1, "children": [
            {"quantity": "60", "location": "A-02"}, {"quantity": "40"}]})
        b2, b3 = [c["batch_id"] for c in s["payload"]["children"]]
        self.cmd("move", {"batch_id": b2, "quantity": "60", "to_location": "A-03"})
        r2 = self.cmd("receive", {
            "sku": "SKU-1", "quantity": "20", "location": "A-04",
            "supplier_lot": "LOT-B"})
        b4 = r2["outputs"][0]["batch_id"]
        m = self.cmd("merge", {"sources": [
            {"batch_id": b3}, {"batch_id": b4}], "location": "A-05"})
        b5 = m["outputs"][0]["batch_id"]
        self.cmd("consume", {
            "allocations": [
                {"batch_id": b2, "quantity": "60"},
                {"batch_id": b5, "quantity": "40"}],
            "work_order": "WO-100", "product": "P-FIN"})

        status, fwd = self.api.request(
            "GET", "/api/trace/forward?supplier_lot=LOT-A")
        self.assertEqual(status, 200)
        self.assertTrue(fwd["found"])
        self.assertEqual(fwd["total_attributed_consumed"], "86.666666667")
        self.assertEqual(len(fwd["consumptions"]), 1)

        status, bwd = self.api.request(
            "GET", "/api/trace/backward?work_order=WO-100")
        self.assertEqual(status, 200)
        lots = {i["supplier_lot"]: i["quantity"] for i in bwd["supplier_lots"]}
        self.assertEqual(lots, {"LOT-A": "86.666666667", "LOT-B": "13.333333333"})

        status, batches = self.api.request("GET", "/api/batches")
        self.assertEqual(status, 200)
        self.assertEqual(sum(1 for b in batches if Decimal_safe(b["quantity"]) > 0), 1)

        status, report = self.api.request("GET", "/api/consistency")
        self.assertTrue(report["ok"], report["anomalies"])

    def test_negative_stock_returns_conflict(self) -> None:
        r = self.cmd("receive", {
            "sku": "S", "quantity": "1", "location": "L",
            "supplier_lot": "L"})
        b = r["outputs"][0]["batch_id"]
        status, payload = self.api.request("POST", "/api/commands/consume", {
            "allocations": [{"batch_id": b, "quantity": "2"}],
            "work_order": "W"})
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"], "conflict")

    def test_reverse_and_correct_compensation_events(self) -> None:
        r = self.cmd("receive", {"sku": "S", "quantity": "5", "location": "L",
                                 "supplier_lot": "L"})
        b = r["outputs"][0]["batch_id"]
        c = self.cmd("consume", {"allocations": [{"batch_id": b, "quantity": "2"}],
                                 "work_order": "W"})
        status, rev = self.api.request("POST", "/api/commands/reverse",
                                       {"seq": c["seq"], "note": "误领"})
        self.assertEqual(status, 201)
        self.assertEqual(rev["supersedes"], c["seq"])

        status, _ = self.api.request("POST", "/api/commands/correct", {
            "batch_id": b, "reason": "short", "quantity": "1"})
        self.assertEqual(status, 201)

        status, event = self.api.request("GET", f"/api/events/{c['seq']}")
        self.assertEqual(status, 200)
        self.assertEqual(event["reversed_by"], rev["seq"])

    def test_idempotency_key_header(self) -> None:
        hdr = {"Idempotency-Key": "fixed-key-1"}
        a = self.cmd("receive", {"sku": "S", "quantity": "1", "location": "L",
                                 "supplier_lot": "L"}, headers=hdr)
        status, b = self.api.request("POST", "/api/commands/receive", {
            "sku": "S", "quantity": "1", "location": "L",
            "supplier_lot": "L"}, headers=hdr)
        self.assertEqual(status, 201)
        self.assertEqual(a["seq"], b["seq"])

    def test_validation_errors(self) -> None:
        status, payload = self.api.request("POST", "/api/commands/receive",
                                           {"sku": "S", "quantity": "-1",
                                            "location": "L"})
        self.assertEqual(status, 400)
        status, payload = self.api.request("GET", "/api/batches/NOPE")
        self.assertEqual(status, 404)
        status, payload = self.api.request("GET", "/nope")
        self.assertEqual(status, 404)


def Decimal_safe(value: str):
    from decimal import Decimal
    return Decimal(value)


if __name__ == "__main__":
    unittest.main()
