"""HTTP/JSON API（标准库实现，零外部依赖）。

路由：
    POST /api/events/receive|move|split|merge|issue|consume|return   业务事件
    POST /api/events/{event_id}/reverse                               撤销（补偿）
    POST /api/corrections                                             历史更正（补偿）
    GET  /api/events?seq=                          事件台账（不可变，按 seq 排序）
    GET  /api/stock                                当前库存位置
    GET  /api/lots/{lot_id}                        批次身份与来源
    GET  /api/trace/forward?supplier_id&supplier_lot   正向追踪
    GET  /api/trace/backward?fg_batch=|lot_id=&..     反向追踪
    GET  /api/recall?supplier_id&supplier_lot          质量召回报告
    GET  /api/consistency                              一致性检查

写操作支持 ``Idempotency-Key`` 请求头。
"""
from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .consistency import ConsistencyChecker
from .ledger import EventStore
from .models import LedgerError
from .projection import Projection
from .services import InventoryService
from .trace import TraceEngine


def _json_default(value):
    from decimal import Decimal
    from .models import Event

    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Event):
        return value.to_json()
    raise TypeError(f"不可序列化：{type(value)}")


def create_server(store: EventStore, host: str = "127.0.0.1", port: int = 8080) -> ThreadingHTTPServer:
    service = InventoryService(store)
    tracer = TraceEngine(store)
    checker = ConsistencyChecker(store)

    class Handler(BaseHTTPRequestHandler):
        server_version = "InventoryTrace/1.0"

        def log_message(self, fmt, *args):  # 安静日志
            pass

        def _send(self, status: int, body) -> None:
            data = json.dumps(body, ensure_ascii=False, default=_json_default).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _body(self) -> dict:
            length = int(self.headers.get("Content-Length") or 0)
            if not length:
                return {}
            raw = self.rfile.read(length)
            try:
                value = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError as exc:
                raise LedgerError(f"请求体不是合法 JSON：{exc}") from exc
            if not isinstance(value, dict):
                raise LedgerError("请求体必须是 JSON 对象")
            return value

        def _idem(self) -> str | None:
            return self.headers.get("Idempotency-Key")

        # ------------------------------------------------------ GET

        def do_GET(self) -> None:  # noqa: N802
            try:
                parsed = urlparse(self.path)
                q = {k: v[0] for k, v in parse_qs(parsed.query).items()}
                path = parsed.path
                if path == "/api/events":
                    events = store.load_all()
                    if "seq" in q:
                        seq = int(q["seq"])
                        events = [e for e in events if e.seq <= seq]
                    self._send(200, {"events": [e.to_json() for e in events], "count": len(events)})
                elif path == "/api/stock":
                    self._send(200, {"positions": Projection(store.load_all()).stock_positions()})
                elif path.startswith("/api/lots/"):
                    lot_id = path.rsplit("/", 1)[1]
                    proj = Projection(store.load_all())
                    info = proj.lot_info.get(lot_id)
                    if not info:
                        self._send(404, {"error": "批次不存在", "lot_id": lot_id})
                        return
                    positions = [x for x in proj.stock_positions() if x["lot_id"] == lot_id]
                    self._send(200, {
                        "lot_id": lot_id,
                        "sku": info.sku,
                        "created_seq": info.created_seq,
                        "created_event_id": info.created_event_id,
                        "supplier_origins": info.origins,
                        "merged": info.merged,
                        "positions": positions,
                    })
                elif path == "/api/trace/forward":
                    self._require(q, "supplier_id", "supplier_lot")
                    self._send(200, tracer.trace_forward(q["supplier_id"], q["supplier_lot"]))
                elif path == "/api/trace/backward":
                    self._send(200, tracer.trace_backward(
                        fg_batch=q.get("fg_batch"),
                        lot_id=q.get("lot_id"),
                        location=q.get("location"),
                        order_id=q.get("order_id"),
                    ))
                elif path == "/api/recall":
                    self._require(q, "supplier_id", "supplier_lot")
                    self._send(200, tracer.recall_report(q["supplier_id"], q["supplier_lot"]))
                elif path == "/api/consistency":
                    self._send(200, checker.check())
                elif path == "/api/health":
                    self._send(200, {"status": "ok", "head_seq": store.head_seq()})
                else:
                    self._send(404, {"error": "未知路由", "path": path})
            except LedgerError as exc:
                self._send(400, {"error": str(exc)})
            except Exception as exc:  # noqa: BLE001
                self._send(500, {"error": f"内部错误：{exc}"})

        # ------------------------------------------------------ POST

        def do_POST(self) -> None:  # noqa: N802
            try:
                parsed = urlparse(self.path)
                path = parsed.path
                body = self._body()
                actor = body.pop("actor", None)
                if not actor:
                    raise LedgerError("缺少 actor 字段")
                occurred_at = body.pop("occurred_at", None)

                if path == "/api/events/receive":
                    event = service.receive(actor=actor, occurred_at=occurred_at,
                                            idempotency_key=self._idem(), **body)
                elif path == "/api/events/move":
                    event = service.move(actor=actor, occurred_at=occurred_at,
                                         idempotency_key=self._idem(), **body)
                elif path == "/api/events/split":
                    event = service.split(actor=actor, occurred_at=occurred_at,
                                          idempotency_key=self._idem(), **body)
                elif path == "/api/events/merge":
                    event = service.merge(actor=actor, occurred_at=occurred_at,
                                          idempotency_key=self._idem(), **body)
                elif path == "/api/events/issue":
                    event = service.issue(actor=actor, occurred_at=occurred_at,
                                          idempotency_key=self._idem(), **body)
                elif path == "/api/events/consume":
                    event = service.consume(actor=actor, occurred_at=occurred_at,
                                            idempotency_key=self._idem(), **body)
                elif path == "/api/events/return":
                    event = service.return_material(actor=actor, occurred_at=occurred_at,
                                                    idempotency_key=self._idem(), **body)
                elif path == "/api/corrections":
                    event = service.correct(actor=actor, occurred_at=occurred_at,
                                            idempotency_key=self._idem(), **body)
                elif re.fullmatch(r"/api/events/[A-Za-z0-9-]+/reverse", path):
                    event_id = path.split("/")[3]
                    event = service.reverse(event_id=event_id, actor=actor,
                                            occurred_at=occurred_at, idempotency_key=self._idem(), **body)
                else:
                    self._send(404, {"error": "未知路由", "path": path})
                    return
                self._send(201, {"event": event.to_json()})
            except LedgerError as exc:
                self._send(400, {"error": str(exc)})
            except TypeError as exc:
                self._send(400, {"error": f"字段不匹配：{exc}"})
            except Exception as exc:  # noqa: BLE001
                self._send(500, {"error": f"内部错误：{exc}"})

        @staticmethod
        def _require(q: dict, *names: str) -> None:
            missing = [n for n in names if not q.get(n)]
            if missing:
                raise LedgerError("缺少查询参数：" + "、".join(missing))

    return ThreadingHTTPServer((host, port), Handler)


def serve(db_path: str = "inventory.db", host: str = "127.0.0.1", port: int = 8080) -> None:
    store = EventStore(db_path)
    server = create_server(store, host, port)
    print(f"库存批次追溯服务已启动：http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        store.close()
