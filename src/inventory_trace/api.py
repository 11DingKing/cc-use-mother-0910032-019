"""HTTP API：基于标准库的 JSON 接口，零第三方依赖。

路由：

写命令（均返回追加的不可变事件）
    POST /api/commands/receive      收货
    POST /api/commands/move         移动
    POST /api/commands/split        拆分
    POST /api/commands/merge        合并
    POST /api/commands/consume      领料（并发安全 + 负库存保护）
    POST /api/commands/return       退回
    POST /api/commands/reverse      撤销（补偿事件）
    POST /api/commands/correct      历史更正（补偿事件）

查询
    GET  /api/events                事件流（?batch_id= 过滤）
    GET  /api/events/{seq}
    GET  /api/batches               当前库存（?sku=&location=&include_zero=）
    GET  /api/batches/{id}

双向追溯
    GET  /api/trace/forward?supplier_lot=...    召回：供应商批号 -> 库位/消耗
    GET  /api/trace/backward?work_order=...     反向：工单/成品 -> 供应商批号
    GET  /api/trace/batch/{id}                  批次双向血缘

审计
    GET  /api/consistency           全量一致性检查
    GET  /healthz

所有写请求支持 ``Idempotency-Key`` 头（或 body 中的 event_id）做幂等键。
"""
from __future__ import annotations

import json
import os
import re
import sys
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from inventory_trace.consistency import run_checks
from inventory_trace.errors import LedgerError
from inventory_trace.lineage import TraceService
from inventory_trace.service import InventoryService


class ApiContext:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.service = InventoryService(db_path)
        self.tracer = TraceService(self.service.store)

    def consistency(self) -> dict:
        conn = self.service.store.open()
        try:
            return run_checks(self.service.store, conn)
        finally:
            conn.close()


# --------------------------------------------------------------------------- #
# 命令分派：字段名与 service 关键字参数一一对应
# --------------------------------------------------------------------------- #

COMMAND_FIELDS: dict[str, tuple[str, tuple[str, ...]]] = {
    "receive": ("receive", (
        "sku", "quantity", "location", "supplier_lot", "actor",
        "batch_id", "event_id", "occurred_at", "note",
    )),
    "move": ("move", (
        "batch_id", "quantity", "to_location", "moved_batch_id",
        "actor", "event_id",
    )),
    "split": ("split", ("batch_id", "children", "actor", "event_id")),
    "merge": ("merge", (
        "sources", "location", "batch_id", "actor", "event_id",
    )),
    "consume": ("consume", (
        "allocations", "work_order", "product", "actor",
        "event_id", "occurred_at", "note",
    )),
    "return": ("return_stock", (
        "work_order", "sku", "quantity", "location", "supplier_lot",
        "actor", "batch_id", "event_id",
    )),
}


class Handler(BaseHTTPRequestHandler):
    server_version = "InventoryTrace/0.2"
    ctx: ApiContext

    # -- 工具 -------------------------------------------------------------- #

    def _send_json(self, payload: dict | list, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            value = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise LedgerError(f"请求体不是合法 JSON：{exc}") from exc
        if not isinstance(value, dict):
            raise LedgerError("请求体必须是 JSON 对象")
        idem = self.headers.get("Idempotency-Key")
        if idem and "event_id" not in value:
            value["event_id"] = idem
        return value

    def log_message(self, fmt: str, *args) -> None:  # noqa: A003
        print(f"[http] {self.address_string()} {fmt % args}")

    # -- GET --------------------------------------------------------------- #

    def do_GET(self) -> None:  # noqa: N802
        try:
            parts = urlsplit(self.path)
            path = parts.path.rstrip("/") or "/"
            query = {k: v[0] for k, v in parse_qs(parts.query).items()}
            ctx = self.ctx

            if path == "/healthz":
                self._send_json({"status": "ok"})
            elif path == "/api/consistency":
                self._send_json(ctx.consistency())
            elif path == "/api/events":
                self._send_json(ctx.service.list_events(
                    batch_id=query.get("batch_id"),
                    limit=int(query.get("limit", "200")),
                ))
            elif re.fullmatch(r"/api/events/(\d+)", path):
                seq = int(path.rsplit("/", 1)[1])
                self._send_json(ctx.service.get_event(seq))
            elif path == "/api/batches":
                self._send_json(ctx.service.list_batches(
                    sku=query.get("sku"),
                    location=query.get("location"),
                    include_zero=query.get("include_zero") in {"1", "true", "yes"},
                ))
            elif re.fullmatch(r"/api/batches/([^/]+)", path):
                bid = path.rsplit("/", 1)[1]
                self._send_json(ctx.service.get_batch(bid))
            elif path == "/api/trace/forward":
                lot = query.get("supplier_lot")
                if not lot:
                    raise LedgerError("缺少 supplier_lot 查询参数")
                self._send_json(ctx.tracer.forward_by_supplier_lot(lot, sku=query.get("sku")))
            elif path == "/api/trace/backward":
                wo = query.get("work_order")
                if not wo:
                    raise LedgerError("缺少 work_order 查询参数")
                self._send_json(ctx.tracer.backward_by_work_order(wo))
            elif re.fullmatch(r"/api/trace/batch/([^/]+)", path):
                bid = path.rsplit("/", 1)[1]
                self._send_json(ctx.tracer.trace_batch(bid))
            else:
                self._send_json({"error": "not_found", "message": f"无此路由：{path}"},
                                HTTPStatus.NOT_FOUND)
        except LedgerError as exc:
            self._send_json(exc.to_dict(), exc.status)
        except Exception as exc:  # noqa: BLE001
            self._send_json({"error": "internal", "message": str(exc)},
                            HTTPStatus.INTERNAL_SERVER_ERROR)

    # -- POST -------------------------------------------------------------- #

    def do_POST(self) -> None:  # noqa: N802
        try:
            parts = urlsplit(self.path)
            path = parts.path.rstrip("/") or "/"
            body = self._read_json()
            ctx = self.ctx
            m = re.fullmatch(r"/api/commands/([a-z_]+)", path)
            if not m:
                self._send_json({"error": "not_found", "message": f"无此路由：{path}"},
                                HTTPStatus.NOT_FOUND)
                return
            command = m.group(1)

            if command in COMMAND_FIELDS:
                method_name, allowed = COMMAND_FIELDS[command]
                kwargs = {k: body[k] for k in allowed if k in body}
                result = getattr(ctx.service, method_name)(**kwargs)
                self._send_json(result, HTTPStatus.CREATED)
            elif command == "reverse":
                target = body.get("seq")
                if target is None and body.get("event_seq") is not None:
                    target = body["event_seq"]
                if not isinstance(target, int):
                    raise LedgerError("reverse 需要整数 seq")
                kwargs = {k: body[k] for k in ("actor", "event_id", "note") if k in body}
                self._send_json(ctx.service.reverse_event(target, **kwargs),
                                HTTPStatus.CREATED)
            elif command == "correct":
                kwargs = {
                    k: body[k]
                    for k in ("batch_id", "reason", "quantity", "actor",
                              "supplier_lot", "sku", "event_id", "note")
                    if k in body
                }
                self._send_json(ctx.service.correct(**kwargs), HTTPStatus.CREATED)
            else:
                self._send_json({"error": "not_found", "message": f"未知命令：{command}"},
                                HTTPStatus.NOT_FOUND)
        except LedgerError as exc:
            self._send_json(exc.to_dict(), exc.status)
        except TypeError as exc:
            # 多余/缺失的关键字参数
            self._send_json({"error": "validation_error", "message": str(exc)},
                            HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # noqa: BLE001
            self._send_json({"error": "internal", "message": str(exc)},
                            HTTPStatus.INTERNAL_SERVER_ERROR)


def build_server(host: str, port: int, db_path: str) -> ThreadingHTTPServer:
    ctx = ApiContext(db_path)
    handler = type("BoundHandler", (Handler,), {"ctx": ctx})
    server = ThreadingHTTPServer((host, port), handler)
    server.ctx = ctx  # type: ignore[attr-defined]
    return server


def main() -> None:
    host = os.environ.get("INVENTORY_HTTP_HOST", "127.0.0.1")
    port = int(os.environ.get("INVENTORY_HTTP_PORT", "8080"))
    db_path = os.environ.get("INVENTORY_DB_PATH", "data/inventory.db")
    server = build_server(host, port, db_path)
    print(f"库存追溯服务已启动：http://{host}:{port}  数据库：{db_path}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
