"""库存批次全链追溯后端（事件溯源）。

- 不可变事件台账：收货 / 移动 / 拆包 / 拆分 / 合并 / 领料 / 消耗 / 退料
- 补偿事件：REVERSAL（撤销冲销）与 CORRECTION（历史更正）
- 任意时点数量守恒、禁止负库存
- 供应商批次 <-> 库位 / 生产工单 / 成品批次双向追踪
"""
from .models import Event, QuantityError, TracebackError, ValidationError
from .ledger import EventStore
from .services import InventoryService
from .trace import TraceEngine
from .consistency import ConsistencyChecker
from .api import create_server

__all__ = [
    "Event",
    "EventStore",
    "InventoryService",
    "TraceEngine",
    "ConsistencyChecker",
    "ValidationError",
    "QuantityError",
    "TracebackError",
    "create_server",
]
