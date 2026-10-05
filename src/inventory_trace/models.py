"""领域模型：事件、账户、数量与异常。

账户（守恒记账的最小单位）：
- ``ext:supplier:{supplier}:{supplier_lot}`` 供应商批次（系统边界外，源头）
- ``stock:{lot}@{location}``                库位上的库存批次
- ``stage:{order}:{lot}``                    生产工单工位旁暂存（已领未耗）
- ``fg:{fg_batch}``                          成品批次
- ``ext:{name}``                             其他边界（scrap/shrinkage/adjustment...）

每条事件由若干带符号分录(Leg)组成，分录之和恒为 0；
内部账户(stock/stage/fg)余额在任意时刻不得为负。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import uuid4

#: 数量精度：六位小数，全链路整数化运算避免浮点误差
QUANT = Decimal("0.000001")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def new_id() -> str:
    return uuid4().hex


def qty(value: Any) -> Decimal:
    """把入参转为 Decimal 并量化，拒绝非法/负数/NaN。"""
    if isinstance(value, bool):  # bool 是 int 的子类，显式拒绝
        raise ValidationError("数量不能是布尔值")
    try:
        d = Decimal(str(value)).quantize(QUANT)
    except Exception as exc:  # InvalidOperation 等
        raise ValidationError(f"非法数量：{value!r}") from exc
    if not d.is_finite():
        raise ValidationError(f"数量必须有限：{value!r}")
    return d


def require_positive(d: Decimal, name: str = "数量") -> Decimal:
    if d <= 0:
        raise ValidationError(f"{name}必须大于 0")
    return d


class EventType(str, Enum):
    RECEIVE = "RECEIVE"        # 收货：供应商批次进入库位，创建库存批次
    MOVE = "MOVE"              # 移动：同批次库位间转移（支持部分数量）
    SPLIT = "SPLIT"            # 拆包/拆分：一批次拆为多批次
    MERGE = "MERGE"            # 合并：多批次合为一批次
    ISSUE = "ISSUE"            # 领料：库位 → 生产工单暂存
    CONSUME = "CONSUME"        # 生产消耗（成品使用）：暂存 → 成品批次/报废
    RETURN = "RETURN"          # 退料：工单暂存 → 库位
    REVERSAL = "REVERSAL"      # 补偿：撤销原事件（红字反向分录）
    CORRECTION = "CORRECTION"  # 补偿：历史更正（显式平衡分录 + 原因）


STRUCTURAL_TYPES = {
    EventType.RECEIVE,
    EventType.SPLIT,
    EventType.MERGE,
}


@dataclass(frozen=True)
class Leg:
    """一条记账分录。delta>0 入，delta<0 出，同一事件全部 delta 之和为 0。"""

    account: str
    delta: Decimal
    lot_id: str | None = None  # 便于归因/投影的冗余信息

    def to_json(self) -> dict:
        return {"account": self.account, "delta": str(self.delta), "lot_id": self.lot_id}

    @staticmethod
    def from_json(value: dict) -> "Leg":
        return Leg(
            account=value["account"],
            delta=qty(value["delta"]),
            lot_id=value.get("lot_id"),
        )


@dataclass(frozen=True)
class Event:
    seq: int
    event_id: str
    event_type: EventType
    actor: str
    occurred_at: str
    payload: dict[str, Any]
    legs: tuple[Leg, ...]
    idempotency_key: str | None = None

    def to_json(self) -> dict:
        return {
            "seq": self.seq,
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "actor": self.actor,
            "occurred_at": self.occurred_at,
            "payload": self.payload,
            "legs": [leg.to_json() for leg in self.legs],
            "idempotency_key": self.idempotency_key,
        }


# ---------------------------------------------------------------- 账户工具

def supplier_account(supplier_id: str, supplier_lot: str) -> str:
    return f"ext:supplier:{supplier_id}:{supplier_lot}"


def stock_account(lot_id: str, location: str) -> str:
    return f"stock:{lot_id}@{location}"


def stage_account(order_id: str, lot_id: str) -> str:
    return f"stage:{order_id}:{lot_id}"


def fg_account(fg_batch: str) -> str:
    return f"fg:{fg_batch}"


def parse_stock_account(account: str) -> tuple[str, str] | None:
    if not account.startswith("stock:") or "@" not in account:
        return None
    body = account[len("stock:") :]
    lot_id, location = body.rsplit("@", 1)
    return lot_id, location


def parse_stage_account(account: str) -> tuple[str, str] | None:
    if not account.startswith("stage:"):
        return None
    body = account[len("stage:") :]
    order_id, lot_id = body.split(":", 1)
    return order_id, lot_id


def parse_fg_account(account: str) -> str | None:
    return account[len("fg:") :] if account.startswith("fg:") else None


INTERNAL_PREFIXES = ("stock:", "stage:", "fg:")


def is_internal(account: str) -> bool:
    return account.startswith(INTERNAL_PREFIXES)


def account_lot(account: str) -> str | None:
    parsed = parse_stock_account(account)
    if parsed:
        return parsed[0]
    parsed = parse_stage_account(account)
    if parsed:
        return parsed[1]
    return None


# ---------------------------------------------------------------- 异常

class LedgerError(Exception):
    """台账领域错误基类。"""


class ValidationError(LedgerError):
    """入参或事件结构不合法。"""


class QuantityError(LedgerError):
    """数量守恒失败：分录不平衡、负库存或库存不足。"""


class TracebackError(LedgerError):
    """批次链断裂、引用不存在等追溯错误。"""


class ConcurrencyError(LedgerError):
    """并发写入冲突（获取写锁超时）。"""
