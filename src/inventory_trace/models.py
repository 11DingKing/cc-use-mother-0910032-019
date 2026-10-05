"""事件与命令的领域模型。

事件是账本中唯一不可变的事实记录。每个命令原子地追加一个事件；
撤销与更正不修改旧事件，而是追加补偿事件。

数量守恒的核心表达：每个事件都有 ``inputs``（来源批次行，数量为负向）
与 ``outputs``（目标批次行，数量为正向），正常事件两侧总量恒等，
补偿事件两侧互换。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Literal

EventType = Literal[
    "receive",   # 收货：供应商批次进入仓库
    "move",      # 移动：批次转移库位
    "split",     # 拆分：一个批次拆成多个
    "merge",     # 合并：多个批次合成一个
    "consume",   # 领用：批次投入生产消耗
    "return",    # 退回：生产余料退回生成新批次
    "reversal",  # 补偿：撤销某历史事件
    "correction",  # 补偿：账面差异更正
]

# 纯库存内部流转事件：inputs 与 outputs 的数量之和必须相等
BALANCED_EVENTS = frozenset({"receive", "move", "split", "merge", "return", "reversal"})
# 消耗事件：库存减少，只允许有 inputs
CONSUMPTION_EVENTS = frozenset({"consume"})
# 更正事件：通过 reason 分类，盘亏时只减库存
CORRECTION_EVENT = "correction"

ALL_EVENTS = BALANCED_EVENTS | CONSUMPTION_EVENTS | {CORRECTION_EVENT}

# 可被 reversal 直接镜像撤销的事件类型；
# correction（单侧调账）不能镜像，应签发反向 correction
REVERSIBLE_EVENTS = frozenset({"receive", "move", "split", "merge", "return", "consume"})


def D(value: str | Decimal) -> Decimal:
    """以字符串构造 Decimal，避免 float 精度问题。"""
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


@dataclass(frozen=True, slots=True)
class Line:
    """事件中的一条批次行。

    batch_id 为 ``*`` 表示外部（供应商 / 生产订单 / 损耗），不占用库存。
    """

    batch_id: str
    quantity: Decimal
    location: str | None = None
    external_ref: str | None = None  # 供应商批号 / 生产工单号等外部凭证

    def to_row(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "quantity": str(self.quantity),
            "location": self.location,
            "external_ref": self.external_ref,
        }

    @staticmethod
    def from_row(row: dict[str, Any]) -> "Line":
        return Line(
            batch_id=row["batch_id"],
            quantity=D(row["quantity"]),
            location=row.get("location"),
            external_ref=row.get("external_ref"),
        )


@dataclass(frozen=True, slots=True)
class Event:
    """一条不可变库存事件（events 表的一行）。"""

    seq: int
    event_id: str
    event_type: str
    occurred_at: str
    actor: str
    inputs: tuple[Line, ...]
    outputs: tuple[Line, ...]
    payload: dict[str, Any] = field(default_factory=dict)
    supersedes: int | None = None  # 补偿事件所针对的历史事件 seq
    correction_of: int | None = None
    reversed_by: int | None = None  # 由投影层回填：本事件被哪条 reversal 撤销

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "occurred_at": self.occurred_at,
            "actor": self.actor,
            "inputs": [line.to_row() for line in self.inputs],
            "outputs": [line.to_row() for line in self.outputs],
            "payload": self.payload,
            "supersedes": self.supersedes,
            "correction_of": self.correction_of,
            "reversed_by": self.reversed_by,
        }


@dataclass(frozen=True, slots=True)
class BatchState:
    """批次的当前投影状态。"""

    batch_id: str
    sku: str
    supplier_lot: str | None
    quantity: Decimal
    location: str
    consumed: bool
    created_seq: int
    last_seq: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "sku": self.sku,
            "supplier_lot": self.supplier_lot,
            "quantity": str(self.quantity),
            "location": self.location,
            "consumed": self.consumed,
            "created_seq": self.created_seq,
            "last_seq": self.last_seq,
        }
