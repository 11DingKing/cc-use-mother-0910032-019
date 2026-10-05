"""领域错误类型。"""
from __future__ import annotations


class LedgerError(Exception):
    """所有账本领域错误的基类。"""

    status = 400
    code = "ledger_error"

    def __init__(self, message: str, *, details: object | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details

    def to_dict(self) -> dict:
        result = {"error": self.code, "message": self.message}
        if self.details is not None:
            result["details"] = self.details
        return result


class ValidationError(LedgerError):
    """请求数据不满足命令前置格式要求。"""

    status = 400
    code = "validation_error"


class NotFoundError(LedgerError):
    """引用的批次、库位或事件不存在。"""

    status = 404
    code = "not_found"


class ConflictError(LedgerError):
    """数量不足、事件已撤销等业务冲突（包含并发领料失败者）。"""

    status = 409
    code = "conflict"


class CorruptedLedgerError(LedgerError):
    """事件日志无法自洽重放，说明存储已被外部破坏。"""

    status = 500
    code = "corrupted_ledger"
