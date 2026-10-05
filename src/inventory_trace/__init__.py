"""库存批次全链追溯后端。

以不可变库存事件为唯一事实来源（event sourcing），覆盖批次进入、移动、
拆分、合并、领用、退回，并通过补偿事件实现撤销与历史更正。
"""
from __future__ import annotations

__version__ = "0.2.0"
