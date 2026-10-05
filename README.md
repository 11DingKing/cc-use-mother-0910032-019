# 库存批次全链追溯

基于**不可变事件台账**的 Python 后端，为质量召回提供从供应商批次到库存位置、
生产消耗与成品批次的双向追踪。所有操作均以"事件"为唯一事实来源，
任何操作都校验数量守恒，并通过补偿事件实现撤销与历史更正。

## 设计

### 事件类型（只追加，永不修改/删除）

| 事件 | 含义 | 会计分录（Leg） |
|---|---|---|
| `RECEIVE` | 收货 | `ext:supplier:{供应商}:{供应商批次}` ➡ `stock:{批次}@{库位}` |
| `MOVE` | 移动 | `stock:{lot}@A` ➡ `stock:{lot}@B`（支持部分移动） |
| `SPLIT` | 拆包/拆分 | 父批次 ➡ 多个新子批次（余量可留父批次） |
| `MERGE` | 合并 | 多个来源批次 ➡ 全新目标批次 |
| `ISSUE` | 领料 | 库位 ➡ `stage:{工单}:{批次}`（已领未耗） |
| `CONSUME` | 生产消耗（成品使用） | 工单暂存 ➡ `fg:{成品批次}` 和/或 `ext:scrap` |
| `RETURN` | 退料 | 工暂存 ➡ 库位，批次身份不变 |
| `REVERSAL` | 补偿：撤销 | 对原事件的红字反向分录，原事件保留 |
| `CORRECTION` | 补偿：历史更正 | 显式平衡分录（如盘盈/盘亏走 `ext:adjustment`） |

每条事件由若干带符号分录组成，**分录之和恒为 0**；事件过账后任何
内部账户（`stock:` / `stage:` / `fg:`）余额不得为负。

### 关键机制

- **双向追溯**：重放事件维护每个账户的"供应商批次成分向量"。
  拆分/合并/移动按数量比例传播（1:1 链路精确无损，混合按比例且舍入残差
  归入最大份额，保证成分之和精确等于余额）。正向可定位批次现在落在哪些
  库位/工单/成品，反向可由成品反查全部供应商批次及数量。
- **并发领料**：写入使用 SQLite `BEGIN IMMEDIATE` 写事务，"重放最新余额→
  守恒校验→落库"在同一事务内完成，多线程/多请求串行化，杜绝超发；
  支持 `Idempotency-Key` 请求头防重。
- **负库存保护**：移动/拆分/领料/退料/消耗前在事务内校验可用量，
  超量返回 `400 QuantityError`。
- **撤销**：追加 `REVERSAL`（红字反向分录）。若影响已被下游继续使用，
  冲销会造成负库存则拒绝，必须自下游向上游按序补偿。
- **历史更正**：追加平衡的 `CORRECTION` 事件并记录原因/依据，
  历史原貌完整保留。
- **一致性检查** `GET /api/consistency`：逐事件平衡、任意时刻无负库存、
  全局边界守恒（引入 = 现存 + 流出）、成分向量闭合、批次身份完整、
  补偿引用有效、供应商批次级数量闭合。

## 目录

- `domain/contract.json`：领域角色、状态、约束和样例。
- `src/domain_contract/`：契约读取与确定性校验。
- `src/inventory_trace/`：事件溯源后端实现。
  - `models.py`：事件/分录模型、账户命名、Decimal 数量与异常。
  - `ledger.py`：SQLite 不可变事件存储（IMMEDIATE 写事务 + 幂等键）。
  - `projection.py`：事件重放投影（余额 / 成分向量 / 批次链边）。
  - `services.py`：收货、移动、拆分、合并、领料、消耗、退料、撤销、更正。
  - `trace.py`：双向追踪与质量召回报告。
  - `consistency.py`：一致性检查报告。
  - `api.py` / `__main__.py`：零依赖 HTTP/JSON API。
- `tools/check_contract.py`：契约摘要检查。
- `tools/demo_recall.py`：质量召回场景端到端演示。
- `tests/`：契约与后端回归测试（含并发领料与 HTTP 端到端）。

## 快速开始

```bash
# 质量召回场景演示（内存库，直接看报告与一致性结论）
python3 tools/demo_recall.py

# 启动 HTTP 服务（SQLite 文件持久化，零外部依赖）
PYTHONPATH=src python3 -m inventory_trace --db inventory.db --port 8080
```

### API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/events/receive` | 收货 |
| POST | `/api/events/move` | 移动 |
| POST | `/api/events/split` | 拆包/拆分 |
| POST | `/api/events/merge` | 合并 |
| POST | `/api/events/issue` | 领料 |
| POST | `/api/events/consume` | 生产消耗/成品使用 |
| POST | `/api/events/return` | 退料 |
| POST | `/api/events/{event_id}/reverse` | 撤销（补偿事件） |
| POST | `/api/corrections` | 历史更正（补偿事件） |
| GET | `/api/events` | 不可变事件台账（可按 `?seq=` 查时点快照） |
| GET | `/api/stock` | 当前库存位置 |
| GET | `/api/lots/{lot_id}` | 批次身份与供应商来源 |
| GET | `/api/trace/forward?supplier_id=&supplier_lot=` | 正向追踪 |
| GET | `/api/trace/backward?fg_batch=` 或 `?lot_id=` | 反向追踪 |
| GET | `/api/recall?supplier_id=&supplier_lot=` | 质量召回报告 |
| GET | `/api/consistency` | 一致性检查 |

写操作请在 body 中提供 `"actor"`，并建议带 `Idempotency-Key` 头。

示例：

```bash
curl -X POST localhost:8080/api/events/receive -H 'Content-Type: application/json' \
  -d '{"actor":"仓储管理员","supplier_id":"SUP-A","supplier_lot":"SL-1",
       "quantity":100,"location":"WH-RAW","lot_id":"LOT-1"}'

curl "localhost:8080/api/recall?supplier_id=SUP-A&supplier_lot=SL-1"
```

## 验证

```bash
# 全部回归测试（23 个：契约 + 守恒 + 负库存 + 并发 + 补偿 + 双向追踪 + HTTP）
python3 -m unittest discover -s tests -v

# 编译检查
python3 -m compileall -q src tools tests

# 契约检查
python3 tools/check_contract.py domain/contract.json
```
