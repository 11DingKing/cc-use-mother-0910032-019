# 库存批次全链追溯

以**不可变库存事件**为唯一事实来源的后端：批次进入、移动、拆分、合并、
领用、退回全部记账为事件；任何操作都校验数量守恒；并发领料、负库存保护、
撤销与历史更正通过补偿事件实现；支持从供应商批号到库位/生产消耗的双向
追踪，并输出一致性检查报告。

适用于质量召回场景：供应商批号 → 受影响的收货、拆包、领料位置与成品
使用记录，即使批次经历过拆分合并，来源链也不会断裂。

## 领域模型

事件日志（仅追加，SQLite 触发器禁止 UPDATE/DELETE，事件间以 SHA-256
哈希链串联）：

| 事件 | 含义 | 守恒形式 |
|---|---|---|
| `receive` | 收货，供应商批次进入仓库 | 外部 = 入库批次 |
| `move` | 库位移动（支持部分移动） | 来源 = 去向 |
| `split` | 一个批次拆成多个子批次 | 父量 = 子量之和 |
| `merge` | 多个同 SKU 批次合并 | 来源之和 = 新批次 |
| `consume` | 领料投入生产（生产工单为外部去向） | 只减库存 |
| `return` | 生产余料退回，与工单血缘相连 | 外部 = 新批次 |
| `reversal` | 撤销某历史事件的镜像补偿 | 镜像两侧互换 |
| `correction` | 盘亏 `short` / 盘盈 `gain` / 元数据更正 `adjust_meta` | 单侧或零行 |

关键保障：

* **数量守恒**：每个事件在写入前校验两侧恒等；`Decimal` 运算，杜绝浮点误差。
* **并发领料/负库存保护**：写命令在 `BEGIN IMMEDIATE` 事务内基于最新投影
  判定，SQLite 写锁将并发领料串行化，超领返回 `409`，库存永不为负。
* **不可变 + 可审计**：旧事件永不修改；撤销追加 `reversal`，更正追加
  `correction`；哈希链使任何底层篡改在一致性检查中暴露。
* **补偿有序性**：被消耗/移动的批次不能直接撤销其上游事件，必须按下游
  相反顺序回滚；盘亏/盘盈不能镜像撤销，需签发反向更正。
* **双向带权追踪**：拆分/合并构成有向血缘图；合并批次按并入占比分摊，
  报告每个批次/工单中归属某供应商批号的精确数量，并满足
  `归属在库 + 归属消耗 = 原始收货量` 的闭环。

## 目录

- `domain/contract.json`：领域角色、状态、约束和样例。
- `src/domain_contract/`：契约读取与确定性校验。
- `src/inventory_trace/`：
  - `models.py`：事件/行/批次模型与守恒常量；
  - `store.py`：仅追加事件存储、哈希链、投影器；
  - `service.py`：命令服务（守恒校验、并发事务、补偿事件）；
  - `lineage.py`：血缘图与双向带权追踪；
  - `consistency.py`：一致性检查；
  - `api.py`：零依赖 HTTP API（标准库 `http.server`）。
- `tools/check_contract.py`：契约摘要检查。
- `tools/recall_demo.py`：质量召回端到端演示。
- `tests/`：契约、领域、并发、补偿、防篡改、HTTP API 回归测试。

## 快速开始

```bash
# 召回场景演示（收货→拆包→合并→领料→退回→双向追踪→一致性检查）
python3 tools/recall_demo.py

# 启动 HTTP 服务（默认 127.0.0.1:8080，数据库 data/inventory.db）
INVENTORY_DB_PATH=data/inventory.db python3 -m inventory_trace.api
```

## API 示例

```bash
# 收货
curl -s -XPOST localhost:8080/api/commands/receive -d '{
  "sku":"RAW-A","quantity":"200","location":"待检区-01",
  "supplier_lot":"SUP-LOT-2026-09"}'

# 拆包
curl -s -XPOST localhost:8080/api/commands/split -d '{
  "batch_id":"B...","children":[
    {"quantity":"120","location":"原料仓-A1"},
    {"quantity":"80","location":"原料仓-A2"}]}'

# 合并
curl -s -XPOST localhost:8080/api/commands/merge -d '{
  "sources":[{"batch_id":"B1"},{"batch_id":"B2"}],"location":"配料区-B1"}'

# 领料（并发安全，超领返回 409）
curl -s -XPOST localhost:8080/api/commands/consume -d '{
  "allocations":[{"batch_id":"B1","quantity":"60"}],
  "work_order":"WO-5001","product":"成品-P1"}'

# 撤销（补偿事件，不删旧事件）
curl -s -XPOST localhost:8080/api/commands/reverse -d '{"seq":5,"note":"误领"}'

# 历史更正（盘亏/盘盈/元数据）
curl -s -XPOST localhost:8080/api/commands/correct -d '{
  "batch_id":"B1","reason":"short","quantity":"2"}'

# 正向召回：供应商批号 -> 当前库位 + 生产消耗（含带权归属量）
curl -s 'localhost:8080/api/trace/forward?supplier_lot=SUP-LOT-2026-09'

# 反向追溯：生产工单/成品 -> 供应商批号
curl -s 'localhost:8080/api/trace/backward?work_order=WO-5001'

# 任意批次双向血缘
curl -s localhost:8080/api/trace/batch/B...

# 一致性检查（哈希链、重放比对、守恒恒等式、非负库存、血缘可达性）
curl -s localhost:8080/api/consistency
```

写请求支持 `Idempotency-Key` 头（或 body 中 `event_id`）做幂等重放。

## 一致性检查内容

1. 哈希链完整（检测外部 UPDATE/DELETE/伪造）；
2. 从空状态重放全部事件，与在线投影逐行比对；
3. 逐事件数量守恒；
4. 库存恒等式：`累计收货 + 退回 + 盘盈 = 当前库存 + 累计消耗 + 盘亏`
   （已撤销事件两侧均不计入）；
5. 重放全程非负库存；
6. 补偿事件指向存在、有效、可撤销的目标；
7. 每个在库批次可追溯到供应商批号或工单来源。

## 验证

```bash
python3 -m unittest discover -s tests -v
python3 -m compileall -q src tools tests
python3 tools/check_contract.py domain/contract.json
```
