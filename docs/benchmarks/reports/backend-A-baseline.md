# A：LangChain 售后项目基线与架构审计

日期：2026-10-27。状态：A 已完成本报告定义的现状审计、离线回归、顺序 API/SQL 与行锁基线。B1 领域与事务、B2 身份权限/API/界面、C 可靠异步、D 性能优化尚未实现。本轮没有修改产品行为、业务 schema、依赖锁或历史成绩。

实施范围采用 [A→B1→B2→C→D 收敛方案](../../design/backend-fulfillment-roadmap.md)：首轮单商户，B1 定义领域权限契约，B2 接入员工认证与正式接口，多租户留作可选后续阶段。[A 正式交接](../../handoffs/backend-A.md)记录最新复核与下一批边界。

## 1. 版本、环境与证据

- 基线提交：`ed501ed012a2f3e986319523e6cc6c2aa73ff7e3`，分支 `refactor/langchain-lite`。本轮新文档和测量脚本尚未提交。
- 产品 Python 源码聚合 SHA256：`13988c28bac25142fd3e2b07701079d1c45a871bd7c7d3d8557d019430312329`；Web 资产 hash 单列于 JSON。该聚合按脚本中的路径与文件 hash 算法生成，不包含 `__pycache__`。
- 锁文件 SHA256：`44388bf7ef0c789719a917fd2a862c30f8fbaeca40458582f88f8701c0c9aa65`；LangChain 1.4.6、LangGraph 1.2.16、checkpoint-postgres 3.2.0。
- 本机：Darwin 27.0.0 / arm64，Python 3.13.14；数据库 PostgreSQL 17.11，独立迁移容器 `aftercare-langchain-db`，`127.0.0.1:55434/aftercare_test`，迁移 `0006_experiments`。
- 14 个 API 测量档位，各 3 次预热 + 30 次正式采样，合计 420 次计量请求。使用进程内 TestClient/ASGI、真实 PostgreSQL，顺序执行，无 Worker 或模型参与测量。
- 结果为本机低负载参考，包含 Python/ASGI/连接池/数据库耗时；没有网络 HTTP QPS、长连接承载量、生产容量或 LLM 性能结论。没有进行 CPU/内存资源隔离，也没有容量压测。
- [完整指标与执行计划](backend-A-baseline.json)。首轮原始输出保留于 `runtime/backend-A/baseline-first.json`、`run-first.log`；离线回归日志与 JUnit 保留同目录。JSON 包含这些证据的 hash 和测量脚本 hash。

## 2. 当前执行架构

```mermaid
flowchart TD
  UI[客户工作台] --> API[FastAPI / 演示会话 / CSRF]
  API --> TX[业务事务：消息、意图版本、旧预览失效、job]
  TX --> DB[(PostgreSQL：业务事实与事件)]
  DB --> W[Worker：会话锁、租约、受控子进程]
  W --> A[LangChain create_agent / BusinessBoundary]
  A --> T[九项业务工具 / 服务端身份]
  T --> P[只读预览]
  P --> H[原生 HITL / PostgresSaver]
  API --> C[可信确认与唤醒入队]
  C --> H
  H --> COMMIT[资格、版本、库存、幂等与回执事务]
  COMMIT --> DB
  DB --> SSE[只读 SSE / Last-Event-ID 重放]
  SSE --> UI
```

业务数据库保存事实，Checkpoint 保存执行位置。原操作回执表示申请已提交；会话/Agent 完成并不表示未来履约已经完成。

| 当前能力 | 源码入口 | 本轮核实 |
|---|---|---|
| 原生 Agent 与人工确认 | `src/aftercare/agent/runtime.py` | create_agent、BusinessBoundary、batched HITL、PostgresSaver；保留自主工具调用 |
| 消息与任务原子写入 | `services/business.py::append_message`、`workers/queue.py` | 同事务保存消息、revision、预览失效和 job |
| 提交与回执 | `services/business.py::commit` | 可信批准、请求摘要、资格/版本复核、库存与唯一操作 |
| 任务执行权 | `workers/worker.py` | SKIP LOCKED、会话锁、owner/租约、过期接管、取消与进程截止 |
| 事件读取 | `api/routes.py::events` | 持久事件、游标、重放不触发 Agent |
| 申请终结辅助函数 | `services/demo.py::settle_application` | 已有幂等的完成/撤销及预留消费/释放；尚非正式员工履约 API |

## 3. 实际回归结果

本轮重新执行：**272 passed、4 skipped、1 deselected，28.46 秒**。4 个跳过项因独立 tau2 依赖未安装；排除的 1 项需要模型权重。产品核心业务未因数据库缺失而跳过。

覆盖迁移契约、自主工具路径、原生 HITL、可信确认及伪造批准拦截、意图/数量证据、并发幂等、最后库存争抢、回滚、丢响应恢复、Worker 租约/取消、API 身份/CSRF 与 SSE 重放。真实本机 PostgreSQL 参与事务测试；回归中的 Agent 为脚本模型。

此结果证明当前离线工程基线可复现，不代表 B/C 已完成，也不是新版本生产可靠性或真实模型成功率。此前 U4 和真实模型实验仍按其原冻结版本单列。

## 4. API 与 SQL 基线

SQL 数指绑定产品 Engine 的 `before_cursor_execute` 次数，包含请求身份查询；不记录 SQL 参数。它不包含事务协议、driver ping 等全部网络往返。每档的 SQL 数和返回形状在 30 次计量中稳定。

订单行分别采用不同 Variant；消息、预览、事件按独立维度递增。历史预览为 INVALIDATED 的合成只读数据，不代表合法待提交预览，也没有借助该数据调用提交服务。除原数据库少量初态外，新增 100 个变体、3 个订单、7 个独立会话及对应历史数据；具体规模见 JSON。

| 请求/数据规模 | SQL 次数 | 返回字节 | p50 ms | p95 ms |
|---|---:|---:|---:|---:|
| 订单详情：1 行商品 | 4 | 583 | 2.999 | 3.152 |
| 订单详情：10 行商品 | 13 | 3,998 | 6.032 | 6.484 |
| 订单详情：100 行商品 | 103 | 38,670 | 29.851 | 33.601 |
| 会话：10 消息、0 预览 | 9 | 2,529 | 4.857 | 5.044 |
| 会话：10 消息、10 预览 | 29 | 8,399 | 10.668 | 10.928 |
| 会话：10 消息、100 预览 | 209 | 61,630 | 57.999 | 62.090 |
| 会话：100 消息、0 预览 | 9 | 23,993 | 5.664 | 6.094 |
| 会话：1000 消息、0 预览 | 9 | 242,896 | 14.299 | 27.846 |
| 会话：100 历史事件 | 9 | 2,529 | 5.470 | 6.243 |
| 会话：1000 历史事件 | 9 | 2,550 | 5.349 | 5.477 |
| SSE：10 历史事件，首批 10 | 9 | 2,260 | 5.492 | 5.637 |
| SSE：100 历史事件，首批 100 | 9 | 22,690 | 5.996 | 6.147 |
| SSE：1000 历史事件，首批 100 | 9 | 22,790 | 6.187 | 6.921 |
| SSE：已读到末尾，空重连 | 9 | 0 | 5.413 | 5.595 |

### 已确认的查询放大

1. 订单详情为 `3 + 商品行数` 条应用 SQL：身份、订单、明细各一次，100 行对应 100 次 Variant 查询，总计 103 条。当前 `services/business.py::get_order` 在明细循环中逐个调用 `s.get(Variant, ...)`。
2. 会话历史预览为 `9 + 2 × 预览数` 条应用 SQL：100 个预览时有 200 次 Variant 查询，总计 209 条。当前 `snapshot` 为颜色与尺码分别调用 `s.get`，本次实际执行没有得到期望中的对象复用。
3. 消息由 10 增至 1000 条时，SQL 数仍是 9，但完整响应由 2,529 增至 242,896 字节。查询数量稳定不等于加载/序列化成本稳定。
4. SSE 末尾空重连仍执行 9 条 SQL，原因包括身份、初始完整 snapshot、二次会话校验及事件查询。1000 历史事件档位只读取首批 100 条，不能称已重放全部 1000 条。

### SSE 刷新放大边界

源码 `web/static/app.js::openEvents` 对每条 trace 调用 `refresh()`；`refresh` 请求完整 snapshot，服务端仍会执行该请求，即使前端随后丢弃过期返回值。首批重放 N 条事件会安排 N 次完整 snapshot 请求，这是源码行为，尚未进行真实浏览器并发或长连接压测。不要把当前顺序档位耗时相加，写成浏览器端到端延迟。

服务端每个活跃连接约每 0.5 秒执行一次事件加载；每轮还有会话校验。真实连接数、连接池压力、锁等待和同会话写延迟需要 D 用本机 HTTP 负载测量。

## 5. 行锁实验与 snapshot 依赖

独立 Holder 先成功执行目标会话的 `SELECT ... FOR UPDATE` 并保持事务。另一个连接分别运行普通 SELECT、加锁 SELECT、UPDATE 和实际 `BusinessService.snapshot`；观察连接记录 `pg_stat_activity` 与 `pg_blocking_pids`。

实验连接的 `lock_timeout=300ms`、`statement_timeout=2000ms`；**这是为探针缩短的超时**。产品 session_factory 当前默认分别为 5000ms、10000ms，不能把 300ms 写成产品超时配置。

| 竞争操作 | 结果 | SQLSTATE | 实测 ms | 数据库等待证据 |
|---|---|---|---:|---|
| `plain_select` | ok | — | 5.115 | 未观察到锁等待 |
| `locking_select` | database_error | 55P03 | 303.717 | Lock / transactionid / 1 blocker |
| `update` | database_error | 55P03 | 303.553 | Lock / transactionid / 1 blocker |
| `product_snapshot` | database_error | 55P03 | 305.179 | Lock / transactionid / 1 blocker |

Holder rollback 后 snapshot 成功。UPDATE 为 `revision=revision` 且未提交；测试没有改变既有会话 revision。

结论：普通读取没有被该行锁阻塞；当前 snapshot 因复用加锁路径会参与同一行锁竞争。该区别与 [PostgreSQL 17 行锁说明](https://www.postgresql.org/docs/17/explicit-locking.html#LOCKING-ROWS)一致。此次只是确定性竞争复现，没有测量真实负载中锁等待的发生率，也没有证明当前系统已出现普遍性能故障。

不能直接删除 `_conversation` 的锁：它还执行归属校验和可选 `run_guard`；Worker 的 guard 对 job 取共享锁并检查 owner/status/cancel/expiry。`BusinessBoundary` 也用 snapshot 获取最新意图与数量证据。D 先拆外部只读 API，内部 Agent/写路径的执行权保护继续保留。

外部 API 在 snapshot 后另开事务补 jobs/operations，当前整个响应也不是单一原子数据库快照。去锁前应选择明确的一致性方案并验证改口、确认、提交和租约接管，不能为了低延迟产生混合版本或假成功。

本轮尚未执行反方向的“长 snapshot 阻塞写入”负载、不同会话对照、连接池耗尽、读中改口等性能专项；原并发正确性测试通过不替代这些测量。

## 6. 执行计划与优化优先级

保存 4 份 `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`：100 行订单明细、100 历史预览、1000 事件取首批 100、1000 事件中查 runtime_state。

本次小型合成数据下订单明细/预览使用顺序扫描；事件使用 conversation_id 索引后过滤/排序。没有执行 ANALYZE，估算行数与合成实际行数可能不符；小表顺序扫描本身不是缺陷，不能据此宣称“缺索引”。D 需要固定更大的数据集、统计信息和请求分布后再决定索引。

优先级：

1. 批量读取订单/预览变体，用相同返回语义验证查询次数改善。
2. 外部只读快照与内部执行快照职责分离，保留身份、租约与版本保护。
3. 消息/会话/事件分页、增量读取和前端事件合并刷新。
4. 真实本机 HTTP/SSE 连接负载、连接池与锁等待、任务吞吐；测得证据后再调整索引/通知机制。

B/C 仍按 [实施方案](../../design/backend-fulfillment-roadmap.md)先完成业务闭环与可靠异步；本报告不提前实施 D 的产品优化。

## 7. 隔离、清理与复现

脚本只接受 `127.0.0.1:55434/aftercare_test`、aftercare 角色及无额外连接参数的 URL；预检当前迁移和其他活跃连接。没有启动、停止或修改其他容器，没有迁移/清空 demo 或访问旧 55432/55433 数据。

测量只新增 UUID 字面前缀记录和本轮专用会话。finally 按转义后的字面前缀删除，并删除专用 session；相关 12 张表行数清理前后恢复一致。Event 使用显式高位 ID，没有推进公共事件序列。该检查是表行数恢复，不宣称全库逐字节 hash 不变。后续完整回归按原测试约定会重置专用 aftercare_test。

复现基准（需已有专用测试库与 demo seed；不运行期间并发测试/Worker）：

```bash
ALLOW_REAL_MODEL_CALLS=false PYTHONPATH=src:. .venv/bin/python scripts/backend_baseline.py --samples 30 --output runtime/backend-A/reproduction.json
```

本轮离线回归命令：

```bash
AFTERCARE_TEST_DATABASE_URL=postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55434/aftercare_test ALLOW_REAL_MODEL_CALLS=false PYTHONPATH=src:. .venv/bin/python -m pytest -m 'not live_model and not model_weights' --junitxml=runtime/backend-A/offline-suite-first.xml
```

首次结果原样保留；新复现应使用新输出文件名，不覆盖 `*-first.*`。本轮真实模型请求为 0，没有运行独立 holdout/Test、推送或公共部署。

## 8. 正式验收复核

用户明确从 A 开始并拆分 B1/B2 后，重新核对产品、锁文件、Web、脚本和首次证据，全部匹配；`pip check` 通过。只读事务核对当前测试库仍为 `0006_experiments` 且具有 pgvector，并检查后续 B1 相关字段与约束；没有修改数据库结构或数据。

本次未重复运行未变化的基准或完整套件。安装版本、32 项字段与 22 项约束的只读盘点、证据 hash 见 [正式验收 JSON](backend-A-acceptance.json)。首次基准 JSON 和测试日志保留原样。
