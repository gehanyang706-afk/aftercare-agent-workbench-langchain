# C1：事务 Outbox 与独立模拟仓储交接

2026-10-28。本批完成 C1：合法换货出库命令原子入队，由独立 Consumer 通过真实本机 HTTP 投递到独立持久化模拟仓储。工程回归与本批真实进程验证通过。**UNKNOWN 的隔离已验证，自动查单合并和完整恢复属于尚未实现的 C2。**

[冻结契约](../design/backend-C1-outbox-contract.md) · [验证清单](../benchmarks/reports/backend-C1-validation.json) · [总实施方案](../design/backend-fulfillment-roadmap.md)

## 实现与关键入口

- `services/fulfillment.py`：默认保留 B1/B2 模式；启用 async_delivery 时，request_shipment 的状态、版本、唯一 ShipmentRequest、Outbox、命令回执和审计同事务提交。精确重放返回原回执，不增加事件。
- `repositories/models.py`、`0009_warehouse_outbox.py`：新增 ShipmentRequest、WarehouseOutbox、OutboxAttempt，固定增量 DDL、唯一约束与不可变 snapshot 触发器；不回填旧申请、不自动发旧出库，禁止破坏性降级。
- `services/outbox.py`：领取、发送前复核、完成、租约过期隔离及所属授权查询；保留 generation/attempt 证据。
- `workers/outbox_worker.py`：独立 Consumer，不修改原 Agent Worker。领取事务结束后才 HTTP，网络结果再由单独业务事务落库。
- `services/warehouse_http.py`、`services/warehouse_attempt.py`：本机 HTTP 协议、严格结果分类与总耗时保护。
- 独立模拟仓储进程：自己的 SQLite 持久状态，不读写产品 PostgreSQL；同键同参返回原单，同键异参拒绝，重启后保留事实。
- 正式 HTTP 接入：复用 B2 session→Actor 和认证事务保护；出库状态可通过 `GET /api/fulfillment-requests/{id}` 查询，返回所属可见的安全字段，不返回 payload、租约 owner 或原始异常。

原 Agent、原 Worker、可信确认、数量证据、预算保护与原业务提交服务保持原边界，旧测试业务断言没有修改。原 A/B1/B2 报告与首次证据不回写。

## 一个完整机制：从出库请求到库存消费

```mermaid
flowchart TD
  A[仓库员工通过会话请求出库] --> B[原业务事务：状态v5 + ShipmentRequest + Outbox + 回执/审计]
  B --> C[Consumer短事务：SKIP LOCKED领取，owner/generation/租约]
  C --> D[提交领取；发送前复核]
  D --> E[事务外 HTTP；稳定effect_key与request_hash]
  E --> F[独立仓储SQLite：持久幂等绑定]
  F --> G{结果是否可信且执行权有效}
  G -->|成功| H[同连接SAVEPOINT调用原confirm_shipment]
  H --> I[库存消费 + 请求成功 + Outbox完成 + 审计统一提交]
  G -->|可能受理或租约过期| J[UNKNOWN/BLOCKED，保留库存，不自动重发]
```

全程使用稳定 `effect_key=exchange:application_id`；四个业务字段的 canonical JSON SHA256 为 request_hash。请求和 Outbox snapshot 在数据库中不可变，领取、发送前与完成时仍复核关联和摘要。异常 snapshot 隔离为 DEAD/MANUAL_REVIEW，不能发送 HTTP。

锁序为 Outbox → ShipmentRequest → order/application/line/fulfillment/reservation/inventory。过期处理逐条短事务，避免跨多个订单长期持有库存锁。完成在锁后重验 status、owner、generation、期限，再绑定同一连接的 SAVEPOINT 调用 B1；外层统一提交请求/事件/attempt 与业务变化。成功审计保留原仓库员工 triggered_by，原 Operation.receipt 不改写。

HTTP 之前的复核可以拒绝已明显失权的旧执行，不能撤销已发出的请求，也不能消除检查后进程被暂停的窗口；外部重复由独立仓储持久幂等约束控制。不得据本批本地 fencing 声称端到端 exactly-once。

| 情况 | 本地结果 | 自动行为与库存 |
|---|---|---|
| 有效成功凭据且租约有效 | SUCCEEDED / DELIVERED，申请推进至 v6 已出库 | 只消费一次预留；客户另行确认后 v7 完成 |
| 明确暂未受理 | PENDING | 有限退避 1、2 秒，默认最多 3 次；保留预留 |
| 暂未受理重试耗尽 | MANUAL_REVIEW / DEAD | 停止；保留预留 |
| 明确永久拒绝 | FAILED / DEAD | 停止；保留预留，等待后续受控处理 |
| 模糊失败/可能已受理 | UNKNOWN / BLOCKED | 不自动 POST、不释放预留 |
| 领取后租约过期 | UNKNOWN / BLOCKED | 不把过期任务重新排回 PENDING |

ShipmentRequest 存在即表示该申请采用异步投递；之后切回进程内模式也不能让原模拟适配器直接确认。C1 HTTP 模式与 B2 原本地模拟出库开关互斥。

## 实际验收记录

| 批次 | 结果 | 范围 |
|---|---|---|
| unit-first | 97 passed，0.36 秒 | 当时的单位专项；早版 provider 为 39 项 |
| integration-first | 33 passed，12.44 秒 | Outbox 核心与 API 专项 |
| deadline-first | 8 passed，6.62 秒 | 总 deadline 与 CLI 等专项 |
| full-first | **532 passed、4 skipped、1 deselected，73.93 秒** | 完整首轮，0 失败；4 项缺 tau2，1 项权重推理排除 |
| 真实本机 TCP/CLI | **5 个场景 PASSED，11.256 秒** | 独立产品 HTTP、Consumer 与仓储进程，实际持久化与杀进程 |

完整回归新增 141 项：Outbox API 16、核心 17、provider 42、配置 39、client 19、deadline 5、CLI 3。专项来自各自执行时版本，不把不同批次通过数相加，也不将早版 provider 39 改写为最终 42。首轮验收均通过，没有业务失败需要隐去；静态格式修复不记作业务失败。

原始工程证据在 `runtime/backend-C1/unit-first.xml`、`integration-first.xml`、`deadline-first.xml`、`full-first.xml`；真实进程结果在 `runtime/backend-C1/http-first/http-first.json`，进程生命周期另见同目录 `processes.jsonl`。

### 五个真实进程场景

下表 provider 数量为对应业务键的持久记录数，不是整个 SQLite 的行数。

| 场景 | 实际触发 | 最终申请/本地结果 | attempt / provider | 库存与结论 |
|---|---|---|---|---|
| normal | 真实 HTTP 投递成功，再由客户确认收到 | COMPLETED v7；SUCCEEDED / DELIVERED | 1 / 1 | 预留消费一次，原回执不变 |
| accepted-lost | 仓储先持久化受理，再返回 HTTP 500 | REPLACEMENT_PENDING v5；UNKNOWN / BLOCKED | 1 / 1 | 保留预留；再次运行 Consumer 不重发；尚未合并成功 |
| retryable | 首次 HTTP 503 明确未受理，随后第二次成功 | REPLACEMENT_SHIPPED v6；SUCCEEDED / DELIVERED | 2 / 1 | 只有第二次产生仓储记录，库存消费一次 |
| permanent | HTTP 422 明确永久拒绝且未受理 | REPLACEMENT_PENDING v5；FAILED / DEAD | 1 / 0 | 保留预留，不假报成功 |
| killed | 仓储已成功、尚未本地 finish 时，实际 SIGKILL 投递探针进程 | REPLACEMENT_PENDING v5；UNKNOWN / BLOCKED | 1 / 1 | 租约过期后隔离，保留预留；未做 C2 恢复 |

accepted-lost 的故障是“受理后返回 500”，没有冒充真实 socket 断开。killed 场景实际子进程 PID 为 5404，退出码 −9；标记文件 `accepted-before-finish.json` 记录 HTTP 成功后、finish 前的位置。该子进程调用生产 OutboxService 和 HTTP client，显式暂停在 finish 前；它是受控故障探针，不代表任意 CLI Worker 位置的完整故障覆盖。场景使用 3 秒测试租约并等待自然过期，不手写 owner 或状态模拟接管。

另有独立 provider 协议验证：两个并发 POST 对同键产生 1 条记录，异参返回 409；仓储进程从 PID 5387 重启为 5391 后，原幂等结果和 normal 记录仍存在。这是 provider 层证据，不声称两个产品 Consumer 已覆盖完整 C2 故障矩阵。

五条产品链的原 Operation.receipt 均未改变。独立合成目标库存初始 20，最终 available=15、reserved=3、已消费=2；三个未完成请求的预留保留。provider SQLite 共 5 条事实，其中 1 条是 provider-only 协议记录，不参与产品库存变化。

## 运行方式与凭据边界

只使用已迁移的专用 `127.0.0.1:55434/aftercare_test`；演示时不要并发跑清库测试或原有 Consumer。准备同一个 32—256 字符、无空白的可打印 ASCII 随机本地令牌，以下两个终端分别隐藏输入同一值。不要将令牌写成明文 argv、shell 历史或仓库文件。

终端一：独立仓储。命令替换捕获隐藏输入结果，不将令牌作为参数传给进程。

```bash
export ALLOW_REAL_MODEL_CALLS=false
export PYTHONPATH=src:.
export MOCK_WAREHOUSE_TOKEN="$(.venv/bin/python -c 'import getpass; print(getpass.getpass("仓储本地令牌（隐藏输入）: "))')"
.venv/bin/python -m aftercare mock-warehouse --state-path runtime/mock-warehouse/state.sqlite3 --port 8140
```

员工登录与操作沿用 [B2 身份/API 交接](backend-B2.md)。当前临时验收员工已停用；若要手动演示，先用 `staff create --username c1-support-demo --role support` 与 `staff create --username c1-warehouse-demo --role warehouse` 建立自己的本地账号，CLI 会隐藏输入密码，并返回供后续停用的 staff ID。

终端二：产品服务与 Consumer 使用同一组环境。令牌必须与终端一相同；新环境默认关闭进程内模拟，显式开启异步模式。

```bash
export DATABASE_URL=postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55434/aftercare_test
export ALLOW_REAL_MODEL_CALLS=false
export PYTHONPATH=src:.
export WAREHOUSE_TOKEN="$(.venv/bin/python -c 'import getpass; print(getpass.getpass("同一仓储令牌（隐藏输入）: "))')"
export WAREHOUSE_BASE_URL=http://127.0.0.1:8140
export WAREHOUSE_DELIVERY_ENABLED=true
export WAREHOUSE_SIMULATION_ENABLED=false
.venv/bin/python -m aftercare db migrate
.venv/bin/python -m aftercare serve --offline --demo-login --async-warehouse --port 8133 &
C1_APP_PID=$!
```

员工按 B2 的隐藏输入 CLI 创建，客户仍从原工作台预览并可信确认申请。员工在正式接口完成审核、收货及请求出库后，在终端二执行：

```bash
.venv/bin/python -m aftercare outbox-worker --once
```

`--once` 会处理过期租约并至多领取一个到期事件；遇到尚未到退避时间或 BLOCKED/DEAD 时可以返回 IDLE，不等于自动恢复未知请求。产品 HTTP 与仓储端口分别为 8133、8140。当前模式面向同一宿主机的原生进程，不提供跨容器 loopback 连接；相关部署演进留待 D。

结束演示时在终端一停止仓储，在终端二停止自己启动的产品 PID，并停用临时员工；不删除数据库卷或真实账本。

```bash
kill "$C1_APP_PID"
wait "$C1_APP_PID"
unset WAREHOUSE_TOKEN
```

复现测试使用新证据目录/文件名，不覆盖 `*-first.*`。本批真实进程脚本生成独立合成订单与库存，保留实际业务轨迹；它没有把 UNKNOWN 场景清成成功。真实模型请求为 0，没有运行独立 holdout/Test，没有下载、新容器、推送或公开部署。

完整回归沿用既有 fixture，会重置专用 aftercare_test；应先保留证据并停止所有使用该库的服务，不连接旧 demo/真实账本库。复现命令如下，测试和真实进程脚本串行执行：

```bash
AFTERCARE_TEST_DATABASE_URL=postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55434/aftercare_test ALLOW_REAL_MODEL_CALLS=false PYTHONPATH=src:. .venv/bin/python -m pytest -m 'not live_model and not model_weights' --junitxml=runtime/backend-C1/reproduction.xml
DATABASE_URL=postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55434/aftercare_test ALLOW_REAL_MODEL_CALLS=false PYTHONPATH=src:. .venv/bin/python scripts/backend_c1_http_smoke.py --output-dir runtime/backend-C1/http-reproduction
```

## 总 deadline 的实际边界

Consumer 使用 BoundedWarehouseClient：默认 2 秒的父进程 monotonic 总预算包含子进程启动与管道通信，不只是每次 socket 读取的 timeout。超时会 kill 并 wait，额外回收等待上限为 1 秒；输入、输出和错误码有界，不输出 provider token 或原始异常。

子进程另有 POSIX alarm，用于限制持续滴流等情况；父进程意外被杀后依靠子 alarm 自行终止的场景尚未实测。OS 进程创建/调度不是硬实时保证，也没有将本批执行耗时当作启动开销或吞吐的性能测量。

killed 场景证明的是“HTTP 已成功、Consumer 尚未 finish 时杀进程”的保守隔离，不替代上述父子 deadline 场景，也不证明未知结果已恢复。

## 最终只读核验与收尾

`runtime/backend-C1/database-final.json` 记录：0009_warehouse_outbox、健康 ready、迁移临时 schema 为 0、库存及订单数量不变量违规均为 0；Outbox 为 DELIVERED 2、BLOCKED 2、DEAD 1，请求为 SUCCEEDED 2、UNKNOWN 2、FAILED 1。provider quick_check 为 ok，5 条事实/5 个不同业务键。

8133/8140 已关闭，本轮两个临时员工已停用，原业务、审计、session 与证据保留，不称为清空测试数据。独立 provider 快照在 `runtime/backend-C1/provider-final.sqlite3`；首次进程现场 SQLite 在 `runtime/backend-C1/http-first/warehouse.sqlite3`。Ruff 检查与 264 文件格式检查、pip check、diff check 均通过。

## C2 交接与已知限制

C1 只完成安全投递与不确定结果隔离。未实现 Inbox、签名回调、回调去重/乱序、自动查单、reconcile/redrive、UNKNOWN 与迟到成功合并，也未执行完整恢复矩阵。人工 provider GET 只用于验收核实外部事实，没有偷偷推进本地 UNKNOWN。

C2 应沿用稳定请求键、不可变 snapshot、原始 triggered_by、原 Operation 回执、锁序与库存语义，设计可信查单/回调的合并入口；先证明租约失效与迟到结果不会重复消费或释放库存，再提供受控恢复。保留本批两个 UNKNOWN 原始案例，不能将隔离通过写成恢复完成，也不能为了新阶段成绩回写本批结果。
