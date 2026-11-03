# C2：可信回调、查单与受控恢复交接

2026-10-28。C2 已完成签名回调、持久 Inbox、乱序处理、可信查单合并和客服受控恢复。先恢复 C1 留存的两个 UNKNOWN，再运行会重置专用测试库的工程回归。收尾发现并修复调度候选积压问题后，最终完整回归 **673 passed**，真实本机进程矩阵 **8/8 通过**。首次专项失败与各首轮结果继续保留。查询耗尽的申请保留库存并进入人工处理，没有为了完成率强行标记成功。

[冻结契约](../design/backend-C2-recovery-contract.md) · [验证清单](../benchmarks/reports/backend-C2-validation.json) · [C1 历史交接](backend-C1.md) · [总实施方案](../design/backend-fulfillment-roadmap.md)

## 实现与关键入口

- `domain/warehouse.py`：严格事件 DTO、原始字节 HMAC 签名与校验辅助函数，保留 C1 请求摘要和出库回执协议。
- `api/warehouse.py`：独立仓储签名入口、16 KiB 请求上限、仅客服可调用的 reconcile/redrive；复用 B2 身份保护。
- `services/recovery.py`：Inbox 先持久接收、语义去重、冲突保留、乱序排队和处理；回调、原投递、查单共享成功合并；查询任务与人工命令有界执行。
- `workers/reconciliation_worker.py`：独立查单 Worker，事务外 GET，支持租约接管；原 Agent Worker 保持原职责。
- `services/warehouse_http.py`、`services/warehouse_attempt.py`：可信 GET 结果及序号校验、回调 Inbox 回执校验，沿用独立短命子进程的总 deadline。
- `0010_warehouse_recovery.py`、`repositories/models.py`：增量增加 Inbox、冲突证据、查询任务、人工命令及序号/预算字段；禁止破坏性降级。
- `mock_warehouse/store.py`、`callbacks.py`、`app.py`：独立 SQLite v1→v2 迁移、稳定事件与有限回调重投；GET 正文兼容 C1，增加 `X-Warehouse-Sequence: 2`。
- `scripts/backend_c2_recover_c1.py`：只复制冻结仓储快照，通过真实 CLI 迁移、GET 恢复历史 UNKNOWN，不直接写业务状态。
- `scripts/backend_c2_http_smoke.py`：8 场景真实进程/HTTP 验收，保留进程标记、首次结果与失败证据。

原可信申请提交、数量证据、员工 session、申请版本、库存不变量、原始 triggered_by 和 Operation.receipt 继续生效。存在 ShipmentRequest 的申请仍不能切回 B1/B2 进程内模拟适配器直接确认。旧 A/B1/B2/C1 报告及 C1 冻结快照未改写。

## 一个完整机制：外部已成功，本地尚未知

```mermaid
flowchart TD
  A[出库请求与 Outbox 原子提交] --> B[Consumer 领取后事务外 POST]
  B --> C[仓储持久出库事实和两条事件]
  C --> D[可信 HMAC 回调]
  B --> E[响应模糊或租约过期：UNKNOWN，保留库存]
  E --> F[查询任务：有预算、有租约的 GET 原业务键]
  D --> G[Inbox 接收事务先提交]
  G --> H[按序处理；缺序或关联未知保留 PENDING]
  F --> I[严格校验查询绑定和 provider sequence]
  H --> J[唯一 Outbox 行作为同业务键的合并互斥入口]
  I --> J
  B -->|可信成功且仍有执行权| J
  J --> K[同连接 SAVEPOINT 调用原 confirm_shipment]
  K --> L[库存消费、申请 v6、请求成功、Outbox完成、审计统一提交]
  L --> M[旧 owner/generation 的迟到写回被拒绝]
```

`effect_key=exchange:application_id` 始终对应唯一请求和 Outbox 行；四个业务字段的 canonical JSON SHA256 固定为 request_hash。合并前再次核对键、申请、数量、目标款式、请求摘要及外部单绑定，不能把一个签名合法但关联错误的事件当成出库成功。

同一业务键的合并从 Outbox 行锁开始，再锁 ShipmentRequest、查询任务/Inbox 和原领域资源；领取查询任务不能先持任务锁再等待 Outbox，避免与回调合并形成锁环。这个互斥域与执行权校验共同约束原投递、回调和查单竞争，不依赖前端隐藏按钮。

共享 `merge_success` 在同一连接的 SAVEPOINT 内调用原领域 `confirm_shipment`，外层统一提交库存、请求、Outbox、相关 Inbox/任务和审计。已成功且绑定相同的事实不会再次消费库存；合并同时使旧执行失权。审计保留首次仓库员工 triggered_by；原 Operation.receipt 仍描述申请已提交，不被改写为出库或完成回执。

## 回调身份、去重与确认边界

回调地址为 `POST /api/integrations/warehouse/events`，不使用人类登录态代替仓储身份。必须同时满足：

- `X-Warehouse-Source: mock-warehouse-v1`。
- `X-Warehouse-Timestamp` 为 Unix 秒，处于服务端时间 ±300 秒内。
- `X-Warehouse-Signature: v1=<sha256hex>`；HMAC-SHA256 覆盖 `timestamp + "." + 原始 body bytes`，常量时间比较。
- body 最多 16 KiB，严格 DTO 禁止额外字段；sequence 为 1..2147483647 的整数。

Bearer 与回调 HMAC 使用两份不同的本地密钥，各为 32..256 字符的可见 ASCII，不含空白。HMAC 验证原始传输字节；Inbox 去重使用严格 DTO 的 canonical payload 摘要，因此 JSON 字段顺序或空白改变不构成业务内容冲突。

Inbox 首次接收事务先提交，再开启业务处理事务。处理阶段失败仍保留已接收事件。200/202 的合法 Inbox 回执只能确认产品已持久接收，不能当成业务出库完成；客户端必须验证 inbox_id 与允许的 status，回调发送器只将该类 `ACKNOWLEDGED` 记为已送达。

| 情况 | 持久处理与业务结果 |
|---|---|
| 同来源、同事件 ID、同语义内容 | 复用原 Inbox 行，不重复业务转换 |
| 同事件 ID、异内容 | 返回 409，追加去重后的冲突证据，不覆盖原事件 |
| seq2 先于 seq1 | 202/PENDING，保留 GAP；seq1 到达后处理已保存的后续事件 |
| 旧序号且事实一致 | STALE，不回退状态或再消费库存 |
| 未知业务键 | 202/PENDING，保留 UNKNOWN_REQUEST，不凭回调创建成功申请 |
| 已知键但关联/外部单冲突 | 留存 PENDING/CONFLICT，不能消费库存 |
| 来源或签名错误 | 401，身份校验失败不写 Inbox |

## 查单预算、租约与人工权限

UNKNOWN 使用 GET 查原键，不能直接重发 POST，也不能换键或释放库存。GET 404 仅表示查询时未找到，不能排除原请求仍在途；503 和其他不确定结果同样保留库存。

查询任务持久记录 owner、generation、租约、attempts、cycle_attempts、下一次执行与 deadline。默认每轮最多 3 次、60 秒截止，有限退避。过期查询可以接管，因为重复 GET 不产生出库副作用；旧 owner/generation/期限失效的完成写回被拒绝。耗尽后为 `MANUAL_REVIEW / DEAD / EXHAUSTED`，申请仍 v5、库存仍 RESERVED；普通 Worker 再运行返回 IDLE，不自动重置预算。

两个正式人工命令仅允许服务端认证的 support 会话：

| 接口 | 用途与限制 |
|---|---|
| `POST /api/fulfillment-requests/{id}/reconcile` | 只安排有界查单；人工开启新查询轮次仍受默认最多 3 轮限制 |
| `POST /api/outbox/{id}/redrive` | 只重排有明确未受理证明的可恢复死信；保留原 key/参数，默认最多 3 次人工重投 |

两者均要求 CSRF、同源保护、Idempotency-Key、expected_generation 和操作理由；复用 B2 身份共享锁，并将业务服务绑定同一连接 SAVEPOINT。事务内只排任务，不执行网络。精确重放返回原人工命令回执，不重开预算，也不改首次操作人。仓库角色返回 403；旧 generation 返回 409。

redrive 要求历史投递尝试均为明确未受理的 FAILED/RETRYABLE，且没有 provider 外部单或已接受序号。UNKNOWN 与查单耗尽但缺少未受理证明的 MANUAL_REVIEW 不允许直接 redrive。重投后的总 attempts 与 generation 单调增加，仅当前轮 cycle_attempts 重置；客服不能提交成功状态或外部单号。

保守恢复限制：如果一个请求已有 DONE 查询任务，后来经 redrive 再次变成 UNKNOWN，自动调度不会重开这个已结束的查询预算，需 support 显式 reconcile，且仍受查询轮次上限约束。

## 独立仓储持久事件

仓储仍只维护自己的 SQLite，不读取产品 PostgreSQL。v1→v2 在事务内保留已有出库事实及幂等绑定，为每条事实补 seq1 ACCEPTED、seq2 SUCCEEDED 两条稳定事件，不重新出库。新出库事实与两条事件同事务提交；事件 payload 由不可变保护约束，同业务键/序号唯一，重启后身份不变。

发送器短事务领取事件，事务外发送，再按 owner/generation/期限回写；每条事件最多 5 次，失败保留并退避，过期领取可重复发送同一个事件。不能把异常或 UNKNOWN 标为 DELIVERED。配置回调后，仓储会在出库提交后、原 POST 响应前尝试发送该键的两条事件；回调失败不撤销出库。产品回调入口和仓储发送配置默认关闭。

测试故障接口仅显式 `--test-faults` 且有效 Bearer 可用，默认不存在。受理后 500、查询不可用、提交后延迟和清除故障不改写成功绑定；延迟上限 5 秒。受理后 500 是受控响应故障，没有声称真实断开 socket。

## 实际验收顺序与结果

先执行历史恢复，后运行清库 fixture。`runtime/backend-C2/c1-recovery-first/recovery-first.json` 记录真实 CLI 迁移 0010、独立仓储 GET 与查单 Worker，**PASSED，2.87 秒**。两个 C1 UNKNOWN 均由 v5 推进至 v6；reserved 从 3→1；外部单号与 C1 人工查单一致、原回执逐值不变。原 5 条 provider 事实及 ID 不变，复制件补 10 条事件；C1 冻结快照 SHA 保持原值。第三次查单 Worker 为 IDLE，没有重复消费库存。

随后才运行专用测试库回归：

| 批次 | 实际结果 | 说明 |
|---|---|---|
| unit-first | 117 passed，3.88 秒 | 本批纯单位专项 |
| query-unit-first | 13 passed，0.02 秒 | 查询传输协议专项 |
| integration-first | 91 passed、1 failed，33.58 秒 | PG/API 专项首次失败保留 |
| integration-repair | 1 passed，0.45 秒 | 修复失败断言后的单项重验 |
| full-first | **672 passed、4 skipped、1 deselected，94.42 秒** | 完整回归首次执行 0 失败；4 项 tau2 可选依赖跳过，1 项权重推理排除 |
| HTTP-first | **8/8 PASSED，24.567 秒** | 首轮真实进程矩阵，保留原结果 |
| scheduling-regression | 1 passed，1.82 秒 | 收尾候选积压修复的专项回归 |
| full-final | **673 passed、4 skipped、1 deselected，98.66 秒** | 修复后的完整回归，0 失败；跳过/排除原因同首轮 |
| HTTP-final | **8/8 PASSED，24.613 秒** | 修复后的真实进程矩阵，未完成项为空 |

PG/API 专项失败是测试读取 `columns()` 省略的 None 字段而触发 KeyError；修复为直接检查数据库绑定值，没有通过放宽业务规则使其通过。原 `integration-first.xml/.log` 保留，不能将本批描述为所有专项首轮均通过。完整首轮新增 140 项，恢复核心为 38 项；追加调度积压回归后，最终新增 **141 项**：API 38、恢复核心 39、provider callback 34、HMAC 17、query transport 13。不把不同专项通过数相加成独立测试总数。

工程原始证据在 `runtime/backend-C2/` 的 `unit-first`、`query-unit-first`、`integration-first`、`integration-repair`、`full-first`、`scheduling-regression`、`full-final` XML/日志。真实首轮矩阵在 `runtime/backend-C2/http-first/http-first.json`，最终矩阵在 `runtime/backend-C2/http-final/http-first.json`；各目录的 `processes.jsonl` 保存对应进程生命周期。最终矩阵 `not_completed=[]`，临时员工已停用、子进程均已结束。

收尾审查发现：`ensure_pending` 的前 20 个候选若持续被历史 CONFLICT 占据，可能挡住新 UNKNOWN 调度。已筛除已有查询任务或已成功的候选，并追加积压回归，保持耗尽预算不重开。修复后的完整回归与新目录 HTTP 复验均已通过；首批 672 项、8/8 及其原始证据没有被覆盖或回写。

### 八个真实进程场景

| 场景 | 实际触发点与核对 | 最终结果 |
|---|---|---|
| before-send-exit | 实际终止仍等待 claim 的进程，替代 Consumer 执行投递，再由客户确认 | COMPLETED v7；不能据此声称“已 claim、发送前崩溃”已覆盖 |
| query-recovers-unknown | 仓储受理后返回 500，投递隔离 UNKNOWN，再 GET 查原单 | SHIPPED v6；没有改 key 或重复出库 |
| post-accept-kill | 投递探针真实 HTTP 成功后、finish 前实际 SIGKILL；租约自然失效后 GET | SHIPPED v6；实际杀进程，不用对象重建代替 |
| query-takeover | 旧查询真实 GET 后暂停，3 秒租约自然过期，新查询完成；放行旧 finish | SHIPPED v6；旧执行得到 RECONCILIATION_LEASE_LOST |
| early-callback | 仓储原 POST 响应前发出两条 HMAC 回调 | 回调先合并至 v6；原投递 CLI 随后 LEASE_LOST，库存只消费一次 |
| callback-ordering | seq2 先到为 202/GAP，重复不增行；seq1 到达后处理；旧序号 STALE；同 ID 异内容 409 留 2 条冲突；坏签名/来源 401 不写；未知键 202/PENDING；真实 sender CLI 获 Inbox 确认 | 已知申请 SHIPPED v6；未知事件与冲突继续留存，202 没有计作业务成功 |
| query-exhaustion | 连续 3 次真实 GET 503，耗尽后复跑 IDLE、预算不重开；拒绝无未受理证明的 redrive | MANUAL_REVIEW v5、DEAD/EXHAUSTED，保留 1 件预留 |
| authorized-redrive | 明确永久拒绝后，客服 CAS/幂等命令重投原 key；仓库 403、旧 CAS 409 均不改事实 | SHIPPED v6；generation 1→2→3、总 attempts=2 单调 |

这些是有明确暂停点和触发标记的受控探针，覆盖表中位置；不代表任意进程时点、所有网络断连或所有调度组合都已穷尽。

8 条业务申请最终 7 条 ShipmentRequest 为 SUCCEEDED，其中 1 条 COMPLETED v7、另 6 条 REPLACEMENT_SHIPPED v6；1 条为 MANUAL_REVIEW v5。合成库存满足 **20 = available 12 + reserved 1 + consumed 7**。各业务 key 在 provider 各有 1 行，共 8 行；本矩阵没有 C1 的 provider-only 协议记录。原回执与首次触发员工保留。异常 Inbox 和冲突证据没有为“全绿”而删除。

## 运行与复现

只使用专用 `127.0.0.1:55434/aftercare_test`。先确认没有并发清库测试或其他 Consumer，再迁移/启动。当前历史恢复已经执行，原 UNKNOWN 先决条件已不满足，不能把历史恢复脚本当作可随时重复的成功演示。保留当前数据库/仓储证据后，才运行会重置该测试库的 fixture。

手动运行时，两端隐藏输入相同的 Bearer 令牌与相同的回调密钥；这两类密钥互不相同，不写入 argv、明文命令或仓库文件。先在产品终端设置：

```bash
export DATABASE_URL=postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55434/aftercare_test
export ALLOW_REAL_MODEL_CALLS=false
export PYTHONPATH=src:.
export WAREHOUSE_DELIVERY_ENABLED=true
export WAREHOUSE_SIMULATION_ENABLED=false
export WAREHOUSE_CALLBACKS_ENABLED=true
export WAREHOUSE_BASE_URL=http://127.0.0.1:8142
export WAREHOUSE_TIMEOUT_SECONDS=2
export OUTBOX_LEASE_SECONDS=15
export OUTBOX_MAX_ATTEMPTS=3
export RECONCILIATION_MAX_ATTEMPTS=3
export RECONCILIATION_DEADLINE_SECONDS=60
export RECONCILIATION_MAX_CYCLES=3
export WAREHOUSE_TOKEN="$(.venv/bin/python -c 'import getpass; print(getpass.getpass("本地仓储 Bearer（隐藏输入）: "))')"
export WAREHOUSE_CALLBACK_SECRET="$(.venv/bin/python -c 'import getpass; print(getpass.getpass("独立回调密钥（隐藏输入）: "))')"
.venv/bin/python -m aftercare db migrate
.venv/bin/python -m aftercare serve --offline --demo-login --async-warehouse --port 8134
```

仓储终端设置回调到产品；状态文件应位于自己的 runtime 目录，不使用冻结的 C1/C2 证据文件作为日常可写状态：

```bash
export ALLOW_REAL_MODEL_CALLS=false
export PYTHONPATH=src:.
export MOCK_WAREHOUSE_TOKEN="$(.venv/bin/python -c 'import getpass; print(getpass.getpass("同一仓储 Bearer（隐藏输入）: "))')"
export MOCK_WAREHOUSE_CALLBACK_SECRET="$(.venv/bin/python -c 'import getpass; print(getpass.getpass("同一回调密钥（隐藏输入）: "))')"
export MOCK_WAREHOUSE_CALLBACK_BASE_URL=http://127.0.0.1:8134
.venv/bin/python -m aftercare mock-warehouse --state-path runtime/mock-warehouse/c2-demo.sqlite3 --port 8142
```

沿用 [B2 员工/客户操作](backend-B2.md)，在正式接口请求出库后，另开继承产品环境的终端运行投递或查单。仓储回调重试使用继承仓储环境的终端：

```bash
# 产品环境：各命令至多处理一个到期任务，IDLE 不等于业务已完成。
.venv/bin/python -m aftercare outbox-worker --once
.venv/bin/python -m aftercare reconciliation-worker --once

# 仓储环境：持久回调重试，ACK 仅代表 Inbox 接收。
.venv/bin/python -m aftercare mock-warehouse-callbacks --once --state-path runtime/mock-warehouse/c2-demo.sqlite3
```

省略 `--once` 可持续运行，退出时停止自己启动的进程并停用自己创建的临时员工；不要删除数据库卷、账本或冻结证据。新增恢复 API 已正式接线，本批没有增加复杂恢复管理前端或新增浏览器兼容性验收。

工程/矩阵复现必须串行，输出选择新目录；脚本拒绝覆盖已有目录。先创建全新证据目录，再执行：

```bash
mkdir runtime/backend-C2/reproduction-new
AFTERCARE_TEST_DATABASE_URL=postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55434/aftercare_test ALLOW_REAL_MODEL_CALLS=false PYTHONPATH=src:. .venv/bin/python -m pytest -m 'not live_model and not model_weights' --junitxml=runtime/backend-C2/reproduction-new/full.xml
DATABASE_URL=postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55434/aftercare_test ALLOW_REAL_MODEL_CALLS=false PYTHONPATH=src:. .venv/bin/python scripts/backend_c2_http_smoke.py --output-dir runtime/backend-C2/reproduction-new/http
```

`backend_c2_recover_c1.py` 默认输出 `runtime/backend-C2/c1-recovery-first`，限时 90 秒、复制原快照、只读核对 PG0009 固定列后通过真实 CLI 升级。仅在保留两条原 UNKNOWN 的数据库仍满足先决条件时，才可指定另一个新目录重现该恢复步骤；本次恢复证据已经保存，不能清库后伪造同一历史现场。

## 收尾证据与限制

临时产品 8134、仓储 8142、历史恢复仓储 8141 均已关闭；本矩阵两名临时员工已停用，业务、审计、异常 Inbox 和进程证据保留。`runtime/backend-C2/database-final.json` 的最终只读核验记录为：0010_warehouse_recovery、健康 ready、临时迁移 schema 为 0、库存及订单数量不变量违规均为 0；请求 SUCCEEDED 7 / MANUAL_REVIEW 1，Outbox DELIVERED 7 / DEAD 1，查询任务 DONE 4 / EXHAUSTED 1。Inbox 为 PROCESSED 4 / STALE 1 / PENDING 1，另保留 2 条冲突与 1 条授权人工恢复命令；PENDING 为未知业务键的异常事件，没有计作业务成功。

独立仓储 v2 有 8 条事实、8 个不同业务键，quick_check 为 ok；一致快照已保存至 `runtime/backend-C2/provider-final.sqlite3`，SHA 见验证清单。回调事件为 DELIVERED 4 / PENDING 12：多数场景刻意不启用仓储回调发送、单独验证查单，因此保留待发事件，不能称所有回调均已送达。合成库存仍为 20 = available 12 + reserved 1 + consumed 7。Ruff 检查、格式检查、pip check 与 diff check 均通过。

C2 继承 C1 的原生进程、literal loopback HTTP、无代理/重定向与独立尝试子进程边界。默认总尝试预算 2 秒包含启动/通信，超时后额外回收最多 1 秒；子进程 POSIX alarm 继续提供后备限时。任意父进程被杀后的全部子进程时序没有因此获得完整覆盖，OS 调度也不是硬实时承诺。

本批验证了稳定键、幂等约束、合并互斥、执行权围栏和库存单次消费；不声称网络端到端绝对 exactly-once、生产吞吐或 SLA。D 阶段的性能基线对照、SSE/批量查询/索引、Trace/压测、CI/容器部署尚不属于本交付。没有真实模型请求、独立 holdout/Test、依赖下载、新容器、推送或公开部署。
