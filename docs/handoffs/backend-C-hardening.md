# C 阶段可靠性补强交接

2026-10-29。继续已批准的 C1/C2，只补强换货出库的故障恢复。最终完整离线回归 **707 passed / 4 skipped / 1 deselected**，新增五项真实进程验证 **5/5 通过，27.699 秒**。D 尚未开始，未增加业务、角色、Agent 工具、依赖或容器。

[增量契约](../design/backend-C-dispatch-contract.md) · [验证清单](../benchmarks/reports/backend-C-hardening-validation.json) · [历史 C2](backend-C2.md) · [实施路线](../design/backend-fulfillment-roadmap.md)

## 修补与关键入口

原协议 claim 提交后、HTTP 前退出也会 UNKNOWN；持续查不到单时只能人工停止。`0011_dispatch_intent` 新增 NOT_STARTED、STARTED、LEGACY_UNKNOWN 和许可时间。新 Worker 先提交发送许可，再事务外 HTTP。可证明未许可且无仓储接受事实的过期尝试可有限重排原键，提升 generation 使旧者失权；其余未知情况继续查单。迁移和数据库默认均为 LEGACY_UNKNOWN，不把旧行误判成未发送。

独立复核又发现 ACCEPTED 回调可能先于许可到达。新增测试先复现“已接受后仍调用 client.deliver”，再修为同事务 UNKNOWN/BLOCKED、关闭尝试并拒发，后续交原查单。回调 ACK 还增加与发送 event_id 的精确绑定；独立仓储在取得 SQLite 写锁后采样生产时钟，防止锁等待后误收过期 finish 或返回缩水租约。

核心为 `services/outbox.py` 的 `begin_delivery/expire_leases/finish`、`workers/outbox_worker.py`、`repositories/models.py`、固定增量迁移 `0011_dispatch_intent.py`、`services/warehouse_http.py`、`mock_warehouse/store.py`。新验证为 `tests/integration/test_dispatch_intent.py`、`tests/unit/test_warehouse_reliability.py`、`scripts/backend_c_reliability_smoke.py`。

旧 C1/C2 测试仅补发送许可前提，模拟“可能已发”的场景继续按 STARTED 隔离；正向 ACK fixture 增加正确 event_id，原库存、并发及幂等断言未放宽。C1 实际发送探针也改为 begin_delivery，C2 导入该探针；查询探针仍用原查询执行权检查。

## 一个完整机制

原请求及 Outbox 原子入队后，申请 v5、库存 RESERVED。Consumer 领取并提交 NOT_STARTED，许可前被 SIGKILL。3 秒自然租约过期后，新 Worker 核对快照、发送阶段和无受理事实，关闭旧尝试为 RETRYABLE/HTTP_NOT_STARTED、提升 generation，再领取同一 key/参数/摘要。

新 Worker 提交 STARTED 后才 HTTP。仓储以持久业务键与参数绑定保留唯一事实；结果复用原共享合并入口，在同一领域事务消费库存、升 v6、完成请求及 Outbox。旧 Worker 恢复后不能获得发送许可或写回结果。

STARTED 只表示许可已提交。许可到 socket 之间退出仍存在不确定窗口，继续 UNKNOWN/查单；404 不允许重投。不声称任意崩溃都自动完成或网络 exactly-once。

## 验证与首次失败

开始时核对 C2 冻结的 159 个源码、31 个受保护文件、50 份证据及两份一致仓储快照，再保存专用测试库只读摘要 `database-before.json`，才运行清库 fixture。旧 C1/C2 报告及快照未改写。源码有意变更逐项列入新验证清单。

原始记录均在 `runtime/backend-C-hardening/`：

| 批次 | 实际结果 | 说明 |
|---|---|---|
| provider-first | 6 failed / 7 passed，1.59 秒 | 四个 ACK 绑定、两个 SQLite 真实锁等待失败，首次保留 |
| provider-repair | 121 passed，2.05 秒 | 仓储/回调相关单位回归，含已有测试 |
| provider-clock-final | 13 passed，1.45 秒 | 新单位测试复验；测试采样移至 COMMIT 前以免调度误判 |
| integration-first | 159 passed，31.59 秒 | 初版发送许可及既有 Outbox/恢复/API/传输专项 |
| full-first | 706 passed / 4 skipped / 1 deselected，102.77 秒 | 首次完整回归；随后独立复核发现 ACCEPTED 门禁缺口 |
| accepted-first | 1 failed，0.37 秒 | 精确复现已受理后仍调用 deliver |
| dispatch-repair | 21 passed，2.84 秒 | 门禁修复后全部发送许可 PG 回归 |
| full-final | **707 passed / 4 skipped / 1 deselected，103.59 秒** | 最终完整回归，无失败 |
| http-first | **5/5 PASSED，27.699 秒** | 最终源码真实进程矩阵，无未完成项 |

新增 **34 项**：发送许可/迁移/已受理门禁 21，ACK/SQLite 时钟 13。专项重叠，不相加为总测试数。4 项跳过仍因可选 tau2 未安装，1 项排除仍为权重推理，没有新增 C 阶段跳过。

完整回归后，先记录临时 fixture 余留计数，再仅清理 55434/aftercare_test 应用表并 seed，为真实矩阵准备独立起点，见 `prepare-matrix.json`。该清理针对工程 fixture；上一轮 C2 证据已归档。真实脚本自身不清库，并拒绝接管已有 PENDING/CLAIMED 工作。

收尾只读核对脚本曾因相对路径构造 SQLite URI 失败，发生在写快照前，不改业务状态。改用绝对路径后核对通过；`closing-first.json` 留存工具错误，最终记录在 `database-final.json`。

## 五项真实进程验证

| 场景 | 实际触发与结果 |
|---|---|
| true-socket-loss | 代理完整收到上游 200、316 字节，但向 Consumer 返回 0 字节并关闭连接；传输 UNKNOWN 后真实 GET 原键恢复 v6，一次消费，代理仅 1 POST |
| claimed-not-started-kill | CLAIMED/NOT_STARTED 已提交、HTTP/provider 0 时实际 SIGKILL=-9；3 秒自然过期后替代 CLI 完成，旧尝试 HTTP_NOT_STARTED、新尝试 STARTED，provider 仅 1 行 |
| stale-delivery-owner | 未许可旧进程暂停，租约过期后新 CLI 完成，再放行旧 begin_delivery；旧者 OUTBOX_LEASE_LOST/HTTP=0，代理总 POST=1，业务逐值不变 |
| armed-no-http-kill | STARTED 提交、代理 POST=0 时 SIGKILL=-9；自然过期后三次真实 GET404，MANUAL_REVIEW/DEAD/EXHAUSTED，v5/RESERVED，redrive409，复跑 IDLE 不重开预算 |
| callback-ack-kill | seq2 经真实 HMAC HTTP 获 ACK、Inbox/业务提交而 provider 事件仍 CLAIMED 时 SIGKILL=-9；等 5 秒自然租约并重启 provider 后，新 sender 重发同 event_id/hash，attempts=2/DELIVERED；Inbox 仍 2 行、审计仍 6 条，库存不重复 |

首场景另用**同一条批准业务请求**在 provider 真重启前后分别并发重放 4、6 次，共 10 次真实 POST，外部单逐值相同，仍为一份事实及两条稳定事件；同 key 改数量返回 409。这是主动幂等协议探针，UNKNOWN Worker 没有自动 POST。脚本保留逐项断言和汇总，没有逐请求抓包。

`http-first/http-first.json` 保存中间状态、CLI、身份/原回执及清理结果；`processes.jsonl` 记录 24 个受管理进程及退出码。独立 marker 保留断链、三种领取/许可暂停、旧 sender 失权和 ACK 后暂停；其中三次 SIGKILL 均为 -9。代理记录的 body SHA256 包含完整 wire 字段，领域 request_hash 仅对四个业务字段 canonical JSON 求摘要，二者口径不同。

## 最终事实、清理与限制

数据库健康、schema=`0011_dispatch_intent`，库存违规 0。5 请求为 4 SUCCEEDED、1 MANUAL_REVIEW；Outbox 为 4 DELIVERED、1 DEAD，7 次尝试为 2 NOT_STARTED、5 STARTED；查单任务 1 DONE、1 EXHAUSTED；Inbox 2 PROCESSED。

合成库存 **20 = 可用 15 + 预留 1 + 已消费 4**。停止分支仍 v5 并保留 1 件预留，没有为完成率释放或伪造出库。原 SKU 库存、原 Operation.receipt 逐值未变；原仓库员工 triggered_by 保留，成功审计由 trusted_adapter/warehouse-outbox 生成。

仓储最终 4 条事实、8 条事件，无重复键；2 事件 DELIVERED、6 PENDING。后六条属于原响应/查单完成的场景且未启用发送循环，不能称所有回调队列清空。`provider-final.sqlite3` 通过 backup API 生成一致快照及 integrity_check；原 SQLite/WAL/SHM 保留但不做稳定指纹。

临时客服/仓库账号均停用，24 个受管理子进程均结束，8135/8143/8144 端口关闭。专用数据库容器保留；没有新增或移除容器/镜像/卷。Ruff、格式、依赖和 diff 检查通过。没有真实模型、holdout/Test、提交或推送。

## 复现与下一入口

当前 schema、专用测试库就绪且无其他测试/Consumer 并行时，从项目根目录用新目录重放。脚本运行时生成临时凭证，不保存明文凭证：

```bash
DATABASE_URL=postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55434/aftercare_test \
ALLOW_REAL_MODEL_CALLS=false PYTHONPATH=src:. \
.venv/bin/python scripts/backend_c_reliability_smoke.py \
  --output-dir runtime/backend-C-hardening/http-replay-01
```

已有证据目录拒绝覆盖。pytest 会清理专用测试库，重跑前先留存当前数据库摘要与仓储快照。0011 仅在 55434/test 验证，旧 demo/55432/55433 未升级；新服务需与 0011 协调启动。

C 的本地受控故障目标已完成。下一阶段仍是 D 的可比 API/SQL 性能基线，不追加业务。本批不代表生产网络、任意崩溃点、所有调度组合或吞吐结论。
