# 电商售后 Agent 与可靠换货履约系统

面向单商户模拟售后，将多轮对话接入可审计、可恢复的业务流程：**诉求澄清 → 操作预览 → 客户明确确认 → 客服审核 → 仓库收货 → 换货出库 → 客户确认收到**。

当前公开范围为 **A—D4 已验收实现**，数据库迁移为 `0011_dispatch_intent`。Agent 使用 LangChain `create_agent`、原生 HITL 与 LangGraph/PostgreSQL Checkpoint；产品依赖已移除 Deep Agents。默认采用脚本模型，运行工作台不需要模型 API Key。

> **2026-11-03 发布复验状态：925 passed / 9 failed / 4 skipped / 1 deselected，另 9 项 JavaScript 测试通过。** 下文 934 项通过是 D4 历史验收，不代表此次复验通过。当前失败集中在日期敏感的 Agent/Worker 测试及评测预检环境隔离，尚待修复复验；首次失败记录保留，详见[本次发布说明](docs/releases/2026-11-03-publication.md)。

## 已实现能力

- **可信提交**：9 项业务工具、政策检索、诉求版本与数量证据；提交前独立重验身份、资格、确认和库存，页面以持久回执展示业务结果。
- **三角色履约**：客户、客服、仓库的服务端身份与权限；版本化换货状态机、命令幂等、库存预留/消费、事务回执及审计。客户登录为显式开启的本地演示入口。
- **可靠异步出库**：业务与 Outbox 同事务持久化，`SKIP LOCKED` 多 Worker 领取，租约、执行代次与发送许可保护；独立 HTTP 模拟仓储持久保存幂等出库事实。
- **异常恢复**：HMAC 回调、Inbox 去重与乱序处理、UNKNOWN 查单、有界重试和受控人工恢复。无法证明未出库时保留库存，不盲目重投。
- **查询与交互**：批量关联读取、兼容分页、独立只读快照、SSE 断点回放、空闲退避与前端刷新合并，内部 Agent/Worker 执行锁保持。
- **工程交付**：增量迁移、离线回归、多进程故障探针、关联日志、CI 门禁和独立 Docker Compose 栈。

技术栈：Python 3.13、FastAPI、LangChain 1.4.6、LangGraph、PostgreSQL 17/pgvector、SQLAlchemy、Alembic、SSE、原生 JavaScript、pytest、Docker Compose。独立模拟仓储使用 SQLite 保存自身事实与事件。

```mermaid
flowchart LR
  UI[客户 / 客服 / 仓库界面] --> API[FastAPI：身份、确认、权限]
  API --> DB[(PostgreSQL：业务、库存、回执、Outbox / Inbox)]
  DB --> Agent[Agent Worker：LangChain + HITL]
  Agent --> Service[业务服务：资格、版本、事务]
  Service --> DB
  Agent --> CP[(PostgreSQL Checkpoint)]
  DB --> Worker[Outbox / 查单 Worker]
  Worker --> Warehouse[独立模拟仓储 HTTP + SQLite]
  Warehouse -->|HMAC 回调| API
  DB -->|SSE 事件重放| UI
```

Checkpoint 保存执行位置，业务数据库保存已发生的事实。网络调用在数据库事务外进行；响应丢失后查询原业务键，重复投递依靠持久幂等约束收敛。取消、退货沿用原申请能力，完整可靠履约链主要覆盖换货；不连接真实支付、物流或商业仓储。

## 已有验收与性能数据

以下为 **2026-10-30 D4 冻结版历史验收**：完整离线回归 **934 passed / 4 skipped / 1 deselected**，另有 **9 项 JavaScript 测试通过**。4 项跳过为未安装可选 tau2 依赖，1 项排除为模型权重推理，新增后端测试没有以数据库缺失为由跳过。原始日志、数据库快照及密钥只保存在本地 `runtime/`，公开报告提供结果、条件和指纹。

| 验证 | 实测结果 | 条件与边界 |
|---|---|---|
| 百行订单详情 | SQL **103 → 4**；p95 **36.456 → 9.312 ms** | D1 同固定数据、真实 PG 17.11、单并发 ASGI，每档 3 次预热、30 次采样；完整 JSON 与字节数不变 |
| 百个历史预览 | SQL **209 → 10**；p95 **58.666 → 9.640 ms** | 同 D1 对照条件；不跨旧 A 或 Deep Agents 版本归因 |
| 1000 事件回放 | **5.139 → 1.441 秒** | D2 本机真实 TCP 单次回放；SQL **27 → 34**，不代表全面减少查询或生产容量 |
| 空闲 SSE | 单连接 3.2 秒窗口 SQL **21 → 10** | 含启动鉴权；退避会增加事件发现延迟 |
| 多进程竞争与接管 | D3 **5/5** 真实探针通过 | 包含争抢、SIGKILL、自然租约过期和旧 Worker 失权；验证范围内单次出库、库存一致 |
| 容器持久化 | D4 待执行/成功态整组重建后快照一致 | 保留 PostgreSQL 与仓储卷；同键仍一份出库事实，重复回调不再消费库存 |

负结果同样保留：部分只读快照与 SSE 档位 p95 变慢；消息分页完整遍历比一次全量读取更贵；关联日志没有带来吞吐提升。详见 [D1 性能报告](docs/benchmarks/reports/backend-D1-performance.md)、[D2 性能报告](docs/benchmarks/reports/backend-D2-performance.md)、[D3 性能报告](docs/benchmarks/reports/backend-D3-performance.md) 和 [D4 验收报告](docs/benchmarks/reports/backend-D4-acceptance.md)。

## 新机器启动脚本演示

需要 Python 3.13 和 Docker Compose。以下命令在项目根目录执行，依赖安装与首次镜像准备需要联网；应用运行仍默认禁止非本机网络和真实模型调用。

```bash
python3.13 -m venv .venv
.venv/bin/python -m pip install --require-hashes -r requirements.lock
.venv/bin/python -m pip install --no-deps --no-build-isolation -e .

# 新演示项目：默认数据库仅绑定本机 55435。
docker compose up -d db --wait
export DATABASE_URL=postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55435/aftercare_demo
export ALLOW_REAL_MODEL_CALLS=false
.venv/bin/python -m aftercare db migrate
.venv/bin/python -m aftercare demo seed --database demo
.venv/bin/python -m aftercare doctor --offline

# 本地客户入口与进程内模拟出库均需显式启用。
.venv/bin/python -m aftercare serve --offline --demo-login --simulate-warehouse --port 8130
```

第二终端在同一目录启动脚本 Agent Worker：

```bash
export DATABASE_URL=postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55435/aftercare_demo
export ALLOW_REAL_MODEL_CALLS=false
.venv/bin/python -m aftercare worker --provider scripted
```

访问 <http://127.0.0.1:8130>，以演示客户体验订单查询、换码及明确确认；三角色页面位于 `/fulfillment`。员工账号通过本地 CLI 创建，命令会隐藏输入密码：

```bash
.venv/bin/python -m aftercare staff create --username support-demo --role support
.venv/bin/python -m aftercare staff create --username warehouse-demo --role warehouse
```

脚本演示的 `--simulate-warehouse` 使用进程内替身。要体验 **独立 HTTP 仓储、Outbox、查单和签名回调**，使用 [后端容器运行指南](docs/operations-backend.md)：独立 `compose.backend.yaml` 默认数据库为 **55436**、应用为 **8138**，包含增量迁移与持久卷。两种出库模式互斥；不要给已有异步请求切换回模拟确认。

本地数据库口令是专用演示值，不用于真实账户。不要将演示服务暴露到互联网，也不要执行 `down -v` 或重置含业务/预算记录的旧数据库。

## 离线测试

测试会清理应用表，必须使用独立的 **127.0.0.1:55434/aftercare_test**，不可替换为演示库、旧 55432/55433 或真实账本。首次可准备一个专用容器；若端口已有本项目测试库，核对后复用，不重复创建：

```bash
docker run -d --name aftercare-langchain-test-db \
  -e POSTGRES_USER=aftercare -e POSTGRES_PASSWORD=aftercare_local_only \
  -e POSTGRES_DB=aftercare_test -p 127.0.0.1:55434:5432 \
  pgvector/pgvector:pg17@sha256:ac08538c6f8b9904c33c8224c5e5706dbe760aca29db1d096972b4052c22a75d

export DATABASE_URL=postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55434/aftercare_test
export AFTERCARE_TEST_DATABASE_URL="$DATABASE_URL"
export ALLOW_REAL_MODEL_CALLS=false
export PYTHONPATH=src:.
.venv/bin/python scripts/backend_d4_ci_check.py target --output runtime/local-check/target.json
.venv/bin/python -m aftercare db migrate
.venv/bin/python scripts/backend_d4_ci_check.py preflight --output runtime/local-check/preflight.json
.venv/bin/python -m pytest -m 'not live_model and not model_weights' \
  --junitxml=runtime/local-check/full.xml
.venv/bin/python scripts/backend_d4_ci_check.py audit \
  --junit runtime/local-check/full.xml --output runtime/local-check/audit.json
node --test tests/web/test_refresh_queue.mjs
```

运行前等待测试库就绪，测试不得与其他测试/Worker 并行。证据输出拒绝覆盖已有文件，重跑使用新目录。CI 同样要求数据库可用、完整回归、18 个关键测试模块及已知跳过口径，配置见 [offline-checks.yml](.github/workflows/offline-checks.yml)。D4 历史交付时云 CI 尚未运行；当次结果以 GitHub Actions 记录为准，不由本地通过结果替代。

## 文档与范围

- [实施路线](docs/design/backend-fulfillment-roadmap.md)、[LangChain 迁移架构](docs/design/langchain-lite-architecture.md)。
- [领域模型与库存事务](docs/handoffs/backend-B1.md)、[身份权限与三角色 API](docs/handoffs/backend-B2.md)。
- [Outbox 与独立仓储](docs/handoffs/backend-C1.md)、[Inbox / UNKNOWN 恢复](docs/handoffs/backend-C2.md)、[发送许可补强](docs/handoffs/backend-C-hardening.md)。
- [D1 查询优化](docs/handoffs/backend-D1.md)、[D2 只读快照与 SSE](docs/handoffs/backend-D2.md)、[D3 多 Worker](docs/handoffs/backend-D3.md)、[D4 CI / Docker](docs/handoffs/backend-D4.md)。
- [后端运行指南](docs/operations-backend.md)、[D4 验证清单](docs/benchmarks/reports/backend-D4-validation.json)、[评测工具说明](evaluation/README.md)。

D5 诊断与指标草稿未纳入本次公开实现。聊天中的模拟模型评测数字不属于项目成果；本次后端升级没有新的真实模型评测、Token 节省或生产成功率结论。真实模型实验必须单独冻结模型版本、场景、源码和资源上限，旧批准不自动沿用。

`docs/experiments`、旧阶段交接和公开评测报告保留 Deep Agents 等历史版本的首次结果与失败，用于追溯，不代表当前 LangChain 版本性能。当前交付不包含多租户、真实物流/支付、生产高可用、跨平台认证或完整跨系统 Trace。
