# B1：换货领域模型与事务服务交接

2026-10-27。范围为用户批准的 B1：领域模型、增量迁移、申请版本、库存不变量、命令幂等、服务层权限与事务测试。B2 的员工认证、正式 API、前端及 C 的可靠异步尚未实现。

**B1 已实现并验收通过。** 实现与验收范围以 [B1/B2 实施方案](../design/backend-fulfillment-roadmap.md)及 [B1 领域契约](../design/backend-B1-domain-contract.md)为准。源码、测试和原始证据指纹见 [B1 验证清单](../benchmarks/reports/backend-B1-validation.json)；本批文件尚未提交或推送。

## 实际验证

| 验证 | 首次结果 | 边界 |
|---|---|---|
| 首轮领域/迁移/旧业务专项 | 74 passed，8.53 秒 | 原始日志与 JUnit 保存在 `runtime/backend-B1/focused-first.*` |
| 最终完整离线回归 | 332 passed、4 skipped、1 deselected，39.24 秒 | 新增 57 项领域测试、3 项迁移测试；原 272 项工程基线继续通过 |
| 3 项补充边界 | 全部包含在最终完整回归 | 模拟仓储、跨申请出库单号冲突回滚、不同订单共享库存的撤回/出库并发 |
| 全仓静态与格式检查 | 通过；237 files already formatted | `ruff check .`、`ruff format --check .`、`git diff --check` |
| 依赖 | `pip check` 通过 | requirements.lock 未变，无新下载 |
| 数据库最终只读核对 | 0007、三张新表、ready=OK；临时迁移 schema 残留 0 | 仅 `127.0.0.1:55434/aftercare_test` |
| 受保护路径 | Agent/API/Worker、旧 0001—0006 迁移及锁文件均未改 | 可信确认和原回执路径仍由完整回归验证 |

4 个跳过项依旧因为独立 tau2 依赖未安装；1 个权重推理测试排除。新增 B1 测试没有跳过。没有新增真实模型、模型用户或裁判请求，没有运行独立 holdout/Test。

最终完整命令（同一测试库不得并发执行其他测试/Worker）：

```bash
AFTERCARE_TEST_DATABASE_URL=postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55434/aftercare_test ALLOW_REAL_MODEL_CALLS=false PYTHONPATH=src:. .venv/bin/python -m pytest -m 'not live_model and not model_weights' --junitxml=runtime/backend-B1/full-first.xml
```

首次 `*-first.*` 证据已冻结，新的复现使用新的输出名。全部结果是本机离线工程证据，不是生产 SLA、吞吐或真实模型效果。

## 已实现的业务链

```mermaid
flowchart LR
  A[可信确认后提交申请 v1] --> B[客服批准 v2]
  B --> C[客户填写寄回信息 v3]
  C --> D[仓库收货 v4]
  D --> E[仓库请求出库 v5]
  E --> F[受控模拟适配器确认出库 v6]
  F --> G[客户确认收到 v7]
```

客服可以拒绝尚未审核的申请；客户可以在寄回前撤回。拒绝/撤回释放目标库存预留和原订单数量占用。收货不自动把旧货增加为可售库存；出库确认消费一次预留，客户确认完成不再扣库存。

创建申请继续使用原 `preview → trusted confirm → commit`，没有增加绕过批准的入口。新聊天修改意图不会直接使已经提交的申请失效；对已提交申请的撤回必须调用正式领域命令。

## 关键代码与数据

| 入口 | 责任 |
|---|---|
| `src/aftercare/domain/fulfillment.py` | Actor、严格命令输入、命令参数、角色和合法状态转移 |
| `src/aftercare/services/fulfillment.py` | 服务层权限、客户归属、版本、幂等、事务、库存和审计 |
| `src/aftercare/services/simulated_warehouse.py` | 稳定 effect_key 对应确定性 SIM 单号，无 HTTP 或外部系统 |
| `src/aftercare/services/business.py` | 原提交事务内创建 native 履约记录与版本 1 审计，原回执保持不变 |
| `src/aftercare/services/demo.py` | 兼容旧演示终结函数，补版本及明确 demo_helper 旁路审计 |
| `migrations/versions/0007_exchange_fulfillment.py` | 固定 DDL 增量迁移与 legacy 回填，不修改旧迁移、不清库 |

数据库新增 `Application.version` 与合法状态/版本约束，以及 `exchange_fulfillments`、`fulfillment_commands`、`fulfillment_audits` 三表。数据结构与模型使用同一约束语义。

### 一次命令如何提交

1. 服务验证可信 Actor、命令角色/来源与参数；客户只能访问自己订单的申请。
2. 按 `order → application → line → fulfillment → reservation → inventory` 顺序取锁，并刷新已读 ORM 对象。
3. 在当前主体作用域查幂等键。原键同摘要返回原回执；异摘要冲突。权限检查先于重放，合法重放先于当前版本判断。
4. 对新命令检查 `expected_version` 和状态转移，再复核预留数量、SKU、状态、库存与订单数量聚合。
5. 同事务写入状态、版本、库存、命令回执及审计。中途失败全部回滚，不能只保存“成功”告知。

命令唯一作用域为 `(application_id, actor_id, actor_role, idempotency_key)`；摘要绑定申请、命令、原 expected_version 与规范化参数。跨申请复用外部出库单号受唯一约束保护，冲突映射为 `SHIPMENT_CONFLICT`，不能部分消费库存。

原 `Operation.receipt` 始终表示“申请已提交”。当前履约状态及命令回执单独保存，B2 必须分别展示，不能回写旧回执伪装成物流已完成。

### 库存与占用不变量

- `inventory.reserved` 等于同 SKU 所有 `RESERVED` 记录数量之和。
- `order_line.occupied` 等于该行除 `REJECTED/WITHDRAWN` 外所有申请数量之和；完成后的数量仍保持占用。
- 申请目标 SKU/数量必须与其唯一预留一致；早期申请对应 RESERVED，已出库/完成对应 CONSUMED，拒绝/撤回对应 RELEASED。
- available/reserved 非负，occupied 位于 0 与购买数量之间。服务复核与数据库约束共同保护，不靠 Agent 文本判断。

`FulfillmentService.get` 使用独立只读 REPEATABLE READ 事务读取申请、履约与审计，不取得行写锁。本批没有修改原 Agent snapshot 或 Worker run_guard。

## 历史数据与兼容

0007 给原申请添加版本 1；为旧 exchange 创建 `origin=legacy` 的履约记录和 `legacy_import` 审计。迁移时间表示导入时间，保留旧状态和所有原字段，不补造物流信息。历史 COMPLETED 的外部单号/出库结果仍为空，不被当作新的仓储成功证据。

原操作、可信批准、幂等绑定、模型预算及未知用量记录不重写。迁移测试在真实 PostgreSQL 的私有 schema 中复建冻结旧结构，逐值比较迁移前后记录；最终整体回滚，未对 public 执行降级。无法识别的旧 exchange 状态会阻止迁移，禁止用改数据或清库绕过。

旧 `settle_application` 继续作为专用 demo/test 辅助函数：对 exchange 递增版本并追加 `demo_settle`，标记 `demo_only/bypasses_fulfillment`，不伪造外部单号。它不能直接终结已经进入正式中间态的申请；B2 不得暴露该函数。

## 身份、模拟与范围限制

Actor 是可信应用调用契约。`test_fixture`、`server_session`、`trusted_adapter` 字符串本身不是凭证，也不是已实现的员工认证。当前权限结果只证明直接调用领域服务时仍受角色/归属规则约束。B2 必须由服务端有效 session 与员工角色记录构造 Actor。

普通仓库角色只能请求出库；只有内部 trusted_adapter 可以确认出库。B1 的 SimulatedWarehouse 是确定性进程内替身，不连接真实物流，没有独立 provider 状态，不证明跨进程重试、UNKNOWN 查单、Outbox/Inbox 或 Worker 接管。上述能力进入 C。

寄回后的撤回、验收不通过和外部永久失败尚无正式异常处理入口；当前拒绝不合法转移并保留状态/库存，不自动退款、销毁、补偿或释放未知结果的预留。后续异常流程需按 B2/C 的具体契约补齐。

仅专用 `127.0.0.1:55434/aftercare_test` 用于本批迁移与测试；旧 demo/预算库和容器未升级。本批没有构建新镜像或发布新运行服务，不能将旧容器视为已具备 B1。

## B2 接入要求

1. 复用已冻结的命令服务、版本、幂等、错误与库存语义；不在路由复制业务规则。
2. 完成员工会话/角色认证及 HTTP→Actor 适配，拒绝请求自声明角色或客户 ID 覆盖身份。
3. HTTP `Idempotency-Key` 映射到领域键，重试保留原版本和参数；接口不能接收测试 fault。
4. 最小界面显示当前履约和审计事实，权限仍由服务端执行；仓库按钮不能直接自报出库成功。
5. 先完成 B2 的真实 HTTP/session、CSRF、身份撤销及三角色接线验收，再进入 C。
