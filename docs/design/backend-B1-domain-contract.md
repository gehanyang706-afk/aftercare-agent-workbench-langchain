# B1 领域契约：供 B2 身份、API 与界面接入

日期：2026-10-27。本文件只冻结当前领域服务的调用与数据语义，不表示 HTTP 认证、员工会话、前端或 C 的可靠外部投递已经具备。阶段验收结果和完成状态另见交接记录。

## 1. 服务入口与职责

实现入口：

- [`domain/fulfillment.py`](../../src/aftercare/domain/fulfillment.py)：Actor、命令输入、参数模型、角色与状态迁移。
- [`services/fulfillment.py`](../../src/aftercare/services/fulfillment.py)：权限、幂等、版本、事务和库存检查。
- [`services/simulated_warehouse.py`](../../src/aftercare/services/simulated_warehouse.py)：确定性的进程内模拟出库确认。
- [`repositories/models.py`](../../src/aftercare/repositories/models.py) 与 [`0007_exchange_fulfillment.py`](../../migrations/versions/0007_exchange_fulfillment.py)：持久约束与增量迁移。

调用签名：

```python
Actor(actor_id: str, role: str, source: str)
FulfillmentCommandInput(command, expected_version, idempotency_key, data={})

FulfillmentService(sessions, clock=utc_now)
FulfillmentService.get(actor, application_id)
FulfillmentService.execute(actor, application_id, command: FulfillmentCommandInput, *, fault=None)

SimulatedWarehouse.confirm(service, application_id, *, expected_version, idempotency_key)
```

`sessions` 是现有 SQLAlchemy session factory；领域服务自行界定事务。服务不接收 HTTP Request、Cookie 或浏览器角色，也不在事务中调用 HTTP、模型或其他外部系统。`fault` 仅供专用测试库的提交前回滚探针，不得作为 B2 产品参数暴露。

领域服务只支持 `action=exchange` 的申请。创建申请继续走原 `BusinessService` 的预览 → 可信确认 → 提交，不新增绕过批准的申请创建接口。

## 2. Actor 与可信调用来源

`Actor` 是不可变 dataclass，来源由可信应用代码负责。领域校验要求其为实际 Actor 对象、主体 ID 为非空且去首尾空白后不超过 100 字符的字符串、角色与来源均处于允许集合。校验不会把主体 ID 自动规范化；B2 应使用服务端记录中的稳定 ID。

角色：`customer`、`support`、`warehouse`。来源：`test_fixture`、`server_session`、`trusted_adapter`。

- `test_fixture`：领域测试注入，不能作为 HTTP 登录能力的证据。
- `server_session`：保留给 B2 的服务端会话适配层；当前字符串校验本身不查询员工、验证密码或证明存在 session。
- `trusted_adapter`：只允许执行 `confirm_shipment`；其他写命令即使角色匹配也拒绝该来源。
- `confirm_shipment` 同时要求 `role=warehouse` 和 `source=trusted_adapter`。普通仓库员工不能通过提交外部单号直接把申请改为已出库。

Actor 对象与 `source` 字符串都不是凭证。B2 必须先校验服务端 session、有效账号和角色，再构造 Actor；禁止从请求体/请求头的自声明 `actor`、`role`、`customer_id`、客户端存储或模型文本构造授权身份。真实适配器身份必须来自内部调用边界，不能由员工请求切换来源。

读取时三个角色均可调用 `get`：客户限本人订单的申请，客服与仓库在当前单商户范围可读全部换货申请。写入另按下表校验角色；客户归属不符与资源不存在统一返回 `NOT_FOUND`，不泄露他人申请。

## 3. 命令输入、角色与迁移

顶层命令模型为 strict 模式，拒绝额外字段。`expected_version` 必须为整数且至少为 1；`idempotency_key` 长度为 1—100 且至少包含一个非空白字符；当前不会自动裁剪幂等键。`data` 按命令对应的 strict 模型校验，拒绝额外字段，字符串参数裁剪首尾空白。

| 命令 | 角色/来源 | `data` | 允许原状态 | 新状态 |
|---|---|---|---|---|
| `approve` | support；test_fixture/server_session | `note` 可省略，默认空串，最多 500 字符 | EXCHANGE_REQUESTED | AWAITING_RETURN |
| `reject` | support；test_fixture/server_session | 同上 | EXCHANGE_REQUESTED | REJECTED |
| `withdraw` | 所属 customer；test_fixture/server_session | 空对象 | EXCHANGE_REQUESTED、AWAITING_RETURN | WITHDRAWN |
| `submit_return` | 所属 customer；test_fixture/server_session | `carrier`、`tracking_number`，各 1—100 字符 | AWAITING_RETURN | RETURN_IN_TRANSIT |
| `receive` | warehouse；test_fixture/server_session | `note` 可省略，默认空串，最多 500 字符 | RETURN_IN_TRANSIT | RETURN_RECEIVED |
| `request_shipment` | warehouse；test_fixture/server_session | 空对象 | RETURN_RECEIVED | REPLACEMENT_PENDING |
| `confirm_shipment` | warehouse；只限 trusted_adapter | `external_shipment_id`，1—100 字符 | REPLACEMENT_PENDING | REPLACEMENT_SHIPPED |
| `complete` | 所属 customer；test_fixture/server_session | 空对象 | REPLACEMENT_SHIPPED | COMPLETED |

每个成功的新命令使申请 `version` 加 1，并生成一份命令回执和一条对应版本审计。新幂等键对同一动作的重复提交不是自动成功：旧版本会冲突，当前版本但状态不允许则拒绝迁移。精确重放原请求才返回原回执。

`receive` 保存收货说明和服务端时间，不自动增加旧货的可售库存。`request_shipment` 设置稳定 `effect_key="exchange:" + application_id` 及 `shipment_status=PENDING`。`confirm_shipment` 要求该键已存在、出库结果为 PENDING 或 UNKNOWN，才保存外部单号并置 SUCCEEDED；外部单号有数据库唯一约束。

REJECTED、WITHDRAWN、COMPLETED 没有新的合法业务转移。B1 没有未知结果自动查单、重试、超时释放或人工恢复任务；UNKNOWN 等结果字段为后续可靠异步边界预留，不能据其存在声称 C 已实现。

## 4. 鉴权、幂等与版本顺序

写命令按如下顺序执行：

1. 检查 Actor、命令对象类型、命令角色/来源权限和参数；测试故障开关也在此受限。
2. 在事务中找到申请及订单归属，按约定取锁，确认是换货申请。
3. 查找当前主体作用域内的历史命令：同键同摘要返回原回执，同键异摘要报 `IDEMPOTENCY_CONFLICT`。
4. 对新的命令检查申请版本，再检查允许原状态。
5. 取得其余资源锁，校验库存/占用不变量，完成状态、资源、回执和审计的同事务变更。

幂等唯一作用域是 `(application_id, actor_id, actor_role, idempotency_key)`，不含 `actor_source`。原 Actor 来源会存入历史命令与审计；B2 改为 server_session 后仍必须使用稳定主体 ID/角色，不能为重试更换身份。来源权限仍在重放查询前检查，不能利用旧回执绕过权限。

请求摘要为排序、紧凑 JSON 的 SHA256，包含 `application_id`、`command`、**原 expected_version** 和规范化后的 `data`。同一请求重试须保留原键、原版本和原参数；把 expected_version 更新为当前版本会成为同键异参冲突。

这个顺序保证先鉴权再读取旧回执，且合法旧请求可在后续状态/版本已变化时重放。不得在 B2 路由先用当前版本拒绝请求，再调用领域服务，从而破坏已提交请求的恢复。

## 5. 事务、锁顺序与库存不变量

正式命令的锁顺序为：

```text
order → application → order_line → exchange_fulfillment → reservation → inventory
```

初始只读定位使用命令不能修改的外键关系；加锁查询用 `populate_existing` 读取锁后当前值。命令重放在 order/application/line 锁定后即可返回，不再修改履约或库存。B2 不应包裹另一套先锁库存的事务，也不应直接修改 ORM 行来替代服务命令。

新命令在修改前和 flush 后都检查：

- 申请、预留与目标 SKU/数量相符，履约记录和库存记录存在。
- `available >= 0`、`reserved >= 0`、`0 <= line.occupied <= line.quantity`。
- 当前 SKU 的 `inventory.reserved` 等于该 SKU **全部 RESERVED 预留**的 quantity 之和。
- 当前订单行的 `occupied` 等于该行**所有非 REJECTED/WITHDRAWN 申请**的 quantity 之和，包含已有退货申请，不能只累加换货。
- 当前申请在 REJECTED/WITHDRAWN 时预留为 RELEASED；在 REPLACEMENT_SHIPPED/COMPLETED 时为 CONSUMED；其余履约状态为 RESERVED。

资源变化：

| 动作 | available | reserved | line.occupied | 预留 |
|---|---:|---:|---:|---|
| 原可信提交创建换货申请 | −quantity | +quantity | +quantity | RESERVED |
| reject / withdraw | +quantity | −quantity | −quantity | RELEASED |
| confirm_shipment | 不变 | −quantity | 不变 | CONSUMED |
| approve / submit_return / receive / request_shipment / complete | 不变 | 不变 | 不变 | 按当前阶段保持 |

reject、withdraw、confirm_shipment 还递增目标库存和原订单的版本。申请每次新命令均递增自身版本；完成后原订单数量占用保持已使用，避免同一数量重复申请。

申请状态/版本、履约字段、库存与数量占用、FulfillmentCommand 回执、FulfillmentAudit 在一个事务提交；任何中间异常必须全部回滚。数据库另保证版本下限、合法换货状态、非负库存/占用边界、出库键/外部单号唯一、命令作用域唯一、每申请每版本审计唯一，以及每条命令至多一条审计。

## 6. 返回结果与原操作回执

`execute` 返回：

```json
{"command_id":"cmd_...","application_id":"app_...","command":"approve","status":"AWAITING_RETURN","version":2}
```

该回执保存在 FulfillmentCommand 中，重放直接返回原值。它不包含全部履约详情；展示运单、收货时间和审计应读取 `get`。

原 `Operation.receipt` 始终表示原申请提交事实，例如 `result=EXCHANGE_REQUESTED`。后续即使履约完成，也不将旧回执改成已发货/完成。B2 应分别展示“申请提交回执”和“当前履约状态”，不能将 Agent 图结束、会话 COMPLETED 或旧回执解释为客户已收到换货。

`get` 返回 `application_id`、当前 `status`、`version`、`fulfillment` 字典和按 `to_version` 升序排列的 `history`。查询在独立的 **REPEATABLE READ、READ ONLY** 事务中读取，不取得行写锁；只保证这一响应中这些数据库读取的一致性，不保证返回后不会发生新命令。

字典序列化沿用 `columns`：时间转换为 ISO 字符串，NULL 字段省略。B2 应把可空履约字段的缺失视为暂无证据。当前 `get` 的审计历史是全量；分页与其他读取优化须按后续方案实施，不能称已完成。这里没有改变原 `BusinessService.snapshot` 的内部锁或 Worker run_guard。

## 7. 创建、历史迁移与演示旁路

### 原可信创建

`BusinessService.commit` 创建新换货申请时，额外在原事务中创建 `origin=native` 的履约记录和版本 1 审计：`command=application_created`、`actor_role=customer`、`actor_source=trusted_confirmation`，关联原 operation/preview/hash。该记录不创建 FulfillmentCommand，不改变原九工具或确认链路。

### 0007 历史导入

所有旧申请增加 `version=1`，旧字段、Operation 回执、预算与未知用量账本保留。旧 exchange 申请额外获得 `origin=legacy` 的履约记录，物流/收货/外部单号/出库结果均为空；历史 COMPLETED 不等于获得了新的物流成功证据。

导入审计为 `legacy_import`，主体 `migration:0007`，角色 system，来源 migration；原状态/版本为空，目标状态沿用旧申请，目标版本为 1，`details={"legacy_import": true}`。时间表示导入时间，不伪造历史业务发生时间。非 exchange 不生成履约记录；未知的历史 exchange 状态安全阻止迁移，不重写状态迁就约束。迁移禁止降级。

`legacy` 是事实来源标记，不自动提升为 native。查询与新合法命令仍以当前申请状态和资源一致性为准；不能依据 origin 或历史终态补造运单。

### demo helper 的明确旁路

`services/demo.py::settle_application` 保留专用 demo/test 兼容能力，可直接将原申请态改为 REJECTED/WITHDRAWN/COMPLETED。对 exchange 会递增版本并写 `demo_settle` 审计：主体 demo-helper、角色 system、来源 demo_helper、`details={"demo_only": true,"bypasses_fulfillment": true}`，无正式 command_id，不虚构仓储出库证据。

此 helper 不属于正式状态机、正式员工授权或 Agent 工具；同终态重复调用仍幂等。B2 禁止从路由/界面调用或暴露它，也不能把 demo_helper、migration、trusted_confirmation 等审计来源当成可登录 Actor 来源。

## 8. 错误与接入处理

| 错误 | 领域含义 |
|---|---|
| ACTOR_REQUIRED / ACTOR_INVALID | 缺失 Actor 或对象/字段/角色/来源不合法 |
| FORBIDDEN | 角色或来源无权执行该命令 |
| NOT_FOUND | 申请/订单不存在，或客户不拥有该资源 |
| UNSUPPORTED_APPLICATION | 申请不是 exchange |
| INVALID_COMMAND_DATA | 传入非命令对象或命令 data 校验失败 |
| IDEMPOTENCY_CONFLICT | 当前主体作用域的原键对应不同请求摘要 |
| VERSION_CONFLICT | 新命令的 expected_version 与当前版本不符 |
| ILLEGAL_TRANSITION | 当前状态不允许执行该新命令 |
| INVENTORY_INVARIANT | 关联记录、预留/库存/数量占用或履约前提不一致 |
| SHIPMENT_CONFLICT | 外部出库单号已属于另一申请，事务已回滚 |
| SHIPMENT_NOT_REQUESTED | 进程内模拟适配器找不到出库业务键 |
| TEST_FAULT_DENIED | 非允许测试环境或故障种类 |

顶层命令模型构造时的字段错误由 Pydantic `ValidationError` 报告；测试提交前探针抛 `INJECTED_FULFILLMENT_FAILURE` RuntimeError。其他未映射的数据库错误保持原异常，不伪装成业务成功。

以上是领域错误码，不冻结 B2 的 HTTP 状态映射。B2 应统一映射权限、输入、版本/幂等冲突与内部不变量故障，并保留关联 ID；不能吞掉异常后返回成功，也不能无条件重试业务写入。`fault` 和测试异常不得进入公开请求模型。

## 9. B2 必须复用的边界

1. session → 服务端身份/角色 → Actor；认证发生在接入层，权限与客户归属仍在领域服务复核。
2. HTTP 参数映射到 FulfillmentCommandInput，复用 `execute`；不得在路由再写状态迁移、库存变化或幂等逻辑。
3. 客户申请创建沿用原可信确认；客服/仓库操作调用正式命令，员工命令不加入 Agent 工具。
4. 仓库员工只能请求出库；B1 演示通过 SimulatedWarehouse 提供适配器确认。其固定主体为 warehouse-simulator，外部单号为 `SIM-` 加稳定 effect_key 的 SHA256 前 24 位；重复输入不生成新单号。它没有 HTTP、独立 provider 存储或故障恢复能力。
5. 界面分别显示申请回执、当前履约、审计来源与真实可用证据；隐藏按钮只改善体验，服务端拒绝才构成权限控制。
6. B2 真实 HTTP/session 测试须覆盖身份篡改、跨客户、失效会话、CSRF、并发版本、精确幂等重放和角色越权。领域夹具 Actor 测试不代替这些认证验证。
7. C 在该契约上添加独立模拟仓储 HTTP、Outbox/Inbox、provider 幂等、UNKNOWN 查单与接管保护；B2 不提前声称具备可靠外部履约。
