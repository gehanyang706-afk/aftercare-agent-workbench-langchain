# B2 员工身份与履约 API 契约

日期：2026-10-27。本文冻结当前员工建号、会话、HTTP 接入与 B1 调用边界；验收数量、首次失败和界面检查另见交接。领域状态、库存和命令语义沿用 [B1 领域契约](backend-B1-domain-contract.md)。客户仍是显式开启的本地演示身份，不是生产客户认证。

主要实现：[`staff_auth.py`](../../src/aftercare/services/staff_auth.py)、[`api/fulfillment.py`](../../src/aftercare/api/fulfillment.py)、[`api/routes.py`](../../src/aftercare/api/routes.py)、[`api/app.py`](../../src/aftercare/api/app.py)、[`cli.py`](../../src/aftercare/cli.py)。

## 1. 员工建号与本地管理

员工账户只能通过可信本地调用创建，没有 HTTP 自注册或修改角色接口。CLI 使用当前数据库配置，先校验专用数据库/角色/项目 schema；它不自动清库或迁移。执行前应核对配置和已完成的增量迁移，不把历史数据库端口当作本轮测试库。

```bash
.venv/bin/python -m aftercare staff create --username support-1 --role support
.venv/bin/python -m aftercare staff create --username warehouse-1 --role warehouse
```

默认使用隐藏密码输入；`--password-stdin` 供可信自动化从标准输入读取一行，密码不作为命令行参数。不要把明文密码写进 shell 命令、仓库或日志。建号结果只含 `id`、`username`、`role`、`active`、`auth_version`。

| CLI | 语义 |
|---|---|
| `staff create --username ... --role support/warehouse` | 创建激活账户，初始 auth_version=1 |
| `staff revoke --staff-id ...` | 账户 auth_version 加 1，撤销其既有全部 session |
| `staff disable --staff-id ...` | 停用账户并递增 auth_version |

可信本地管理还可调用 `StaffAuthService.update_account(staff_id, *, role=None, active=None, password=None)`；至少提供一项，任何有效更新都递增 auth_version。它不是给浏览器调用的管理 API，也不新增管理员登录角色。

用户名经 strip/lower 后须匹配 `[a-z0-9][a-z0-9_.-]{0,99}`，唯一性在数据库约束中保证。新密码为 12—128 字符、不可全空白，不施加额外字符组合规则；登录输入允许有界的 1—128 字符，错误凭据统一处理。

## 2. 密码与员工 session

- 密码使用 stdlib scrypt：固定 N=16384、r=8、p=5，随机 16 字节 salt、32 字节派生值，maxmem=64 MiB。每进程最多同时执行两个派生操作，这是资源并发上限，不是登录限流。
- 存储格式包含算法、固定参数、salt 与派生值；验证拒绝不支持/越界的参数，不按数据库中的任意成本值分配资源。比较使用 `compare_digest`。
- 未知用户名执行 dummy 派生；未知用户与密码错误都返回 AUTH_FAILED。此机制不构成已验证的严格等时响应，也不代替专用登录速率限制。
- `StaffAccount` 保存当前 role、active、auth_version 与密码哈希。`StaffSession` 保存 token 哈希、staff_id、CSRF、登录时 auth_version、创建/过期/撤销时间；不保存角色权限快照。
- 员工 token 与 CSRF 分别由 `token_urlsafe(32)` 生成。数据库只保存 `SHA256(raw_token)`；原 token 只交给 HTTP Cookie，不能出现在 JSON 或日志中。
- 员工 session 固定 8 小时有效，当前没有滑动续期。认证时重读账户的当前角色、active、auth_version，以及 session 的 revoked_at/期限；不信任登录响应或浏览器缓存的角色。
- Auth 服务默认使用独立 `utc_now`，不沿用业务模拟或测试的历史时钟。

员工 Cookie 名为 `aftercare_staff_session`，设置 HttpOnly、SameSite=Strict，HTTPS 请求时加 Secure；Max-Age 根据服务端 expires_at 计算。本地 HTTP 演示不因此获得 HTTPS 传输保证。

0008 仅新增 `staff_accounts`、`staff_sessions` 及其约束/索引，不重写旧申请、履约、回执或预算账本，且禁止降级。员工角色只允许 support/warehouse，账户 auth_version 至少为 1。

## 3. 登录、退出与身份切换

| 接口 | 输入/要求 | 结果 |
|---|---|---|
| `POST /api/staff/sessions` | JSON：username、password；同源检查 | 设置员工 Cookie；JSON 返回 actor_id、role、username、csrf_token、expires_at |
| `GET /api/staff/session` | 有效员工 Cookie | actor_id、role、display_name、csrf_token |
| `DELETE /api/staff/session` | 有效员工 session、CSRF、同源检查 | 设置 revoked_at 并清除 Cookie，返回 `{"ok":true}` |
| `GET /api/fulfillment/session` | 有效员工或演示客户身份 | 当前 actor_id、role、display_name、csrf_token |
| `POST /api/demo-sessions` | 演示开关开启；JSON customer_id，仅 alice/bob/carol | 本地客户 Cookie 与 CSRF；不校验客户密码 |
| `GET /api/session` | 有效演示客户 Cookie且演示开关开启 | 原客户 principal 与 CSRF |
| `DELETE /api/session` | 有效演示客户 session、CSRF、同源检查 | 删除服务端 DemoSession 并清除客户 Cookie |

登录不返回密码、密码哈希、原 token 或数据库 session_hash。登录输入采用 SecretStr；请求校验错误只返回字段位置、类型与说明，剔除原输入和上下文，避免默认校验错误回显密码。

员工退出在服务端撤销该 session，复制的旧 Cookie 不能继续使用。停用、改角色、改密码或 revoke_all 通过账户版本使旧 session 失效；重新启用不会复活旧版本 session，必须重新登录。

员工登录会清除当前响应中的客户 Cookie；演示客户登录会清除员工 Cookie。新履约接口收到两种 Cookie 时直接返回 AMBIGUOUS_IDENTITY，不自行选择更高权限身份。切换身份清除 Cookie 不等于撤销其他已签发 session；需要撤销时使用明确的退出/revoke 流程。

## 4. Actor 与来源边界

员工 Actor 从当前服务端账户构造：`Actor(account.id, account.role, "server_session")`。客户 Actor 从有效 DemoSession 对应客户构造：`Actor(customer_id, "customer", "server_session")`。客户 source 取同一个名称只表示服务端完成了演示 session 解析，不把本地客户身份升级为生产认证。

HTTP 写入模型为 strict、extra=forbid，不接受 `actor`、`role`、`customer_id`、`source` 或测试 `fault`。自填 X-Role、X-Actor-Id、X-Actor-Source 等头不会参与身份构造。请求只能选择业务参数，不能选择谁在执行或获得 trusted_adapter 来源。

所有非 GET/HEAD 的已登录操作要求 `X-CSRF-Token` 与当前 session 匹配；存在 Origin 时必须与请求 scheme/netloc 一致。匿名登录/演示建 session 入口独立检查同源与 JSON Content-Type。当前没有跨站登录协议或 CORS 授权方案。

## 5. 写入期间的认证锁与 SAVEPOINT

履约写入不仅依赖请求开始时的一次身份检查。执行前再次建立认证事务：

```text
HTTP 身份/CSRF检查
→ 外层 auth_session.begin
→ 员工 account FOR SHARE → staff_session FOR SHARE，并重查身份/版本/期限
  或客户 DemoSession FOR SHARE，重查归属/期限/演示开关
→ 同一 Connection 创建 join_transaction_mode="create_savepoint" 的领域 session factory
→ B1 FulfillmentService.execute：鉴权→幂等→版本→状态→库存/回执/审计
→ 释放领域 SAVEPOINT
→ 外层必要审计更新
→ 外层 COMMIT 后才返回成功
```

员工 guard 使用 `populate_existing` 读取锁后数据；若这次解析的 Actor 与请求最初解析结果不同，拒绝执行。这样角色在两次检查之间变化时，不会继续使用旧 Actor 写入。

内层 B1 的事务提交只释放 SAVEPOINT，不提交外层物理事务。认证共享锁与领域写入保持到外层提交；撤销账户版本、停用/改角色/改密码需要账户写锁，员工退出需要 session 写锁，因而与已持有 guard 的业务写入串行化。

若撤销先提交，随后 guard 校验失败；若已授权写先取得共享锁，它可以先提交，撤销随后生效。该语义不是“撤销会取消已经开始的业务”，也不回滚已提交申请。外层提交失败不能发送成功回执。

退出接口先完成短认证事务，再调用自行开启事务的 logout；不能在仍持有该 token 共享锁时另开连接等待同一 session 的写锁。领域 session 不在锁内进行外部 I/O，C 也必须保留这一边界。

## 6. 履约 API、参数与返回

所有下表 POST 都要求 CSRF、同源检查、`Idempotency-Key` 头及整数 `expected_version>=1`。幂等键不可空/全空白、最长 100 字符。精确重放保留原键、原版本和原参数，路由不在 B1 之前另做当前版本拒绝。

| 路径 `/api/applications/{id}/...` | 角色 | JSON 中除 expected_version 外的字段 | B1 命令 |
|---|---|---|---|
| `review` | support | decision=approve/reject；note 可选，最多 500 字符 | approve / reject |
| `withdraw` | 所属 customer | 无 | withdraw |
| `return-shipment` | 所属 customer | carrier、tracking_number，各 1—100 字符 | submit_return |
| `receive` | warehouse | note 可选，最多 500 字符 | receive |
| `replacement-shipments` | warehouse | 无 | request_shipment |
| `complete` | 所属 customer | 无 | complete |
| `simulate-shipment` | warehouse，且模拟开关开启 | 无；不接收外部单号 | 内部适配为 confirm_shipment |

`POST /api/applications/{id}/confirm-shipment` 未对 HTTP 暴露。仓库员工只能请求正常出库，或在明确模拟模式下触发内部模拟确认，不能用自填 source/外部单号直接确认真实出库。

成功写入返回原 B1 命令回执：command_id、application_id、command、status、version。原 Operation.receipt 始终保留“申请已提交”的历史事实，不改写成履约完成。

读取接口：

| 接口 | 返回与边界 |
|---|---|
| `GET /api/applications?after=...&limit=20` | 只列换货申请；limit 为 1—100，after 最长 100；按 application_id 字典序升序，返回 items/next_cursor |
| `GET /api/applications/{id}` | B1 当前状态、版本、fulfillment、history，加不可变订单/申请元数据、原 receipt 与 allowed_actions |
| `GET /api/applications/{id}/events` | `{"items":[...]}`，B1 版本审计历史；当前是完整列表，不是 SSE |

客户读取限制为本人订单；客服、仓库在首轮单商户范围可读全部换货申请。列表游标不是时间顺序或跨请求一致性快照，不承诺并发插入时的一次性全量视图。

## 7. 三角色 allowed_actions 与模拟审计

allowed_actions 由服务端根据 B1 TRANSITIONS、当前状态和当前 Actor 角色生成：

- support：初始申请显示 approve/reject。
- customer：本人申请可在申请/待寄回态显示 withdraw，待寄回显示 submit_return，已发换货显示 complete。
- warehouse：寄回途中显示 receive，已收货显示 request_shipment；待出库且模拟开关开启时显示 simulate_shipment。

列表不包含 confirm_shipment。返回按钮集只是当前读取时的提示；并发状态可能在点击前变化，最终权限、版本、状态和库存仍由 B1 校验。隐藏按钮不构成授权。

模拟确认使用固定内部 Actor `warehouse-simulator / warehouse / trusted_adapter`，由稳定 effect_key 计算 `SIM-` 加 SHA256 前 24 位作为单号。HTTP 只能触发这个受限模拟适配，不能提交自选单号。

模拟业务命令审计仍记录 trusted_adapter；外层事务在同一条审计的 details 中增加首次触发者：

```json
{"triggered_by":{"actor_id":"staff_...","role":"warehouse","source":"server_session"}}
```

该字段与业务写入共同提交，重放不会覆盖首次触发人。模拟命令沿用适配器 Actor 的幂等作用域，因此不同有效仓库员工对同一申请、同键同参数可以得到同一原回执；这不改变原触发人，也不产生第二次库存消费。

这是确定性的进程内模拟，没有独立 provider 状态、真实物流、网络超时或可靠投递保证；不得将这条按钮演示称为 C 的异步履约。

## 8. 错误映射与客户端处理

HTTP 业务错误采用 `ok=false`、error_code、user_message、retryable；HTTPException 还保留 detail，请求模型错误的 detail 为经过脱敏的字段列表。未映射异常不能被客户端当成成功。

| HTTP | 主要错误 |
|---|---|
| 401 | AUTH_FAILED；SESSION_REQUIRED/SESSION_EXPIRED；ACCOUNT_DISABLED；STAFF_SESSION_REQUIRED；DEMO_SESSION_REQUIRED/DEMO_SESSION_EXPIRED/DEMO_LOGIN_DISABLED；AMBIGUOUS_IDENTITY |
| 403 | FORBIDDEN、ACTOR_REQUIRED/ACTOR_INVALID、CSRF_REQUIRED、ORIGIN_DENIED |
| 404 | NOT_FOUND；关闭的演示登录/模拟接口；未开放路径 |
| 409 | VERSION_CONFLICT、IDEMPOTENCY_CONFLICT、ILLEGAL_TRANSITION、SHIPMENT_CONFLICT、SHIPMENT_NOT_REQUESTED 等未另映射的业务冲突 |
| 415 | JSON_REQUIRED |
| 422 | INVALID_INPUT、INVALID_COMMAND_DATA、STAFF_INPUT_INVALID、IDEMPOTENCY_KEY_REQUIRED |
| 503 | INVENTORY_INVARIANT；演示数据缺失等明确未就绪条件 |

密码错误不回显密码；凭据未知与密码不符使用相同 AUTH_FAILED。正确密码但账户已停用返回 ACCOUNT_DISABLED。STAFF_EXISTS 用于可信本地建号错误，不存在 HTTP 自注册入口。

版本冲突先刷新详情并由用户决定下一动作，不替换旧请求参数后沿用原幂等键；不确定旧写结果时保留原请求重放。对库存不变量或未知服务器错误停止自动业务重试并检查实际状态，不能显示“已发货”。

## 9. 开关、只读边界与 C 接入

`demo_login_enabled` 与 `warehouse_simulation_enabled` 默认均为 false。源码服务可显式使用 `serve --demo-login --simulate-warehouse`；Compose 使用 `DEMO_LOGIN_ENABLED`、`WAREHOUSE_SIMULATION_ENABLED`，默认也关闭。`GET /api/auth/options` 公开返回这两个演示能力开关供界面选择入口。

关闭客户演示入口后，已签发的客户演示 Cookie 也不能继续访问受其身份保护的接口。关闭模拟开关后，不返回模拟动作且模拟请求拒绝。员工登录不依赖这两个演示开关。

只读请求在短认证事务后查询业务数据。B1 `get` 的履约状态/版本/履约字段/审计在单独 REPEATABLE READ、READ ONLY 事务中一致读取；HTTP detail 随后另开事务补不可变申请元数据和原回执。不能把整个 HTTP 响应连同认证、元数据补查统称为一份原子快照，也不保证返回后状态仍未变化。原 Agent/Worker 的 snapshot 锁和 run_guard 没有被此接口替换或移除。

C 应复用现有 session→Actor、受控命令、guard 与外层事务边界；出库请求在业务事务中写唯一业务键及 Outbox，网络调用在事务外执行。未来独立模拟仓储/可信回调不接收人类请求自填的 trusted_adapter 身份，并需补充 provider 幂等、Inbox 去重/乱序、UNKNOWN 查单和接管保护。保留首次员工触发记录与原申请回执，不能以外部响应超时释放库存或宣告成功。

当前只覆盖本机单商户演示的员工密码/session 与三角色业务接入，没有专用登录限流、失败锁定、MFA、SSO、生产客户认证或生产容量结论。后续能力需分别实现与验收，不由 source 字符串、密码哈希参数或本轮界面演示推导。
