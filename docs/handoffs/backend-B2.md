# B2：身份权限、正式 API 与最小履约界面

2026-10-27—28。本批按 A → B1 → B2 → C → D 顺序，接入 B1 已冻结领域服务。员工会话、正式换货接口和三角色页面已实现，工程与真实 HTTP 验证已通过；浏览器三角色链及桌面/窄屏截图也已于 2026-10-28 通过。源码/迁移/首次证据指纹见[验证清单](../benchmarks/reports/backend-B2-validation.json)，身份、接口与事务细节见[冻结契约](../design/backend-B2-auth-api-contract.md)。C 的独立仓储 HTTP、Outbox/Inbox、UNKNOWN 查单与接管尚未实现。

## 已实现

- 员工账号与可撤销服务端会话；随机盐 scrypt 保存口令，数据库只保存随机会话 token 的 SHA256，不将口令或原 token 返回公开 JSON。
- 客服/仓库身份来自有效员工与当前角色记录；客户限本地演示身份且仅能操作本人申请。两个演示开关默认关闭。
- 审核、撤回、填写寄回信息、收货、请求出库、确认收到等正式接口，复用 B1 版本、幂等、库存与审计。
- 单独的本地模拟出库入口，要求仓库身份与显式开关；内部模拟适配器确认，员工不能填写外部单号或声明 trusted_adapter。审计保存首次真实发起员工。
- `/fulfillment` 三角色页面：登录/退出、申请分页、详情、角色动作、原申请回执、当前履约凭据与审计时间线。原工作台增加导航链接。
- `staff create/revoke/disable` 可信本地命令，不开放 HTTP 自注册或角色管理。

B1 的 `domain/fulfillment.py`、状态机、库存服务及 Worker/Agent 工具边界保持复用。`SimulatedWarehouse` 仅提取了确定性单号方法，供事务内的 HTTP 适配使用，不增加外部系统能力。

## 核心接线机制

```mermaid
flowchart LR
  B[浏览器 Cookie + CSRF] --> I[服务端解析有效会话与角色]
  I --> A[构造可信 Actor]
  A --> G[外层认证事务：账号/会话共享行锁]
  G --> S[B1 命令服务：绑定同一连接的 SAVEPOINT]
  S --> T[状态/版本/库存/命令回执/审计]
  T --> C[外层一并提交，释放认证锁]
```

认证依赖先取得身份，业务写入前再 guard。账号停用、角色/密码更新、全会话撤销会更新 auth_version；退出撤销单个 session。与正在提交的命令发生竞争时，撤销等待已获锁命令提交；之后旧凭证失效。若在依赖取得身份后、业务 guard 前发生撤销，请求被拒绝且不产生业务写入。

B1 内层只释放 SAVEPOINT，不能先于认证事务提交；身份保护与业务变化因此在同一事务边界内生效。接口不接收 Actor、角色、客户归属或测试 fault。隐藏按钮不是权限依据。

员工与客户 Cookie 分离，登录切换清理另一类 Cookie；同时提供两类 Cookie 时统一履约接口拒绝歧义。客户关闭演示开关后，旧客户 Cookie 也不能继续访问。Cookie 为 HttpOnly、SameSite=Strict；HTTPS 时使用 Secure。

## 当前验收记录

| 批次 | 结果 | 说明 |
|---|---|---|
| 首轮专项 | 59 passed，28.40 秒 | 初版 B2 API/认证与原工作台，未跳过 |
| 首轮完整 | 386 passed、5 failed、4 skipped、1 deselected，52.27 秒 | 原样保留，5 个旧工作台测试未显式开启新演示开关 |
| 修复后完整 | 391 passed、4 skipped、1 deselected，56.86 秒 | 新增 37 项 HTTP + 22 项认证/迁移测试，全部通过 |
| 实际 TCP HTTP | 已通过：2 次真实 CLI Worker、6 个履约命令及各 1 次精确重放 | 同一申请最终 COMPLETED / version 7，原申请回执未改 |
| 真实浏览器界面 | 已通过：3 个独立身份、6 步页面操作，5.272 秒 | 另一申请最终 COMPLETED / version 7；原回执不变、首次仓库发起人正确、最终动作清空 |
| 桌面与窄屏 | 已通过：1440 与 390 像素宽度 | 无横向溢出；两张完整截图均已视觉检查 |

旧测试修复仅在 Settings 测试准备处增加 `demo_login_enabled=True`，原确认、租约、预算停止、暂停、唯一副作用等业务断言未改变。首轮失败日志未覆盖；没有把失败计作通过。

4 个跳过仍为缺少独立 tau2 依赖，1 个排除仍为权重推理。新增 B2 没有跳过。真实模型请求 0，未运行独立 holdout/Test。

完整回归覆盖原 Agent/HITL、可信确认、预算、Worker 与 SSE；B2 新增检查包括：

- 原 HTTP 消息 → Worker → 明确确认创建申请，再由三个真实会话完成换货。
- 审计中的员工 ID/角色/source 确实来自服务端会话；请求体/请求头不能升级身份。
- 跨客户列表/详情/审计/写入、角色越权、CSRF、Origin、额外身份字段、伪造出库结果均拒绝。
- 退出后旧 Cookie、过期、停用、角色/密码更新及 auth_version 撤销失效。
- 认证共享锁、实际数据库阻塞关系、HTTP 命令与撤销的提交顺序，以及 guard 前撤销负例。
- HTTP 版本/幂等冲突回滚；冲突不额外改变认证记录；相同原请求重放返回同回执。
- 模拟出库首次发起人不能被其他仓库员工的重放覆盖；错密码及无效输入不回显口令。
- 私有 schema 中 0007 → 0008 增量迁移保留旧履约、版本 2 命令/审计与账本，新员工表为空，禁止破坏性降级。

## 运行与复现

当前验收只使用已保留的 `127.0.0.1:55434/aftercare_test`。该库为可被测试清空的专用库，演示时不要并发运行测试或另一个 Worker。旧 55432/55433 服务保持停止，旧 demo/容器没有迁移或重建。

```bash
export DATABASE_URL=postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55434/aftercare_test
export ALLOW_REAL_MODEL_CALLS=false
export PYTHONPATH=src:.
.venv/bin/python -m aftercare db migrate
.venv/bin/python -m aftercare staff create --username support-demo --role support
.venv/bin/python -m aftercare staff create --username warehouse-demo --role warehouse
.venv/bin/python -m aftercare serve --offline --demo-login --simulate-warehouse --port 8132
```

建号时终端隐藏输入密码，至少 12 字符；自动化可使用 `--password-stdin`，不要把真实密码放进命令行参数或提交到仓库。CLI 仅打印不含密码的账号字段。客户申请工作台仍需另一个终端运行原脚本 Worker：

```bash
.venv/bin/python -m aftercare worker --provider scripted
```

打开 `http://127.0.0.1:8132/fulfillment`。客户先到原工作台提交并明确确认换货申请；客服批准，客户填写寄回，仓库收货并请求出库，显式本地模拟出库后由客户确认收到。正式 API 不允许直接新建申请来跳过原确认边界。

复现完整测试时用新的日志名，不覆盖首次记录：

```bash
AFTERCARE_TEST_DATABASE_URL=postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55434/aftercare_test ALLOW_REAL_MODEL_CALLS=false PYTHONPATH=src:. .venv/bin/python -m pytest -m 'not live_model and not model_weights' --junitxml=runtime/backend-B2/reproduction.xml
```

`scripts/backend_b2_http_smoke.py` 是本批真实 TCP/CLI 受控演示脚本，要求显式测试库 URL 与离线标志；临时口令写入权限 0600 的 runtime 文件，服务结束后停用账号。脚本使用固定演示订单及库存，**要求专用库处于未消耗这些订单/目标库存的初始 fixture 状态**；仅换输出目录不会恢复库存，也不能直接在本次已完成换货的库上重放整条申请链。不要为复现重置旧 demo 或真实账本库。

在满足初始 fixture 条件的专用测试库中，可显式使用新目录：

```bash
DATABASE_URL=postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55434/aftercare_test ALLOW_REAL_MODEL_CALLS=false PYTHONPATH=src:. .venv/bin/python scripts/backend_b2_http_smoke.py --output-dir runtime/backend-B2-reproduction
```

首次报告与服务器日志在 `runtime/backend-B2`，脚本在任何数据库/服务操作前拒绝覆盖首次报告或服务器日志。首次运行的脚本源码另存 `runtime/backend-B2/http-script-first.py`；之后只增加输出目录和证据防覆盖检查，没有重跑或覆盖首次业务结果。浏览器检查、最终指纹及清理结果在本批验证清单中另列。

## 浏览器验收与环境收尾

[浏览器记录](../benchmarks/reports/backend-B2-browser-QA.md)保留 CUA 连接失败与后续独立 headless Chrome 验证的实际结果。首轮 CUA 未取得页面，随后用已安装 Chrome 与捆绑 Playwright，在独立临时环境中实际完成界面操作；源码/HTTP 检查未替代界面观察。此次只覆盖一条本地流程与两种视口，未做完整浏览器兼容性或无障碍审计。

[桌面截图](../screenshots/backend-B2-desktop.png) · [窄屏截图](../screenshots/backend-B2-narrow.png)

首轮 TCP/CLI 服务已停止，两个临时员工已停用，auth_version 均为 2；只读核验数据库为 `0008_staff_auth`、健康 ready、迁移临时 schema 为 0。业务、审计和会话记录保留，不称为清空全部测试数据。浏览器补验收复用首轮保留的另一条申请，使用另一个限时服务与新的临时员工；已成功退出服务，三角色均通过页面退出、临时浏览器配置已删除。最终只读复核原 HTTP 两个员工及 UI 各次夹具/中断/恢复共六个临时员工全部停用，两条申请均为 COMPLETED / version 7。未触碰用户浏览器配置，未安装或下载新软件。

## C 的接入边界与限制

客户仍是显式开启的本地演示身份，不是生产客户登录；员工认证只面向当前本机单商户演示，没有 SSO、找回密码、完整账号管理或互联网部署验收。

本地模拟适配器没有独立 provider 状态或网络故障。B2 没有 Outbox/Inbox、跨进程至少一次投递、UNKNOWN 查单、死信或任务接管，不能把本批 HTTP 接线成功解释成外部履约可靠性。

C 应复用正式 `request_shipment`、稳定 effect_key、命令幂等与库存语义，在业务事务中加入 Outbox，并以独立仓储 HTTP 替换模拟确认。员工仍只能请求出库，外部成功由可信回调/查单推进；不修改原 Operation 回执。
