# C1：事务 Outbox 与独立模拟仓储

2026-10-28。用户授权先实施 C1。本契约将 C 拆分，C1 的验收不替代 C2 的完整恢复矩阵。B2 冻结证据与旧数据库保留。

## 本批边界

C1 交付可选择启用的 HTTP 异步出库：合法出库命令在原业务事务内写申请状态、唯一 ShipmentRequest、Outbox、命令回执与审计；独立 Consumer 领取事件后在事务外访问独立仓储进程，确认成功时再通过原领域命令消费库存。仓储使用独立 SQLite 持久文件，不读写产品 PostgreSQL，无新增容器或依赖。

C2 才实现 Inbox/签名回调、自动查单合并、UNKNOWN 恢复、人工 redrive/reconcile 与完整杀进程/乱序/迟到成功矩阵。本批遇到无法判断是否受理、或已领取租约过期时必须隔离为 UNKNOWN，保留库存，停止自动 POST。不能因 HTTP 超时返回成功，不能自动释放库存或换业务键。

## 模式与信任

- `warehouse_delivery_enabled=false` 为默认值，保留 B1/B2 进程内演示兼容；与 `warehouse_simulation_enabled=true` 互斥。
- HTTP 模式的 `request_shipment` 原子产生持久记录；已存在 ShipmentRequest 即为该申请不可变的异步归属。之后即使切换配置，原模拟按钮也不能直接确认该申请。
- 旧进程内出库申请不回填、不自动投递。精确重放不能把旧命令变成一次新出库。
- Actor 来自 B2 session；请求中不得自填 mode、事件、外部单号或可信来源。成功由内部 `warehouse-outbox / warehouse / trusted_adapter` 在持有当前 Outbox 执行权的事务内推进。
- 原 Operation.receipt 保留“申请已提交”的历史事实。独立仓储仍为本机模拟，不是实际物流。

## Outbox 与执行权

ShipmentRequest 保存 application_id、稳定 effect_key、参数摘要、请求 payload、首次真实发起者、结果状态与外部单号。Outbox 保存不可变 payload、待投递状态、owner/generation、租约、尝试次数、下次执行时间和错误码；每次领取保存 Attempt。

领取采用短事务与 `FOR UPDATE SKIP LOCKED`，提交后再 HTTP。完成时在同一事务锁定并复核 owner/generation、租约未过期和当前状态，领域服务绑定同一连接的 SAVEPOINT；领域变化、库存消费、请求结果和 Outbox 完成统一提交。领取保护不能撤销已发出的 HTTP，外部重复由持久幂等绑定控制。

| 结果 | Outbox | 请求/履约结果 | 库存 |
|---|---|---|---|
| 有效成功凭据 | DELIVERED | SUCCEEDED，原领域推进为 REPLACEMENT_SHIPPED | 消费一次预留 |
| Provider 明确暂未受理 | PENDING，有界退避，最多 3 次尝试 | PENDING | 保留预留 |
| 明确暂未受理但重试耗尽 | DEAD | MANUAL_REVIEW | 保留预留 |
| Provider 明确永久拒绝 | DEAD | FAILED | 保留预留，等待后续受控处理 |
| 超时/不可信响应/可能已受理 | BLOCKED | UNKNOWN | 保留预留 |
| CLAIMED 租约过期 | BLOCKED | UNKNOWN，不重新 POST | 保留预留 |

锁序：Outbox → ShipmentRequest → 原领域 order/application/line/fulfillment/reservation/inventory。原领域仅插入新的 Outbox，不在持有业务锁时等待现有 Outbox 执行权。C2 新路径须复核该锁序。

## 独立仓储 HTTP 契约

Bearer 密钥至少 32 字符，通过环境配置，不作为命令行参数或日志内容。出站只允许无 userinfo/query/fragment/path 的 literal loopback HTTP 地址，无代理和重定向。Consumer 每次 HTTP 尝试在独立短命子进程执行。父进程默认 2 秒 monotonic 总预算包含启动与通信，到期 kill 后最多再等待 1 秒回收；子进程另有 POSIX alarm 限制缓慢 header/body。OS 创建/调度不是硬实时保证。租约默认 15 秒，Consumer 不做隐式网络重试，只有持久状态允许的明确未受理错误才能另行领取重试。

`POST /mock-warehouse/shipments` 输入严格四个业务字段及摘要：

```json
{"effect_key":"exchange:app_...","application_id":"app_...","variant_id":"tee-black-M","quantity":1,"request_hash":"sha256..."}
```

摘要是四个业务字段的 UTF-8 canonical JSON SHA256，键排序、ensure_ascii=false、separators=(",",":")。独立仓储重新校验摘要；effect_key 和 application_id 均唯一。同键同参数返回原出库记录，同键异参或同申请换键均拒绝。事务成功后才返回：effect_key、application_id、request_hash、external_shipment_id、status=SUCCEEDED。

`GET /mock-warehouse/shipments/by-key/{effect_key}` 返回同一持久结果；404 不能证明没有尚在途的旧请求，因此 C1 Consumer 不据此换键或重发。重启仓储保持幂等绑定。

测试故障接口只在显式测试模式且 Bearer 认证有效时开放；正常模式返回 404。C1 覆盖明确暂未受理、永久拒绝、已持久化后返回 500 三种情形。最后一种证明模糊 HTTP 失败不能直接计为失败/成功，不冒充真实断开 socket 或完整 C2 故障矩阵。

## 验收

- 增量迁移保留 B2 数据，旧请求不自动发送；事务回滚无 Outbox，精确重放只有一个请求/事件。
- 两个 Consumer 竞争只一个取得执行权，旧 generation/过期租约不能确认成功。
- 真实独立进程通过 HTTP 完成出库，重复请求与仓储重启仍一份出库记录。
- 原 Worker/Agent/HITL 与 B1/B2 全套回归不退化；未知结果、拒绝和耗尽不虚报成功，库存预留保留。
- 保存首轮失败、实际命令、源码/证据摘要与停止/清理结果。只使用 55434/aftercare_test；无模型调用、旧账本清库、推送或部署。
