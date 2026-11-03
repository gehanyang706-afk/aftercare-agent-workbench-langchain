# C2：可信回调、Inbox、查单合并与人工恢复

2026-10-28。用户已批准 C2。沿用 C1 的稳定业务键、不可变请求、库存预留和原申请回执。本批不实施 D 的性能优化、容器网络或公共部署。

## 结果与身份边界

仓储回调使用独立 HMAC 密钥，签名覆盖 `timestamp + "." + 原始 body bytes`，来源固定 `mock-warehouse-v1`，时间窗 ±300 秒。产品在身份校验后有界读取最多 16 KiB，严格校验事件字段。人类 session 不替代仓储签名，人工接口不能自填成功、单号或 trusted_adapter。

回调事件包括 event_id、effect_key、application_id、request_hash、external_shipment_id、sequence、status。seq1 为 ACCEPTED、seq2 为 SUCCEEDED。Inbox 先持久接收，再处理业务；202 仅代表事件已持久保存，不表示已出库。

| 情况 | 处理 |
|---|---|
| 同来源同事件 ID、同语义 payload | 去重，保持原业务结果 |
| 同事件 ID、异内容 | 持久冲突证据，409，不覆盖原事件 |
| seq2 先于 seq1 | 保存 PENDING/GAP，安排查单；seq1 到达后可处理已保存 seq2 |
| 旧序号、相同已知事实 | 标记 STALE，不回退状态 |
| 未知键、错误关联或不同外部单 | 保存 PENDING/CONFLICT，不凭该事件消费库存 |
| 可信查单成功 | 校验键、申请、摘要、外部单与 provider sequence，统一合并 |

回调、原投递成功和查单成功复用一个合并机制，在同一事务内调用原 `confirm_shipment`、消费一次库存、更新请求/Outbox/Inbox/查询任务和审计。回调可先于投递响应；旧投递或查询 generation 不能覆盖已合并事实。原 Operation.receipt 始终保留申请提交事实。

## 查单与执行权

UNKNOWN 只通过 GET 查原键。404 只是查询时未找到，不允许换键重发或释放预留。查询任务记录 owner/generation、租约、尝试数、下一次执行与总截止。默认每轮最多 3 次、60 秒截止，有限退避；查询租约过期可安全接管 GET，旧 owner/generation 的写回被拒绝。次数或总时限耗尽进入 MANUAL_REVIEW，库存仍保留，自动调度不得重置耗尽预算。

新查询任务只为未成功、且尚无 ReconciliationTask 的请求产生；已有任务由自己的领取/预算队列处理，不反复占新任务候选名额，不自动重开 DONE/EXHAUSTED。若先前 DONE 的任务对应请求经 redrive 后再次出现 UNKNOWN，需客服显式 reconcile 新一轮查询。

网络在事务外，仍使用 C1 的独立尝试子进程总时限。锁序以 Outbox 门闩起步：Outbox → ShipmentRequest → 查询任务/Inbox → 原领域资源；领取不可先持查询任务锁再等待 Outbox，防止与回调合并成环。

## 人工命令

`POST /api/fulfillment-requests/{id}/reconcile` 与 `POST /api/outbox/{id}/redrive` 仅真实 support 会话可调用，要求 CSRF、同源、Idempotency-Key、expected_generation 与操作理由。复用 B2 认证共享锁与同连接 SAVEPOINT，事务内只排任务，不进行网络调用。

reconcile 调度有界查单；redrive 仅允许明确未受理的可恢复死信。UNKNOWN 和缺少未受理证明的 MANUAL_REVIEW 不允许直接 POST。重投复用原键/原参数，attempts 与 generation 单调，每轮 cycle_attempts 可重置，人工轮次有限并留下命令回执与理由。精确重放不重新打开预算，不覆盖首次操作人。

## 独立仓储事件

SQLite v1 → v2 保留 C1 所有出库事实并补充持久事件，不重新出库。新出库事实与两条事件在同一 SQLite 事务写入。回调发送器领取后在事务外发送，失败保留并有界重试；200/202 的有效 Inbox 回执才算发送确认，不能解释为业务出库完成。显式配置时，独立仓储会在原 HTTP 响应返回前尝试回调，用于验证响应顺序竞争；默认不开启回调。

## 验收顺序与边界

先对 C1 留存的两个 UNKNOWN 做真实查单恢复，原 provider 快照只复制到新目录，原 C1 文件/报告不回写。保存 PostgreSQL 迁移与业务前后证据后，才运行重置专用 aftercare_test 的工程测试。

本批覆盖：回调重复/乱序/同 ID 异参/伪来源/早于 HTTP 响应；查单连续不可用与有限停止；原投递/查询 Worker 被杀或接管后的旧写回；明确可恢复死信的受控重投。使用真实独立进程与本机 HTTP，保留首轮失败与实际触发点，不以对象重建替代杀进程，不推导生产 SLA。

只使用 `127.0.0.1:55434/aftercare_test`。不运行真实模型、独立 holdout/Test，不新建容器/下载依赖、不重置旧账本、不推送或部署。
