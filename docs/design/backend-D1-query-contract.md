# D1 查询与分页兼容契约

2026-10-29。C 已结束。本批基于 0011_dispatch_intent 做 PostgreSQL 查询与 API 性能优化；完成 D1 后停止，不进入 D2。

## 修改边界

- 订单详情授权后批量读取商品变体；保持全部字段及明细语义。
- snapshot 仅将预览目标变体的逐条读取改为批量映射；内部事务、`_conversation` 行锁、`run_guard`、执行权及状态推导不变。外部只读 snapshot 与 SSE 锁优化属于 D2。
- 新增消息分页和显式启用的履约事件分页。现有消息写入、快照、默认事件返回和前端不变；申请列表保留已有 ID 游标分页。
- 不修改业务、Agent、仓储协议或恢复流程，不增加基础设施，不调用真实模型。

## 兼容接口

| 接口 | 无分页参数的行为 | 分页行为 |
|---|---|---|
| `GET /api/conversations/{id}` | 现有完整快照，原结构与字段 | 本批不增加参数 |
| `GET /api/conversations/{id}/messages` | 新接口，默认 limit=50 | limit=1..100；after 为消息 ID；返回完整消息 items 和 next_cursor |
| `GET /api/applications` | 保留已有 limit=20、after 字符串、items/next_cursor | 原有 application ID keyset，不修改契约 |
| `GET /api/applications/{id}/events` | 无 limit/after 时保持原 `{items: 完整历史}` | 显式 limit=1..100，after 为非负版本，返回 items/next_cursor；只传 after 返回 400 |

消息先校验客户对会话的归属，随后将 after 锚点限制在同一会话内，以 `(revision, created_at, id)` 的严格大于与同序排序读取 limit+1。不存在或跨会话锚点统一 400 INVALID_CURSOR；越权或不存在的会话仍按原身份规则 404，匿名 401。消息 ID 仅是游标定位，不承担授权作用。同 revision/time 的消息靠 ID 打破并列，不能错误使用 id>after 替代元组比较。

履约事件复用可信 Actor、归属、exchange 及履约存在性检查，按唯一 `(application_id, to_version)` 过滤和排序。next_cursor 为本页最后版本；多取一行只用来判断是否还有下一页，不能回传或丢掉这行。默认全量分支仍采用原服务路径。

keyset 翻页不是跨请求的一致性快照。只读页内按明确顺序返回；并发新增的后续项可在后续页看到。不得据此声称在任意并发插入时捕获某一时刻的全量视图。

## 测量约定

复用 A 的固定合成数据、SQLRecorder、3 次预热、30 次正式采样、单并发 TestClient/ASGI 方法。before/after 使用同一持久 fixture、时间、规模、顺序、数据库和依赖；完整响应 canonical JSON hash、字段、列表顺序必须一致，才能比较 N+1 优化收益。SQL 次数为应用 SQLAlchemy 游标执行次数，不包含驱动 ping 和事务线协议。

订单 1/10/100 行，历史预览 0/10/100，消息 10/100/1000。记录每次耗时、SQL、响应字节和失败，保存源码、迁移、数据库/主机版本、GUC、索引和 EXPLAIN ANALYZE BUFFERS。A 的旧结果只作历史参考，不跨源码版本归因。

分页首屏是另一种读取合同，不能将只取一页与返回全部记录直接算成同语义 p95 收益。单列每页体积、完整遍历的总请求/SQL/字节及完整投影一致性。旧工作台仍调用完整 snapshot，本批不声称已降低其历史消息网络体积；消息分页供后续显式客户端使用。

## 验收与证据保护

新增专项覆盖完整返回内容、重复变体、空集合、权限、预览确认条件、稳定游标、并列排序、末页/空页、锚点作用域、默认响应及员工停用。完整离线回归覆盖既有 B/C 事务与恢复执行权。基线 fixture 与 pytest 串行，pytest 清库前先留存测量与 C 的业务证据。

只使用 127.0.0.1:55434/aftercare_test；不连接旧 demo/真实账本库。A/B/C 首次报告、运行记录和冻结仓储快照保持原样。新增索引须由实际计划与负载支持，不为技术列表添加索引、迁移或服务。报告保留无改善及变慢指标，并说明本机小样本、热缓存、单并发 ASGI 的局限。
