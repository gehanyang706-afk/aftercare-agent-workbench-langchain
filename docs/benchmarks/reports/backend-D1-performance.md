# D1 PostgreSQL 查询与 API 性能对照

2026-10-29。D1 只修改确定性 N+1 和兼容分页。主对照中 100 行订单从 103 次 SQL 降为 4 次、p95 从 36.456 降为 9.312 ms；100 个预览从 209 次降为 10 次、p95 从 58.666 降为 9.640 ms。完整响应字段、值、顺序及字节数保持一致。分页另列首屏和完整遍历成本，不将少取一页当成完整响应的等价性能收益。

[兼容契约](../../design/backend-D1-query-contract.md) · [工程交接](../../handoffs/backend-D1.md) · [验证与指纹](backend-D1-validation.json)

## 条件与归因范围

- 同一台 Darwin 27.0.0 / arm64，Python 3.13.14；PostgreSQL 17.11 (Debian 17.11-1.pgdg12+2)，固定迁移 0011_dispatch_intent。只访问 127.0.0.1:55434/aftercare_test，无新迁移或索引。
- 复用 A 脚本 fixture、SQLRecorder、nearest-rank p95 方法；单并发进程内 TestClient/ASGI，真实 PG，每档 3 次预热、30 次采样。p50 为中位数，p95 为第 29 个有序样本；应用 SQL 计数包含认证，排除驱动 ping 和事务线协议。
- 主对照 9 档、每侧 270 次正式请求，失败均为 0；另有每侧 27 次预热。snapshot-messages-10 与 snapshot-previews-0 是同一路径的重复控制，不当成独立负载。每次耗时、SQL、字节、HTTP 状态及失败分母保留在原 JSON。
- 固定 seed `backend-D1-20261029-first`、相同实体 ID/时间/数据投影；fixture 为 3 订单/111 明细、100 变体/库存、7 会话、1150 消息、110 预览、1150 事件、1 非当前策略。1000 条消息档位和预览档位各自独立。当前 C 的真实模拟业务额外 5 条申请和 6 条审计事件用于分页对照，不制造不可能的千条履约状态转换。
- PG 条件：shared_buffers=128MB、work_mem=4MB、JIT=on、random_page_cost=4、UTC；完整 GUC、索引、表规模、计划和主机条件见 result.json。未隔离 CPU/内存、未清空缓存，不是 TCP/QPS、并发容量、生产 SLA 或模型性能。
- 优化前产品指纹：`fa434d1cd437186c880c147b0721d0e8a86e5b0aa753c13036a779a322d4d0b4`；优化后：`d739fe7a43005b663c9a66b7fcd31ddf2c429b284c73702e4eb1d2112dfa5a8c`。主测量脚本 SHA256 两侧相同：`04b3df7450a50dd90adbe24ef1f8c6269fd68567c14f346886022aaf6f87e100`，A 原脚本与依赖锁也相同。源码是未提交工作树，不能只用共同 Git HEAD 表示版本相同。
- 旧 A 报告仅历史参考。本批只比较本批冻结前后源码在同一 fixture、环境和方法上的表现，不跨 A/B/C 版本归因。

## 测量条件变化与补测

首次 `before-first` 本身成功、无请求失败；随后发现后台 autoanalyze 更新业务表统计，因此不把它作为最终时延归因基线，原记录保留。备份候选 business.py 后临时恢复已冻结的 C 原文件，核对整个产品 Python 指纹与原始基线完全相同，补测 `before-stable` 和旧分页 API，再在 finally 原样恢复候选。无 reset/stash、没有覆盖其他未提交改动。

稳定基线前后、候选测量前后四份实际 pg_stats 内容 SHA256 均为 `8853d40e747ea218502085c910d13ffbbafbdc949c88b500697c6090aacdb2bd`。pair 的数据、时间、依赖、GUC、采样、主机和完整响应哈希均通过比较门禁。比较文件的 statistics_changed=true 仅因 demo_sessions.n_dead_tup 从 1 变为 3，来自临时认证 session 的创建/清理；实际 pg_stats 及 ANALYZE 信息没有改变，不能把该运行计数当成 planner 漂移。

## 主 API 同语义对照

时间单位 ms，响应为未压缩 JSON body 字节。9 档完整 canonical JSON 哈希逐一相同，不删除字段、截断记录或修改权限。

| 档位 | SQL 前→后 | p50 前→后 | p95 前→后 | 字节 前→后 |
|---|---:|---:|---:|---:|
| order-lines-1 | 4 → 4 | 2.961 → 3.005 | 3.345 → 3.353 | 625 → 625 |
| order-lines-10 | 13 → 4 | 5.983 → 3.119 | 6.640 → 3.200 | 4310 → 4310 |
| order-lines-100 | 103 → 4 | 29.431 → 5.282 | 36.456 → 9.312 | 41682 → 41682 |
| snapshot-previews-0 | 9 → 9 | 4.741 → 5.461 | 4.947 → 5.644 | 2655 → 2655 |
| snapshot-previews-10 | 29 → 10 | 10.551 → 6.188 | 11.149 → 6.481 | 8885 → 8885 |
| snapshot-previews-100 | 209 → 10 | 57.562 → 8.463 | 58.666 → 9.640 | 65356 → 65356 |
| snapshot-messages-10 | 9 → 9 | 5.111 → 5.332 | 5.584 → 5.571 | 2655 → 2655 |
| snapshot-messages-100 | 9 → 9 | 5.598 → 6.124 | 6.237 → 6.404 | 25199 → 25199 |
| snapshot-messages-1000 | 9 → 9 | 14.264 → 14.354 | 28.255 → 17.028 | 254902 → 254902 |

订单先做原授权查询，随后取明细和一次 Variant IN 查询；预览先取原记录，再一次 Variant IN 映射。字典持有强引用，避免逐字段读取触发重复加载。空集合不做 Variant 查询。原 `_conversation`、run_guard、事务、行锁和状态推导不变；源码 AST 检查证明 snapshot 仅替换 previews 渲染表达式。

## 旧默认 API 兼容对照

3 档、每侧每档 30 正式样本和 3 预热，失败 0；完整响应哈希相同。申请列表原来已经有 ID keyset，未重复重做。

| 档位 | SQL 前→后 | p50 前→后 | p95 前→后 | 字节 前→后 |
|---|---:|---:|---:|---:|
| applications-default | 3 → 3 | 3.402 → 3.400 | 3.542 → 3.616 | 1095 → 1095 |
| applications-limit2 | 3 → 3 | 3.315 → 3.366 | 3.625 → 3.575 | 485 → 485 |
| fulfillment-events-default | 9 → 9 | 5.027 → 5.272 | 5.248 → 5.896 | 3727 → 3727 |

## 显式分页：首屏与全部遍历

旧工作台继续使用完整 snapshot，没有自动改接分页接口。新增 messages 默认每页 50、最大 100，按 revision/created_at/id 元组排序；events 只有显式 limit 才启用版本 keyset，默认仍只返回完整 items。申请列表沿用原接口。每个首屏和完整遍历档位均 30 次正式样本和 3 次预热、失败 0。

| 首屏 | SQL | p50 / p95 ms | JSON 字节 |
|---|---:|---:|---:|
| messages-first-page | 4 | 3.428 / 3.529 | 12646 |
| applications-first-page | 3 | 3.443 / 3.573 | 485 |
| fulfillment-events-first-page | 5 | 4.016 / 4.164 | 1078 |

| 完整遍历 | 总项数 / 页大小 | 请求数 | 总 SQL | p50 / p95 ms | 总 JSON 字节 |
|---|---:|---:|---:|---:|---:|
| messages | 1000 / 50 | 20 | 99 | 80.124 / 86.245 | 256145 |
| applications | 5 / 2 | 3 | 9 | 10.269 / 11.047 | 1223 |
| fulfillment-events | 6 / 2 | 3 | 15 | 12.258 / 12.455 | 3800 |

每次完整遍历逐页核对 prefix、游标、完整字段和最终投影，拼接结果与旧 snapshot.messages、旧 events.items、原完整申请列表逐值相同。消息首屏 12646 字节只包含显式要求的 50 条；全部 1000 条需要 20 请求、99 SQL、256145 字节、p50 80.124 ms，比完整 snapshot 一次 9 SQL、254902 字节、p50 14.354 ms 更贵。分页提供按需读取与响应上限，不能宣传完整历史加载更快，也不将消息投影等同完整快照合同。

## 执行计划与索引决定

主脚本保存实际订单/变体/预览/消息 SELECT 的 EXPLAIN ANALYZE BUFFERS，补充计划探针保存 7 个实际 HTTP 读取位置的 11 份计划及全表索引目录；计划采样不混入 API 时延统计。

- 订单明细已用 ix_order_lines_order_id；变体已有主键索引。本次瓶颈是重复往返，批量 IN 消除 99 次额外读取，不是增加索引。
- 预览已有 conversation_id 和活动预览索引；目标 Variant 批量读取。所有历史预览仍完整返回。
- 1000 消息首屏实际为顺扫 1000 条、过滤 150 条，再 top-N heapsort，执行约 0.120 ms；after950 的锚点用 messages_pkey，页查询仍顺扫、过滤 1100 条并排序，约 0.066 ms。当前不是数据库扫描成本恒定的分页，没有宣称 O(page_size)。
- 申请规模只有 5 条，计划有顺扫及小排序；当前不足以外推大申请表性能。履约 audit 有 application_id 索引及唯一(application_id,to_version)，6 行的页查询约 0.006–0.007 ms，当前选了单列索引加小排序。
- 本批没有添加缺少同条件 API 收益证据的索引，保留 0011。消息复合索引(conversation_id,revision,created_at,id)是未来更大历史规模下可验证的候选，不能把候选设计写成已完成收益。

## 未改善、失败与限制

单行订单 SQL 仍 4，p95 3.345→3.353 ms；零预览 SQL 仍 9，p95 4.947→5.644 ms；100 消息 SQL 仍 9，p95 6.237→6.404 ms。完整响应体积全部不变；旧 UI 的 1000 消息仍 254902 字节。未改变的控制路径也有时延波动，不能将所有数字变化都归因于批量查询。1000 消息控制的 p95 变化尤其不作为 D1 查询收益。

全部遍历比一次全量请求有额外往返和 JSON 包装成本；新接口保留这一真实代价。keyset 不提供跨请求时间点快照。页内 REPEATABLE READ/READ ONLY 不更改内部 snapshot 的写锁机制。实际业务权限、库存和恢复正确性另由专项/完整离线回归验证，见工程交接；本报告不是并发压测。

## 原始证据

`runtime/backend-D1/` 保留 source-before/database-before、pair-first/fixture-projection、before-first、before-stable、pages-before-first、after-first、pages-after-first、compare-first、plans-first、四份 pg-stats、窗口恢复记录、保护 AST 检查及清理记录。性能脚本仅移除自己前缀的数据；清理后 C 业务/库存逐值相同，之后才运行会清理专用测试库的 pytest。A/B/C 原始报告和故障证据不覆盖。
