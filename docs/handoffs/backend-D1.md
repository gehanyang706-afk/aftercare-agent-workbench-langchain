# D1 查询、API 性能与兼容分页交接

2026-10-29。D1 已完成并停止，D2 未启动。基于 `0011_dispatch_intent` 修复两处确定性 N+1，新增显式分页读取，保持原默认响应和领域保护。没有新增迁移、索引、依赖或基础设施。

[性能报告](../benchmarks/reports/backend-D1-performance.md) · [验证清单](../benchmarks/reports/backend-D1-validation.json) · [兼容契约](../design/backend-D1-query-contract.md) · [C 历史交接](backend-C-hardening.md)

## 实现与保持的边界

- `services/business.py::get_order`：原订单归属查询后读取明细，一次 IN 批量取变体；保留全部字段、列表次序，空订单不查变体。
- `BusinessService._preview_views`：一次批量映射颜色/尺码，保留每个预览原字段、标签格式和逐项时钟判断。snapshot 只将 previews 渲染交给 helper；原事务、`_conversation FOR UPDATE`、run_guard、消息求值顺序及状态推导的 AST 保持一致。
- 新 `services/read_pages.py` 与 `api/read_pages.py`：消息用同会话锚点和 `(revision,created_at,id)` keyset，默认50、最大100，返回完整消息行；先归属校验，再验证锚点。新只读页不调用内部 snapshot。
- `api/fulfillment.py::events`：显式 limit 才启用 to_version keyset，复用身份、归属与履约门禁；默认仍为原服务的 `{items:完整历史}`。`app.py` 只注册新 router。
- 申请列表原有 ID keyset 保持原样。本批没有修改前端、SSE、写命令、员工认证、领域事务、仓储协议或恢复代码。

一个完整读取过程：客户调用新消息页 → 原服务器会话认证 → READ ONLY/REPEATABLE READ 中校验会话归属 → 同会话解析 after 锚点 → 按原排序元组严格大于、LIMIT+1 → 返回完整记录及可验证的 next_cursor。多取的一行只用于判断下一页，下一页会正常包含它。游标是定位信息，不是授权凭据，也不提供跨请求的时间点快照。

## 测量与结果

复用 A fixture、SQLRecorder 和 p95 方法。相同固定实体 ID、业务时钟、数据投影、主机、依赖、PG 版本/GUC；单并发 ASGI/TestClient，每档3预热+30正式采样。主对照9档、每侧270正式请求，均0失败；完整 JSON hash、值、列表顺序及字节数逐项相同。

| 目标 | SQL 前→后 | p50 ms 前→后 | p95 ms 前→后 | 响应字节 |
|---|---:|---:|---:|---:|
| 100行订单 | 103→4 | 29.431→5.282 | 36.456→9.312 | 41682，不变 |
| 100个预览 | 209→10 | 57.562→8.463 | 58.666→9.640 | 65356，不变 |

全部1/10/100订单、0/10/100预览、10/100/1000消息，以及申请/事件默认 API 的 SQL、p50/p95、字节、失败分母见性能报告，不跨旧 A 版本归因。

首次基线后发生后台 autoanalyze，原 `before-first` 留存。备份候选后，在受控窗口临时恢复冻结 C 的 business.py，校验整个产品源码仍与原始基线相同，补测 `before-stable` 后 finally 原样恢复候选。四份真实 pg_stats 指纹一致，证明最终主对照未混入 planner 统计变化。comparison 的 statistics_changed=true 保留；仅 demo_sessions.n_dead_tup 1→3，来自临时认证行生命周期，不是列统计变化。

消息首屏50条为4 SQL、12646字节、p50/p95=3.428/3.529ms；全部1000条要20请求、99 SQL、256145字节、p50/p95=80.124/86.245ms。完整拼接逐值等于原消息投影，但完整遍历比一次 snapshot 更贵。现有 UI 仍读取完整 snapshot，不能宣称其消息体积已经下降。

申请5条/每页2条：完整遍历3请求、9 SQL、1223字节。履约真实6条审计/每页2条：3请求、15 SQL、3800字节。旧默认响应严格相同，不通过生成不合法的千条状态转换制造事件规模。

11份实际分页 SELECT 的 EXPLAIN ANALYZE BUFFERS 和全索引目录已保存。订单/预览已有过滤索引与变体主键；消息页当前仍 SeqScan+Sort，首屏查询约0.120ms、深页约0.066ms，没有声称数据库扫描成本恒定。6条 audit 的小排序约0.006–0.007ms。缺少更大规模及 API 收益对照时未加入猜测性复合索引，保留0011。

负向指标保留：单行订单和零预览未提速；100消息 p95 6.237→6.404ms；全部旧响应体积不变；分页完整读取增加请求和总字节。没有生产QPS、SLA、并发锁或模型性能结论。

## 工程验证

| 批次 | 结果 | 原始记录 |
|---|---|---|
| D1专项与旧UI/API兼容 | **96 passed，35.79秒** | runtime/backend-D1/integration-first.xml/.log |
| 完整离线回归 | **761 passed / 4 skipped / 1 deselected，111.62秒** | runtime/backend-D1/full-first.xml/.log |

新增54项：批量读取/权限/时钟/执行保护26，分页/身份/稳定游标/默认响应28。专项含旧 web 和履约 API，不能把专项与全量通过数相加。完整套件覆盖 B1/B2/C1/C2/C 补强的权限、事务、库存、幂等、Outbox、Inbox、UNKNOWN、租约及旧 Worker 失权。4项跳过仍为可选tau2依赖，1项排除仍为权重推理；没有新增D1跳过，也没有实际测试失败后放宽断言。

源码保护复核：除 business.py 两个读取入口、app router注册和fulfillment事件路由外，C 的已有源码均未改变；新功能只在新增文件中。snapshot 的唯一结构变化为 previews 渲染 helper，其他业务方法、routes.snapshot/events 及全部 Worker/履约/恢复服务保持冻结版本。Ruff、格式、依赖一致性与 diff 检查通过。

## 数据与证据保护

开始时 C 源码/证据全部匹配，专用测试库与 C 最终只读业务行逐值一致，保存 database-before.json。性能 prepare 只新增唯一前缀；pair 前后 fixture hash 和其他业务投影不变。性能完成后显式 cleanup 只删自己的前缀，恢复原表计数；C 业务/库存仍逐值相同，保存 database-after-measurement.json。之后才运行已授权、会清理专用测试库的 pytest。

当前专用库为完整测试留下的临时fixture，不能称为C矩阵最终数据。最终只读核对 schema=0011、health=ready，库存及订单数量违规均0；见 database-final.json。C 的真实矩阵、manual预留、原快照/首次失败和A/B/C报告继续保存在原证据中，没有为成绩清成成功。

只使用55434/aftercare_test，没有连接或重置旧demo、真实账本库；没有真实模型/holdout/Test，没有新容器、依赖、公共部署或提交/推送。

## 复现与停止点

`scripts/backend_d1_benchmark.py` 的 prepare→measure before→measure after→compare→cleanup 使用持久pair；主脚本与A原脚本均有前后相同SHA。独立 `backend_d1_pages_benchmark.py` 记录旧默认响应及新首屏/全遍历；`backend_d1_plan_probe.py` 保存实际计划与索引。所有脚本仅允许专用测试库、显式关闭真实模型、新输出目录且拒绝覆盖。

```bash
AFTERCARE_TEST_DATABASE_URL=postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55434/aftercare_test \
ALLOW_REAL_MODEL_CALLS=false .venv/bin/python -m pytest -m 'not live_model and not model_weights'
```

本批pair已清理，脚本会拒绝把它当活数据复测；旧数据投影与源码仍可审计。新复测需新pair，先保留当前测试数据、协调无并行测试。分页补充还需真实合法申请/审计作为参考，不会凭空生成业务成绩。

**D1到此停止。D2的只读snapshot分离、SSE锁/轮询、多Worker性能及Trace/部署均未启动，不自动推进。**
