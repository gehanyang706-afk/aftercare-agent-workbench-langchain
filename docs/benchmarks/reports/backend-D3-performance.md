# D3 多 Worker 与关联日志实测

2026-10-30。本批交付是受执行权保护的多进程验收与可定位故障的最小日志。固定负载下增加 Worker 缩短排队；开启日志后的吞吐变化为 **−0.865%～+0.368%**，不能据此宣称代码提速或日志零开销。所有 36 组正常路径矩阵通过，失败处理另见真实进程验收。

[契约](../../design/backend-D3-worker-contract.md) · [交接](../../handoffs/backend-D3.md) · [验证](backend-D3-validation.json)

## 版本、环境与负载

- before 为当前 D2 产品 78 个文件指纹 `656cf69a59f3c2efb5cb89f043526fcf978a3b5e16d16d88eb54259b37f67ffc`；after 为加入日志并显式开启后的 79 个文件指纹 `0fbd72c44b873a843e76d688c278db79d3ff4c773efabbee1e0d93c443f7eb61`。不是旧 A/C 性能横向归因。
- 同一测量脚本 SHA256 `7d787a17ef51dd3d65ba9479b7d6297bc822b880bc8d53d954449bc3f27a9b5c`，依赖锁 SHA256 `44388bf7ef0c789719a917fd2a862c30f8fbaeca40458582f88f8701c0c9aa65`。产品仅 CLI、两个履约 Worker 与新观察模块变化；184 项完整源码指纹在验证清单中。
- macOS Darwin 27.0.0/arm64、Python 3.13.14；PG `17.11 (Debian 17.11-1.pgdg12+2)`，`0011_dispatch_intent`，仅 `127.0.0.1:55434/aftercare_test`。数据库 role aftercare，fsync/synchronous_commit 均 on；shared_buffers128MB、work_mem4MB、effective_cache_size4GB、max_connections100、jit on、track_io_timing off。
- 开始时 7 个订单/7 行、7 个变体和库存行、1 会话/2 消息/5 事件，所有 ShipmentRequest/Outbox/Query/Inbox 均为空。完整原表计数和内容哈希逐组、逐阶段还原；不 TRUNCATE、不回退 sequence。
- 每侧 2 类消费者 × 1/2/4 进程 × 3 轮 × 12 个合法申请，共 **18 组、216 次正式处理**；两侧合计 432 次，其中 Outbox216、Query216。Query 另有真实 POST 准备 UNKNOWN，准备不计 GET 正式吞吐。不是随机到达或持续容量负载。
- 每组同目标 SKU、数量1，库存初始20；完成后可用8、预留0、消费12，申请v6且每单6条审计。保留原 receipt 和 triggered_by。性能使用可信测试 Actor；真实员工 session/HTTP链另在可靠性验收。
- 独立仓储 HTTP 进程/SQLite，实际 BoundedWarehouseClient 及原 CLI work 循环，保留每轮0.5秒等待。每个 Worker 就绪后共同门闩放行；启动、准备另记，不含吞吐分母。每侧完整运行约 127.943/129.146 秒。

## 正式测量

吞吐 p50 是 **3轮整批TPS中位数**，TPS的p95就是3轮最大值，不是容量百分位。排队、执行、HTTP分位汇总每档 **36次操作**；单位ms。Outbox排队为门闩到持久 attempt.started_at；Query使用run_once入口代理，早于maintenance/claim，不能当精确数据库领取时间。创建到开始另存原始证据，含fixture及启动等待。

|模式|Worker|phase|吞吐 p50 / p95 TPS|排队 p50 / p95|执行 p50 / p95|HTTP p50 / p95|SQL 三轮|锁采样|fleet RSS峰值 MiB|provider RSS峰值 MiB|
|---|---:|---|---|---|---|---|---|---:|---:|---:|
|outbox|1|before|1.479 / 1.488|4006.713 / 7926.097|207.369 / 237.529|153.291 / 159.183|[1068, 1068, 1068]|0|125.9|67.5|
|outbox|1|after|1.478 / 1.481|3981.507 / 7918.794|208.721 / 223.541|155.639 / 167.103|[1068, 1068, 1068]|0|126.0|67.7|
|outbox|2|before|3.096 / 3.098|1851.064 / 3676.759|216.840 / 251.228|154.539 / 164.848|[1068, 1068, 1068]|13|251.3|67.7|
|outbox|2|after|3.083 / 3.092|1867.039 / 3688.794|219.973 / 257.710|158.562 / 170.981|[1068, 1068, 1068]|9|251.3|67.8|
|outbox|4|before|6.673 / 6.685|818.358 / 1568.509|235.695 / 326.193|168.731 / 188.085|[1068, 1068, 1068]|22|499.6|68.1|
|outbox|4|after|6.615 / 6.691|823.344 / 1585.166|242.048 / 322.218|174.959 / 183.065|[1068, 1068, 1068]|25|493.5|68.0|
|query|1|before|1.501 / 1.507|3916.505 / 7799.539|199.178 / 207.897|154.358 / 161.434|[696, 696, 696]|0|126.2|67.7|
|query|1|after|1.507 / 1.508|3893.685 / 7769.542|197.530 / 212.499|153.123 / 162.131|[696, 696, 696]|0|126.1|67.7|
|query|2|before|3.160 / 3.191|1798.081 / 3596.095|201.446 / 239.697|155.572 / 165.327|[696, 696, 696]|9|248.9|67.7|
|query|2|after|3.151 / 3.157|1806.195 / 3598.911|205.673 / 248.562|158.160 / 165.754|[696, 696, 696]|9|251.5|67.7|
|query|4|before|6.920 / 6.948|780.763 / 1520.194|218.011 / 288.743|169.294 / 179.917|[696, 696, 696]|12|500.1|67.9|
|query|4|after|6.861 / 6.869|786.436 / 1534.707|233.430 / 297.767|177.092 / 216.622|[696, 696, 696]|14|497.1|67.8|

每组12个请求都只有12次相应正式外部尝试，0失败/0未完成；Query阶段只有GET，provider事实及外部回执逐行保持。before/after SQL分别稳定为 Outbox1068、Query696次/组；统计是Worker SQLAlchemy游标执行，包含原maintenance/claim/check/finish，排除pre_ping/事务控制以及父进程/采样SQL。相同SQL数量不等于整个数据库负载或查询耗时完全相同。

## 开销和未改善指标

百分比先用原始统计量计算再显示小数，未先舍入分母：

- outbox / 1 Worker：吞吐中位数 -0.008%，执行 p95 -5.889%，HTTP p95 +4.976%；SQL 三轮相同。
- outbox / 2 Worker：吞吐中位数 -0.422%，执行 p95 +2.580%，HTTP p95 +3.721%；SQL 三轮相同。
- outbox / 4 Worker：吞吐中位数 -0.865%，执行 p95 -1.219%，HTTP p95 -2.669%；SQL 三轮相同。
- query / 1 Worker：吞吐中位数 +0.368%，执行 p95 +2.214%，HTTP p95 +0.432%；SQL 三轮相同。
- query / 2 Worker：吞吐中位数 -0.289%，执行 p95 +3.698%，HTTP p95 +0.258%；SQL 三轮相同。
- query / 4 Worker：吞吐中位数 -0.849%，执行 p95 +3.125%，HTTP p95 +20.401%；SQL 三轮相同。

特别保留 Query/4 的 HTTP p95 **179.917→216.622ms（+20.401%）**、执行p95 **288.743→297.767ms**。Outbox/4吞吐 **6.673→6.615任务/秒**。这次顺序本地测试无法将全部波动归因于日志，但不能隐去回退或声称零开销。

固定12任务、首轮立即放行、最后一轮不计0.5秒sleep，使4/1吞吐比例可以超过4；不能称线性或超线性扩容。原sleep与每次独立HTTP子进程影响结果。4进程的较短排队来自本负载下更多并发执行，不能外推生产QPS/SLA。

## 等待和资源

主表锁采样是三轮采样记录之和；重复采样可能属于同一等待，不是锁事件次数或持续时间。PG采样名义20ms，还叠加查询、ps和调度；单Worker未采到等待，不代表完全没有短等待。全部36组采样错误为0，原始wait_event/blocking PID记录可查。

主表RSS是各轮采样峰值中的最大值，Worker fleet包括当时可见的有界HTTP后代，独立仓储另列，排除父测量进程/PostgreSQL。4Worker约494～500MiB的fleet RSS，不是整机或Docker内存。共享页可能重复计数，至少100ms采样可能漏短命HTTP子进程/瞬时峰值。

|模式/Worker|before Worker CPU 秒（三轮）|after Worker CPU 秒（三轮）|before/after FOR UPDATE 次数（每轮）|
|---|---|---|---|
|outbox/1|1.48/1.75/1.70|1.62/1.87/1.56|[504]/[504]|
|outbox/2|1.98/1.84/1.91|1.78/1.71/1.91|[504]/[504]|
|outbox/4|2.50/2.16/2.51|2.36/2.26/2.38|[504]/[504]|
|query/1|1.67/1.50/1.95|1.65/1.62/1.43|[300]/[300]|
|query/2|1.64/1.38/1.91|1.74/1.79/1.52|[300]/[300]|
|query/4|2.44/2.44/2.25|2.28/2.34/2.63|[300]/[300]|

CPU是ps观测到的**生命周期累计下界**，包括Worker启动；不是门闩区间CPU或峰值百分比。仓储每轮累计CPU为before0.22～0.26秒、after0.22～0.26秒，Query还包含准备POST。不能用这些CPU差异宣称优化收益。原始文件保留每次采样、startup_to_all_ready、游标总耗时、创建到开始等待。

## 日志与证据

before结构化日志0条；after **2592条、1296对阶段记录、216个完整cycle**，每个请求可关联application/effect/shipment_request/outbox或task/owner/generation，Query不虚构attempt。未观察logging_dropped字段；这只说明本次正常输出未报告丢弃，SIGKILL与背压下仍可能丢尾部。默认关，`--log-json`显式开，stdout不变。

真实进程可靠性和完整回归独立于上述正常路径矩阵，不混算吞吐；最终数量、失败记录和进程清理结果见[交接](../../handoffs/backend-D3.md)。日志不替代数据库/仓储事实。本批没有跨HTTP/Agent/Inbox全链路Trace、监控告警或生产采集系统。

原始目录：`runtime/backend-D3/before-first/`、`after-first/`、`comparison-first/`；额外只读交叉核对在`analysis-summary.json/.md`。每组业务终态JSON、provider JSON、SQLite backup、Worker journal/stdout/stderr、资源等待采样全部保留。摘要首版误用三位显示值算变化率，保留`analysis-summary-first.*`并从原数修正；原始数据和测量脚本未变。

## 可比范围和复现

相同host/PG/GUC/依赖/脚本/负载规格/逻辑基表内容已检查；before/after顺序执行，生成ID、时间和请求哈希不同。逻辑行还原不等于物理页、缓存、膨胀、planner统计、autovacuum、OS负载或sequence还原，没有随机化和因果置信区间。不宣称前后响应字节相同、不宣称长期容量或模型收益。

```bash
# 在项目根目录，先确认专用测试库没有未完成工作，使用新的输出目录。
DATABASE_URL=postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55434/aftercare_test \
ALLOW_REAL_MODEL_CALLS=false PYTHONPATH=src:. .venv/bin/python scripts/backend_d3_benchmark.py \
measure --label after --output-dir runtime/backend-D3/new-after
```

重建before须使用冻结D2源码而非将after标成before；脚本的label不负责替换源码。测量中修改产品/harness会拒绝比较；不与pytest或其他Worker同时运行。不覆盖现有目录，首次失败保留后停止，不自动加轮直到出现有利结果。
