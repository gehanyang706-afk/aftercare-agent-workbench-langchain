# D3 多 Worker 与关联日志交接

2026-10-30。D3 已完成后停止；不自动进入 CI/Docker。迁移仍为 `0011_dispatch_intent`。本批新增默认关闭的 Worker 关联日志，完成真实多进程容量描述、并发和故障验收，没有修改业务、Agent、仓储协议或恢复规则。

[性能报告](../benchmarks/reports/backend-D3-performance.md) · [实现契约](../design/backend-D3-worker-contract.md) · [验证清单](../benchmarks/reports/backend-D3-validation.json) · [D2历史交接](backend-D2.md)

## 实现范围

新增 `services/worker_observation.py`；CLI 和两个履约 Worker 接入 `--log-json`。每次 run_once 有独立 run_id，领取后关联 application/effect/shipment_request/outbox或task/owner/generation，按 maintenance、claim、permission、transport、finish 和 cycle 记录结果/耗时。Outbox记录真实attempt，查询只记录cycle。没有声称贯通 HTTP request、Agent、Inbox 的完整 Trace。

默认关闭，不生成日志线程/run_id，不增加SQL；原stdout和返回对象不变。开启后只向stderr写白名单标识符、状态和耗时，不记录payload、凭据或原始异常。内置sink有界非阻塞入队、独立daemon writer、复制stderr FD，队列满/写失败丢日志；正常退出有界收尾。慢输出和SIGKILL可丢尾部，日志不是审计账本或业务凭证。

逐段对比D3冻结前源文件，Outbox的expire→claim→begin_delivery→HTTP→finish、Query的expire/ensure/claim/check→GET→finish及catch码/返回字段保持原顺序。所有领域和恢复service、仓储client/provider、内部snapshot/run_guard、SSE和前端保持D2字节一致。日志的局部包装仍有CPU开销，默认关闭不等于数学上的零成本。

新增两个脚本和两个测试文件：`backend_d3_benchmark.py`、`backend_d3_reliability_smoke.py`、`test_worker_observation.py`、`test_worker_concurrency.py`。无迁移/索引/依赖/容器变化。

## 实测结果

每侧2类消费者×1/2/4进程×3轮×12任务，共18组/216次正式处理；前后合计36组/432次全部通过。固定合法申请、共享目标SKU、库存20；每组最终可用8/预留0/消费12，v6、原receipt及triggered_by不变。Query准备UNKNOWN的真实POST不计正式GET吞吐；正式阶段仓储事实不变且不再POST。

|消费者|1 Worker TPS before→after|2 Worker TPS before→after|4 Worker TPS before→after|每组SQL before→after|
|---|---:|---:|---:|---:|
|Outbox|1.479→1.478|3.096→3.083|6.673→6.615|1068→1068|
|Query|1.501→1.507|3.160→3.151|6.920→6.861|696→696|

TPS为三轮整批中位数。增加Worker缩短本负载排队，但有限12任务的末轮不计sleep，可出现4/1超过4，不能称线性扩容。日志前后吞吐变化−0.865%～+0.368%，不是代码提速证据。Query/4 HTTP p95 179.917→216.622ms（+20.401%），执行p95也回退；全部负结果和资源/等待口径在性能报告中。

## 测试与真实进程故障

|验证|结果|原始记录|
|---|---|---|
|日志纯unit初版|30 passed，0.08秒|runtime/backend-D3/observation-unit-20261030/first.log|
|补满stderr真实管道后|31 passed，0.23秒|同目录expanded-first.log，未覆盖初版|
|D3及原C保护专项|123 passed，18.26秒|runtime/backend-D3/focused-first.xml/.log|
|完整离线回归|842 passed / 4 skipped / 1 deselected，117.25秒|runtime/backend-D3/full-first.xml/.log|
|独立前端队列unit|9 passed|runtime/backend-D3/refresh-unit-existing-runtime.log|
|真实进程/HTTP故障|5/5通过，19.856秒|runtime/backend-D3/reliability-first/reliability-first.json|

Python新增38项=31观察unit+7并发集成；804+38=842。专项和真实故障不再叠加到全量，Node9独立计数。四个跳过仍为可选tau2缺依赖，一个排除仍为模型权重推理；没有新增跳过或隐藏失败。完整回归包含B1/B2权限/事务/库存/幂等、C1/C2/发送许可/Inbox/UNKNOWN/Worker执行权及D1/D2读路径保护。

真实探针使用员工session经HTTP批准出库、原BoundedWarehouseClient、独立仓储进程，最多5个同时活跃Worker：

|场景|实际观察|
|---|---|
|4进程争抢1请求|1成功、3 IDLE，1次POST、1份仓储事实|
|CLAIMED/NOT_STARTED后SIGKILL|自然3秒租约到期，4替代者仅1次发送成功|
|仓储真实受理后、finish前SIGKILL|自然到期转UNKNOWN，2查询者只1次GET恢复，无第二次POST|
|旧Worker许可前暂停至失权|新Worker成功；旧begin_delivery拒绝，不发新HTTP|
|旧Worker受理后暂停至失权|查询先完成；旧finish拒绝，原终态不变|

20个真实Worker进程（含2个SIGKILL旧进程）、2个服务进程有PID/门闩/暂停点及自然过期证据。每案最终1份出库事实、v6、CONSUMED、6条审计，原receipt及server_session仓库操作者保持。五案总库存20=15可用+0预留+5消费。正常退出日志丢弃/待写计数均0；两次SIGKILL尾部未知，依数据库和provider事实判断。

## 一个完整定位过程

`stale-after-accept`使用真实申请 `app_d86eacc050824d0eb3c0d63dfd4e020d`，effect_key为`exchange:`加该ID。

1. 旧sender `d3-old-38518`、generation1完成begin_delivery；暂停标记证明实际HTTP已成功受理，但尚未finish。数据库仍v5、RESERVED，仓储已有1份事实。
2. 等待真实3秒租约过期，查询Worker的同一effect日志关联原shipment_request和outbox，经一次GET完成原事务合并，申请变v6、库存消费一次。
3. 放行旧sender，其transport日志虽为SUCCEEDED，`finish failed/OUTBOX_LEASE_LOST`与`cycle LEASE_LOST`仍记录同一owner/generation/run_id。旧HTTP成功不等于旧执行仍有完成权。
4. 最终provider POST总数1；GET总数2，其中1次是入队前查不存在的404，故障恢复实际只1次GET。旧Worker回来前后业务快照完全相同。

原始关联片段和终态在 `runtime/backend-D3/trace-example.json`，包含具体源日志文件。故障client在真实HTTP返回后人为暂停，因此本探针transport日志跨度包含暂停，不当作网络耗时；性能矩阵没有该暂停。

after性能矩阵2592条日志、1296对阶段、216完整cycle；真实探针另160条日志。两份计数不混算，日志丢失也不能用缺行证明没发送。

## 证据与收尾

D2开始时179项源码、62份证据及历史D1/C快照通过哈希校验；D3只改D2中的CLI和两个Worker，另增5项源码，最终184项清单见验证JSON。性能前后同一脚本/依赖/PG/GUC/负载规格，逐组清理精确拥有的ID并验证所有原行hash恢复，不回退sequence。之后才运行会重置专用库的现有pytest fixture，最后留下本批5条真实业务事实。当前库不再被描述成C原故障矩阵。

原A/B/C/D1/D2报告、性能/故障输出和一致SQLite backup保持历史内容。D3每组SQLite通过backup导出`provider-final.sqlite3`并核验，活动warehouse.sqlite3及WAL/SHM不作为冻结一致快照。最终库健康、库存/订单数量约束、临时8150/8151/8152端口和进程组清理见验证清单；临时员工已禁用，业务事实保留。

真实探针没有启用回调循环，仓储自动生成的10个回调事件保持PENDING，产品Inbox为空。既有回调/乱序保护由完整离线回归覆盖，不能把本批五案说成新增真实回调验收。

工具问题另列 `runtime/backend-D3/tool-issues.json`：初次Node命令因PATH未启动，改用应用现有Node24.22.1通过；派生摘要曾用舍入数字算变化率，保留first并改用原统计量，原始矩阵不变。自动审批服务两次只读检查失败/重试另记，不算应用故障，不绕过审核。

没有真实模型、独立holdout/Test、旧demo/真实账本访问、下载依赖/镜像、新基础设施、提交或推送。

## 复现与边界

```bash
# 项目根目录，专用测试库；不要与Worker/矩阵并行。
AFTERCARE_TEST_DATABASE_URL=postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55434/aftercare_test \
ALLOW_REAL_MODEL_CALLS=false .venv/bin/python -m pytest -m 'not live_model and not model_weights'

# 仅在现有任务已妥善处理且输出目录全新的情况下重跑真实探针；不清库。
DATABASE_URL=postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55434/aftercare_test \
ALLOW_REAL_MODEL_CALLS=false PYTHONPATH=src:. .venv/bin/python scripts/backend_d3_reliability_smoke.py \
--output-dir runtime/backend-D3/new-reliability
```

正常Worker可显式加`--log-json`；不开delivery开关或数据库未ready仍按原保护拒绝。不要直接针对旧demo复制测试命令。当前真实探针业务已成功，可以通过只读接口查看，临时账号已禁用；不是保留可复用登录凭据的演示环境。

局限：一个本地共享SKU、每档仅3轮，不代表生产容量；查询排队是入口代理，资源和锁为有盲区的采样，物理PG统计/缓存和主机噪声未完全冻结。日志是有限best-effort观察，不保证采集/跨系统Trace/告警，也不替代执行权。五个进程故障点是受控工程验收，不覆盖任意时刻、任意网络故障。

**D3到此停止。CI/Docker与后续工程化等待新的阶段授权。**
