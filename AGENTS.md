# 售后 Agent 工作台执行约束

## 当前工作入口：后端升级 A→B1→B2→C→D（2026-10-27）

用户将后端升级路线收敛为 A 基线与架构审计 → B1 领域模型与业务服务 → B2 身份权限、正式 API 与最小界面 → C 可靠异步执行 → D 性能与工程化，首轮单商户；多租户为可选后续阶段。方案和逐批验收见 docs/design/backend-fulfillment-roadmap.md。B1 先交付领域模型、迁移、版本、库存、幂等命令及事务测试，B2 再接员工认证/session、正式路由和前端，不能混成一次开发。
当前已完成 A 与用户批准的 B1。B1 交接 docs/handoffs/backend-B1.md、契约 docs/design/backend-B1-domain-contract.md、验证 docs/benchmarks/reports/backend-B1-validation.json：完整离线回归332 passed/4 skipped/1 deselected，其中新增57项领域+3项迁移测试；0007仅在55434/aftercare_test验收，旧demo/容器未迁移。用户随后授权按顺序继续，B2员工/session、正式API与最小界面已实现，完整离线回归391 passed/4 skipped/1 deselected，新增37项HTTP+22项认证/迁移测试；实际TCP/CLI三角色换货及6次精确重放通过，浏览器三角色链及桌面/窄屏截图验收已于 2026-10-28 通过。B2交接docs/handoffs/backend-B2.md、契约docs/design/backend-B2-auth-api-contract.md、验证docs/benchmarks/reports/backend-B2-validation.json。0008仅在55434/aftercare_test验收；两个演示开关默认关闭。截至B2冻结时C、D尚未实施；浏览器验收范围是一条本地三角色链及两种宽度，不是完整兼容性/无障碍审计。原A/B1交接、报告和首次证据保持历史口径，不覆盖runtime/backend-A、runtime/backend-B1、runtime/backend-B2首次记录。
新增履约命令必须复用 FulfillmentService，不绕过原可信申请提交，不在API复制状态机/库存/幂等。Actor只是可信调用契约，B2已从服务端session构造；业务提交前重验身份并持有账号/session共享锁，B1绑定同一连接SAVEPOINT，外层统一commit；员工source字符串不是凭证。旧settle_application仅demo/test兼容，不向API/Agent暴露。B2按冻结契约接入，不能把B1/B2进程内模拟适配器当作C可靠HTTP仓储。
用户于2026-10-28明确授权先做C1：事务Outbox+独立模拟仓储；C1已通过完整532 passed/4 skipped/1 deselected（新增141）和5个真实进程/HTTP受控场景。见docs/handoffs/backend-C1.md、docs/design/backend-C1-outbox-contract.md、docs/benchmarks/reports/backend-C1-validation.json。0009只在55434/test验收；独立仓储SQLite位于runtime，不新增容器/依赖。warehouse_delivery_enabled默认false，和旧simulate互斥；已有ShipmentRequest不可切回模拟确认。Worker短事务claim/check/finish，事务外HTTP；结果写回与原领域命令同连接SAVEPOINT，owner/generation/期限复核。超时/过期CLAIMED隔离UNKNOWN/BLOCKED并保留库存，只有provider明确未受理才有界重试；截至C1冻结时，C2的Inbox/回调/自动查单合并/reconcile/redrive尚未实现，不得把C1安全暂停说成恢复完成。独立HTTP尝试子进程默认总预算2秒+最多1秒回收；只允许literal loopback，无代理/重定向，原Agent Worker保持不变。C1首次证据runtime/backend-C1冻结；不得清除其中UNKNOWN库存预留以伪造完成。
用户随后批准C2：当前已完成0010增量迁移、独立HMAC回调/Inbox、可信GET合并、有界查询任务与接管、support reconcile/redrive。交接docs/handoffs/backend-C2.md、契约docs/design/backend-C2-recovery-contract.md、验证docs/benchmarks/reports/backend-C2-validation.json。先真实恢复C1两条UNKNOWN并归档before/after，再运行重置专用测试库的fixture；原C1冻结snapshot哈希未变。最终完整673 passed/4 skipped/1 deselected（本批新增141），最终8项真实进程场景通过；first91pass1测试NULL字段错误、修复及收尾调度饥饿修复证据均保留，不回写first。新的查询候选只取未成功且尚无Task的请求，已有PENDING/CLAIMED/DONE/EXHAUSTED任务不重开预算；已有DONE任务经redrive后再UNKNOWN需support显式reconcile。回调只按effect_key归属锁Inbox，先持久接收再处理，同ID异内容留冲突；源/签名/时间窗失败不写Inbox。merge_success复用B1同连接SAVEPOINT，回调/查询/原响应共用结果入口，原receipt和triggered_by不覆盖。默认回调关闭，HMAC与Bearer密钥独立；UNKNOWN/模糊耗尽禁止直接redrive，仅明确未受理死信可有界重投。当前8场景保留1个MANUAL_REVIEW/DEAD和1份库存预留、未知键Inbox及冲突证据，这是验证的停止分支，不能为成绩清成成功。C2只本机进程，D性能/Trace/CI/容器尚未实施，不擅自运行真实模型或旧待批计划。
后续性能优化必须先保留可比基线。snapshot 的外部只读路径和内部 Agent/Worker 路径分开评估，不得全局删除 _conversation 行锁或 run_guard；未知外部履约结果先查单，不直接重试副作用或释放库存。原可信确认、幂等回执、数量证据及真实调用保护继续有效。
下文“基础验收后停止”“停止新增功能”等描述保留此前批次范围；当前后端演进范围以本段、用户最新指令和上述 A→D 方案为准，不沿用历史真实模型批准。

2026-10-29继续C可靠性补强已完成：0011_dispatch_intent、新NOT_STARTED持久发送许可、无接受事实时有限重排、已接受门禁、ACK事件绑定及SQLite锁后时钟。交接docs/handoffs/backend-C-hardening.md、契约docs/design/backend-C-dispatch-contract.md、验证docs/benchmarks/reports/backend-C-hardening-validation.json。最终707 passed/4 skipped/1 deselected，新增34项；五项真实网络/进程验证5/5通过27.699秒，含真实上游200后下游0字节断链、claim后未许可SIGKILL、安全接管/旧sender失权、许可后socket前SIGKILL后三次404保守停止、真实ACK后sender SIGKILL及自然租约重发。新worker必须begin_delivery提交许可后才HTTP，NOT_STARTED不能直接finish；STARTED不是socket已发送证明。旧行/旧writer默认LEGACY_UNKNOWN，不按404重投；接受回调先到也必须拒发转查单。C1实际发送探针已接许可，旧测试只补协议前提不弱化断言。原C1/C2报告/快照保持冻结，新矩阵保留1 MANUAL/v5/RESERVED，库存20=15可用+1预留+4消费；provider4事实8事件，6个未启用循环的回调PENDING保留。当前schema只在55434/test到0011，旧demo未迁移；当前库和证据不得为成绩强行清成功。D尚未实施。

2026-10-29用户正式结束C并授权D1，现D1已完成后停止，不自动进D2。见docs/handoffs/backend-D1.md、docs/design/backend-D1-query-contract.md、docs/benchmarks/reports/backend-D1-performance.md与backend-D1-validation.json。同一固定pair/0011/PG17.11/3预热30采样ASGI顺序对照：100line103→4SQL、p95 36.456→9.312ms；100preview209→10、58.666→9.640ms，所有完整默认JSON/字节相同。首次before后autoanalyze有变，保留first并用冻结C源补测before-stable；四份pg_stats相同，comparison统计标记true仅demo_sessions死元组1→3，不改成false。新messages按revision/time/id分页，events显式limit按to_version，默认全量合同/前端不变，applications保留原keyset。消息仍SeqScan+Sort、全1000页遍历20请求99SQL/80.124ms更慢，不夸大全量或UI提速；无新增索引/迁移，仍0011。专项96pass，完整761pass/4skip/1deselect（新增54），库存/数量违规0。业务snapshot仅previews helper变化，事务/_conversation FOR UPDATE/run_guard/task_state结构原样；SSE、领域写、Outbox/Inbox/UNKNOWN/Worker代码未动。性能fixture清理恢复原C业务行后才pytest；当前test库是工程fixture，C原矩阵已归档，勿伪称仍为C live结果。旧A/B/C证据不覆盖，无云模型/旧demo清库/新依赖容器/推送。D2等待新授权。

用户随后批准下一步，2026-10-29 D2已完成后停止；D1原停止段保留历史口径。当前入口docs/handoffs/backend-D2.md、docs/design/backend-D2-read-stream-contract.md、docs/benchmarks/reports/backend-D2-performance.md与backend-D2-validation.json。外部snapshot新ReadSnapshotService同RR/READ ONLY读完整字段/jobs/receipts；BusinessService整文件及Worker/写命令保持D1原样，旧load_events/trace不变。SSE初始to_thread短批次、每批session+owner重验/100条、满批即排空、空闲.5/1/2、stream_closed无id终止；前端epoch/abort/单飞+尾随刷新、60ms合并与帧级渲染。真实写锁下外部GET/SSE500→200，内部仍55P03；三份pg_stats一致，snapshot/trace静态语义不变。1/4连接3.2s组SQL21/84→10/40（含启动非QPS），1000事件5139→1441ms但27→34SQL；snapshot多1 SET且部分p95变慢。真实浏览器200事件窗口页面GET1、切会话不串、会话失效显式close后观察2.5s无重连；Node假网络200→2不能当TTI。专项74pass，full804pass/4skip/1deselect（新增Python43=26snapshot+17SSE），独立JS9pass。事件ID前缀只在当前持conv锁writer+sequenceCACHE1且无手工ID/setval前提下保证，性能高IDfixture仅只读；慢流已载批次可能延后身份检测，2s非SLA，to_thread不能瞬间杀查询、100条非字节限。未新增迁移/索引/依赖/容器/云模型/旧库访问/推送，当前库pytest余留且库存/数量违规0、临时8136/37关闭。多Worker/Trace/CI与其他D项未启动。

2026-10-30用户批准D3后已完成并停止，以上D2停止段是历史记录。当前入口docs/handoffs/backend-D3.md、docs/design/backend-D3-worker-contract.md、docs/benchmarks/reports/backend-D3-performance.md与backend-D3-validation.json。新增默认关闭的--log-json，白名单关联application/effect/shipment_request/outbox或task/owner/generation及真实attempt/cycle；stdout不变，有界异步stderr，日志可丢tail不能当提交凭证。只改CLI+两履约Worker和新增观察模块，所有领域/恢复services、仓储协议、内部snapshot/run_guard、SSE/前端保持D2字节一致；schema仍0011。前后各18组216任务的真实1/2/4进程矩阵全部通过，SQL每组Outbox1068/Query696不变；after日志2592条，完整离线842pass/4skip/1deselect（新增31unit+7并发），独立Node9pass。5/5真实抢单/SIGKILL/自然过期/旧sender失权验收19.856秒通过，5provider事实、库存20=15可用+0预留+5消费；临时员工禁用、8150/51/52及登记进程组关闭。吞吐变化-0.865%～+0.368%，Query4 HTTP p95 179.917→216.622ms，负结果保留；固定12任务末轮不计sleep，不能宣称线性扩容、生产QPS或日志零开销。性能fixture精确ID清理恢复全原行hash后才pytest，最终库保留D3五案，不伪称C live矩阵。旧证据/快照不覆盖，派生摘要舍入错误和工具PATH/审批问题另存；未新增依赖/容器/迁移/索引/云模型/旧库访问/推送。D3结束，CI/Docker及后续D项等待新授权。

2026-10-30用户继续原路线，D4离线CI/Docker本地工程验收已完成后停止。入口docs/handoffs/backend-D4.md、docs/operations-backend.md、docs/design/backend-D4-delivery-contract.md、docs/benchmarks/reports/backend-D4-acceptance.md与backend-D4-validation.json。新增固定输入Actions工作流及精确55434/preflight/JUnit18模块门禁；GitHub云job和默认干净下载构建未执行，不宣称云CI绿。独立compose.backend.yaml使用新aftercare-d4-test项目/55436 test库/8138及3卷，旧Compose/旧demo不动；app共享network namespace保持原literal loopback，PG Unix socket，非root/只读/资源和日志有界。已用缓存镜像无网络构建：87锁分发+project、16旧额外SDK移除、133源码/资产文件全匹配；metadata不等wheel hash复验，仅ARM64实测。原D3源码仅migrations/env.py增加可信外部Connection/私有版本表支持，其余183项保持，领域/Worker/恢复/前端字节不变；schema仍0011。最终934pass/4skip/1deselect（新增92），Node9，CI审计18模块；首轮924pass和实际prepare连接失败均保留。宿主DNAT不能访问容器loopback，验收仅改固定docker exec provider transport，凭据stdin，无代理/重定向/任意地址；继续同一首失败申请未另造。原请求PENDING和成功态两次整组down/up未删卷、完整快照一致；warehouse重启/同键POST仍1事实，库存20=19可用+0预留+1消费；2回调DELIVERED、Inbox均STALE不再推进。私有0006→0010→0011保留原字段/模拟UNKNOWN预留99及LEGACY_UNKNOWN，公开model_calls0。DB停机live200/ready503、恢复200；可选原scripted Agent doctor就绪、jobs0，无模型调用。Docker cp tmpfs/归档器可选字段问题及修复另存。最终D4容器网络关闭，3卷/镜像保留，临时员工禁用，旧容器ID/状态不变。原测试55434在pytest前已备份；旧A/B/C/D1/D2/D3证据不覆盖，runtime/env/SQL备份不入Git。共享namespace重建app须整组协调，非生产HA/滚动升级/跨平台或性能收益结论。未下载大型镜像/新依赖、未推送/公开部署/旧账本访问；后续云CI首跑、发布、完整Trace另列范围。

## 当前分支优先说明：LangChain 轻量迁移

用户于2026-10-26指定从5b435ead0c3b9f998c89d9d31f510ec29ad77884独立重建，当前为refactor/langchain-lite。此段替代下面历史阶段的默认框架/环境/旧计划入口说明；历史成绩不回写。
产品创建入口为LangChain create_agent + HumanInTheLoopMiddleware(batched)，底层LangGraph/PostgresSaver。BusinessBoundary、九工具、Prompt、业务服务、可信确认、事务与回执沿用基线，不迁移D1/CE/S1/S2/S3优化，不使用fixed-workflow替代默认Agent。
本轮仅完成框架兼容与离线验收，不调用云端/Ollama/模型用户/裁判，不运行holdout/Test，不执行历史待批计划。后续同模型性能对照须独立冻结与批准；基础验收后停止。
测试显式使用127.0.0.1:55434/aftercare_test（新独立测试容器），真实55432和旧实验55433不得用来清库验收。Compose使用新项目/镜像及默认55435/8130，避免影响旧容器与数据卷。
当前交接docs/handoffs/langchain-lite-migration.md；迁移前私有备份位于同级aftercare-migration-backups/20261026-langchain-lite，禁止推送备份/runtime/密钥/账本。原实验树的未提交修改原样保留，不reset/stash/混合提交。

目标：本机单商户模拟订单的多轮查询、取消、退货、换货、转人工；不连接真实支付/物流。
技术栈：Python3.13、FastAPI、PostgreSQL/pgvector、Deep Agents/LangGraph、原生HTML/CSS/JS。
设计见docs/design四份文档、docs/business-contract.md、docs/architecture.md、docs/decisions.md。

## 当前状态与收尾范围

阶段0—5完成，真实已见开发结果分批保留，口径见docs/closing/results-summary.md。
原四类业务闭环v4为4/4；最新数量批次冻结3/4、数量过程3/4，不能称全面通过。
当前收尾候选：来源标识/中文邻接订单号/明确暂停及真实账本重置保护已离线验证，237 passed/0 skipped。
最后六题的两种脚本协议各5/6，fa1-03“大一号”为替身解析限制，保留失败不改题。
8128离线app/worker已更新；候选源码/镜像/证据见evaluation/manifests/closing-candidate-v1.json。
仅final-development-acceptance-v1六题计划待批准，264k Token/96请求/1800秒；没有新真实执行授权。
停止新增功能和提示词迭代。最后验收后除错误提交、授权绕过、重复副作用、虚假回执或预算安全异常，其他记录为已知限制，不自动追加真实回归。新真实修复批次仍须另批。

## 目录、运行与验证

src/aftercare为产品、服务、检索、预算和worker；data/configs/migrations为资产；tests/evaluation为验证。
runtime仅保存本机运行证据，不入Git；docs/closing是当前交付入口，详细启动见docs/operations.md。

```bash
# 项目根目录；已有本地镜像，无网络更新方法见操作说明
docker compose up -d db
AFTERCARE_PORT=8128 docker compose --profile app up -d --no-build --no-deps app worker
# 以下离线PG验证须串行，不与真实评测并行
AFTERCARE_TEST_DATABASE_URL=postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55432/aftercare_test ALLOW_REAL_MODEL_CALLS=false .venv/bin/python -m pytest -m 'not live_model'
ALLOW_REAL_MODEL_CALLS=false .venv/bin/python -m evaluation.evaluator.regression_offline --scenario-file evaluation/dev/final-acceptance-v1.jsonl --user-policy-version script-user-v3 --model-mode default
.venv/bin/ruff check .
.venv/bin/ruff format --check .
```

源码serve/worker与容器二选一，普通工作台默认ScriptedModel。唯一依赖锁requirements.lock，项目.venv隔离。
每阶段实现、验证、修复后再推进；保存交接中的功能、命令、结果、数据流、关键入口、限制及一个完整机制。
当前交接见docs/handoffs/phase-06A-closing-candidate.md。最终真实结果尚待批准验收，不伪称已完成。

## 权限与事实边界

- 默认禁止真实调用；.env/环境开关不代替授权。获批也须核对具体模型、地域、hash、源码、任务、请求/Token/时间/费用上限及当前免费余量/FreeTierOnly。
- 所有真实调用共用BudgetLedger、请求计数和截止；关闭隐式重试。未知usage保留预留并暂停；不换模型、扩大批次、调用模型用户/裁判，不借用旧预算。普通工作台不开放真实入口。
- aftercare_test可被验证清库；aftercare_demo含持久真实账本，禁止重置、删账本或删batch-state绕过。demo reset已拒绝含真实批次的库；不得down -v。
- 只改本项目，不覆盖未知文件或用户既有报告编辑。密钥不入Git/日志，不修改全局配置，不自行推送/公开部署。
- 不读取或运行独立验收答案/holdout，不硬编码案例、修改评分或回写历史成绩提高结果。开发场景不得称盲测。
- case是模型提议，消息锚点不等于语义证据；申请数量来自实际user消息，购买量/库存不能替代。后台独立复核身份、资格、版本、确认及事务幂等。
- 提交须预览→可信确认→同一业务服务。新消息失效旧预览；图结束不等于业务完成；最终告知依实际回执，停止码保留。明确暂停走共享runtime，不新增提交或模型调用。
- 普通离线实现、测试、修复自主完成；改变范围、增加费用或新真实调用须确认。全局软件、权重、大型镜像先列资源；已获具体授权不重复询问。

## 公开评测升级（2026-10-20）

本轮按 docs/benchmarks/implementation-v1 四份规格推进 U0→U1→U2 及环境就绪后的 U4。
授权离线实现、测试、修复与交接；不授权真实 Agent/用户/裁判调用、付费、推送或公开部署。
旧 final-development-acceptance-v1 本轮不执行，不与公开评测叠加。历史冻结结果及上述保护仍有效。
官方 tau2 v1.0.1 固定 commit；隔离源码、环境和依赖锁，不修改产品 requirements.lock。
仅官方 Orchestrator 执行官方工具；Agent 不接收 task、隐藏目标、参考动作、评分答案或完整环境。
开发只读 train；test 由可信准备/评测入口处理，不打印答案；项目独立 holdout 不读不运行。
所有未来真实角色共用持久总账及精确模型跨角色额度保护，未知 usage 保留预留并暂停，不沿用旧批准。
U4 仅 aftercare_test，真实进程故障须保存触发证据；不得以对象重建替代杀进程。
每阶段记录 docs/handoffs/upgrade-U*.md；缺依赖先集中资源清单，继续独立工作。
新命令在 benchmarks/tau_retail/README.md 中随验收更新；未验收明确标注。


### 升级离线交付状态

U0/U1/U2完成；U4冻结版本bde3c0f完成12组×20次，240/240通过（M=12），首次失败与修复证据保留。
公开摘要见docs/benchmarks/reports，命令见benchmarks/tau_retail/README.md。
U4后仅调整公开评测seed/必要裁判/旧provider暂停保护，产品可靠性源码未变；U3必须使用重新冻结的新计划hash。
校准train107/96实际包含NL_ASSERTION；107需裁判，96断言为空。裁判也须单独列精确模型和角色预算批准，不能删奖励或换题回避。
本轮真实请求0，真实账本156次/309920Token/unknown0未变。未执行六题旧计划、test或独立holdout。
当前demo/8128仍为0005旧离线候选；新增0006迁移仅在aftercare_test验收。真实校准前须完成保留账本的增量迁移和离线应用版本协调，未就绪预检拒绝；不得清库解决。


### U3首轮真实校准（2026-10-20）

用户已明确批准calibration-v1（含Agent/模型用户/必要裁判），本批已在4 HTTP/12233Token后协议硬停止，3个episode未运行、0个有效评分。
详细见docs/benchmarks/reports/U3-calibration-v1-first.md；原manifest/结果/trace保留，不重跑。
离线修复已按官方默认路由处理混合响应、保存原始响应、封闭运行状态；trace-v2，28项隔离契约通过，尚无修复后的真实结果。
修复指纹变化，旧批准已结束，不可续跑；未来新的真实计划仍须另批及新账户确认，不转移剩余预算。
0006/demo/8128已协调升级健康，普通app真实flag仍false；持久真实账本160请求/322153Token/unknown0。


### U3 修复后的待批计划

calibration-v2 已冻结，见 docs/handoffs/upgrade-U3-calibration-v2-pending.md。
29 项隔离契约通过；新账户快照只能经新计划批准更新，未知用量暂停不解除，旧批预算不转移。
旧 v1 24 个原始文件保持不变，真实账本仍 160 请求 / 322153 Token / unknown 0。
新批需要新 hash 批准及当前精确模型 ≥140万免费 Token、北京 FreeTierOnly 确认；尚未运行。


### U3a calibration-v2 已执行（历史待批段落保留）

用户新批准及当前账户确认后，本批仅运行一次，4 episode 完成、官方均通过；78 HTTP / 509626 Token / unknown 0，累计481.03秒。
报告 docs/benchmarks/reports/U3-calibration-v2-first.md；官方2/2、全历史2/2，全历史Agent多耗6.12%，不称优化收益。
两次自然语言输出截断保留，不改评分或自动重跑；v1原始结果和24文件hash未变。
普通工作台真实flag仍false；真实历史238请求/831779Token；项目预算可用999660Token，不擅自扩cap。
本次真实授权已结束；U3b、validation、Test、holdout等均未获新批执行授权。


### U3b 离线候选准备

见 docs/handoffs/upgrade-U3b-offline.md：词元原样 JSON 空白投影、51请求离线重放、39 benchmark契约+1产品共享投影测试通过。
整请求体字节仅减少1.13%，没有新真实Token收益，建议暂缓付费/免费额度实验，不为简历指标无限加功能。
tune-v1为待批8episode计划，322请求/260万Token/80分钟；项目现可用999660，缺1600340，未改总cap。
新真实批次、260万当前免费余量/FreeTierOnly和项目cap调整必须分别明确批准；不复用校准批准，不运行validation/Test/holdout。


### U3c 工具可见性离线范围（替代原tune-v1方向）

按 docs/handoffs/upgrade-U3c-tool-visibility-offline.md 实施；本阶段不是未见validation。
原tune-v1保留hash并暂缓，入口拒绝其执行。新候选tool_visibility_v1仅改Agent每步可见工具子集；完整官方实现/参数/政策/历史/输出保留，基线不动。
只能用实际已见订单结果及实体来源选择；多订单并集、查询/转人工常驻、历史工具保留、未知/同批状态顺序不确定全目录回退。可见性不是授权，官方Orchestrator唯一执行。
73项离线契约通过；51请求体字节减15.48%，已见调用30/30，均非真实效果证据；告知质量只诊断，不混改提示词。
新tool-visibility-v1计划仅train96×两配置，80请求/600kToken/1200秒，无judge/aux；未批准未执行，普通工作台仍离线，不读validation/Test/holdout。
项目cap保持2000000，当前可用999660；不复用旧批批准或旧余额，真实运行仍需新hash授权及当前精确模型免费余量/FreeTierOnly确认。


### U3c tool-visibility-v1 首次真实批次已结束

用户批准新hash并确认当前≥600k免费余量/FreeTierOnly后，只执行train96官方→候选各一次；见docs/benchmarks/reports/U3c-tool-visibility-v1-first.md。
基线reward0、候选1；均USER_STOP，基线漏商品修改。34HTTP/164590Token/unknown0，累计166.12秒。
候选Agent多耗28.50%，无相同成功路径节省结论；同历史schema切换仅证明请求体字节下降14.30%。fallback/未暴露提议/工具错误均0。
截断0；候选冗长及到账措辞风险只诊断、未改prompt/输出/评分。原历史结果保留，不为旧开发失败大修或重跑全量/U4。
真实历史272请求/996369Token，项目cap仍2000000，可用835070，普通工作台flag仍false。
本批授权结束，真实调用停止；不自动delivered/validation/Test，不借旧批余额、不追加新模型或实验。
