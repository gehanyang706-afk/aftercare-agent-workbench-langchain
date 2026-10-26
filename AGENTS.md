# 售后 Agent 工作台执行约束

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
