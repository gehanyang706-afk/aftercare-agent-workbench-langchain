# LangChain 轻量 Agent：迁移与验收交接

2026-10-26。用户指定“GitHub原始基线重建→轻量迁移→后续选择性回迁”，本轮完成基础迁移并停止。真实云端/Ollama/模型用户/裁判调用均为0，未运行独立答案或新的性能实验，未推送。

## 版本与目录

- 精确起点：`5b435ead0c3b9f998c89d9d31f510ec29ad77884`。该基线已经有九工具、业务服务、工作台和安全闭环；Deep Agents只在创建入口使用，并通过HarnessProfile关闭通用能力，没有后来的D1/CE/S1/S2/S3实现。
- 新分支：`refactor/langchain-lite`。
- 新目录：`/Users/gehanyang/编程/aftercare-agent-workbench-langchain`。
- 原工作树：`/Users/gehanyang/编程/aftercare-agent-workbench`，仍为5b435ea，用户已有文档修改保留。
- 原实验树：`/Users/gehanyang/编程/aftercare-agent-workbench-exp`，仍为dcb4348，全部未提交工作保留。

最终本地提交和源文件hash见本交接同目录的 `langchain-lite-freeze.json`；源提交后仅追加交接元数据，不改变产品指纹。

## 保护与隔离

迁移前私有备份：`/Users/gehanyang/编程/aftercare-migration-backups/20261026-langchain-lite`，目录权限0700。包含两树的工作文件tar、staged/unstaged二进制patch、状态与HEAD、源文件hash、完整runtime的APFS克隆、Git bundle，以及旧55432持久库custom dump。不复制或公开密钥内容；若原树存在.env，它只在私有备份中。

最终复核：原工作树349个工作文件、22,474个runtime文件全部一致；实验树535个工作文件、2,343个runtime文件全部一致。两树HEAD和git status均与备份前一致。旧持久库再次只读导出，恢复为数据文本后剔除pg_dump控制随机串，前后数据SHA256完全相同。没有reset、stash、覆盖用户修改、清账、解暂停或迁移旧checkpoint。

迁移独占新容器 `aftercare-langchain-db`、新卷 `aftercare-langchain-pgdata`、宿主55434，仅用本地已有pgvector/PG17镜像，不下载。`aftercare_test`用于离线测试，`aftercare_demo`用于实际HTTP/Worker演示。原55432和55433未作为迁移写入目标。

本分支Compose另采用独立项目/镜像及默认55435/8130，避免照抄命令影响旧环境。源码Settings/.env.example默认55435，测试默认55434；临时HTTP smoke用8131，已停止。Docker新镜像本轮未构建。

## 主要改动

1. `agent/runtime.py`：`create_deep_agent`替换为LangChain `create_agent`；移除Deep Agents注册、HarnessProfile、子Agent/通用工具排除配置。
2. 保留原 `BusinessBoundary`，显式添加 `HumanInTheLoopMiddleware(interrupt_on=commit_action, interrupt_mode="batched")`。下层LangGraph/PostgresSaver不换版本。
3. `pyproject.toml`显式依赖langchain1.4.6；锁定包105→89，移除Deep Agents及15个专用附带包。保留包版本和原hash不变，独立环境卸载这些包后pip check通过。metadata驱动的保守锁裁剪脚本和离线编译失败日志均保留，不虚称联网重解或Linux安装验收。
4. doctor报告框架版本由Deep Agents改为LangChain；启动无网络测试只替换被导入框架，原断言不变。
5. 新增4项迁移验收：不安装Deep Agents也可导入；同一用户请求下两种相反合法工具顺序；原生batched中断协议与可信确认后唯一提交。
6. 独立环境/Compose端口隔离、当前运行说明和迁移文档。

业务Prompt、原BusinessBoundary、九工具及schema、资格/数量证据/确认/事务/回执服务、API、Worker、原场景、评分器、原业务断言均保持基线内容。没有迁入D1/CE/S1/S2/S3，也没有通过修改fixed-workflow替代Agent。

详细数据流与功能差异：[新架构](../design/langchain-lite-architecture.md)。

## 实际验收

| 验收 | 结果 | 边界 |
|---|---|---|
| 首轮核心兼容 | 71 passed，13.19秒 | 原Agent/HITL/确认续跑/意图/提交后停止测试 |
| 最终离线套件 | 272 passed，4 skipped，1 deselected，29.42秒 | 4项模块跳过因独立tau2环境未安装；排除1项ONNX权重推理；核心业务无跳过 |
| 不含Deep Agents的独立环境 | find_spec(deepagents)=None，pip check通过 | 真实卸载，并非只删import |
| 自主工具路径 | 目录→订单、订单→目录两种模型计划均实际执行 | 使用ScriptedModel验证执行层，不代表LLM能力提升 |
| 原生HITL | action_requests/review_configs/approve/reject保持 | 中断前无操作，可信API批准后唯一操作 |
| 真实本机HTTP＋CLI Worker | 取消S1001成功；重复确认1回执；SSE重放通过 | 临时8131服务＋Worker受控子进程，均脚本模型 |
| ruff/format/diff | 通过 | 只检查新改代码；未用新断言掩盖旧业务失败 |

测试具体覆盖：多轮数量证据/中文数量、确认前改口与旧预览失效、拒绝、伪造框架批准被业务层拒绝、重复/并发确认、图结束后的应用续跑、checkpoint重建、提交后丢响应与同回执恢复、提交前回滚、提交后预算/协议停止仍依原回执告知、API身份/CSRF、Worker租约与取消、SSE断点。

日志在 `runtime/langchain-migration/`：`core-contracts.log`、`final-offline-suite.log/xml`、`http-smoke.json`、`pip-check.txt`、`protection-proof.json`。最初未指定新库的沙箱测试只得到1 passed/27 skipped，不作为验收；正式两次均显式指定55434。没有因为依赖尚未就绪把跳过项算通过。

## 后续优化资产取舍（本轮不迁入）

| 资产 | 决策 | 依据与后续条件 |
|---|---|---|
| 原业务数量证据、可信批准、事务幂等、回执、预算/恢复 | 保留，已在基线 | 属于业务正确性底座，与Agent框架解耦 |
| S3-D库存语义契约 | 优先评估回迁；当前默认不加入 | 真实开发full1/3、S3-D2/3，两次无效库存查询能恢复；但总Token+335.45%、首次错误数未减。其价值是区分INVALID_QUERY和缺货，须单独质量/成本对照 |
| S3-C显式可写case Schema | 暂缓性能推广，可作为接口维护候选 | 框架无关且减少契约歧义，但云端筛选四组均1/3，尚无整体收益；不能与S3-D同时默认叠加归因 |
| S2字段错误反馈 | 暂缓 | 反馈本身增加输入，尚未证明避免重复错误；需区分缺用户事实与可修参数，避免把所有requires_user_input=false理解为事实齐全 |
| S1确定性回执收尾 | 保留可选设计，暂缓默认 | 单一回执后模型调用1→0有机制证据，但共同成功整题Token+30.02%，不能等同总体降耗；需多义务及恢复反例 |
| D1/harness_v2、CE保守历史投影 | 淘汰为当前默认迁移候选，历史资产封存 | 旧路径成本增加、安全精确覆盖未命中，无质量收益证据；不因框架迁移重新叠入 |
| CE-prefix固定指令压缩 | 淘汰为当前默认候选，保留负结果 | 离线字节下降，真实2/4对2/4但Token+19.27%、请求+27.78%；不据短Prompt直接推广 |
| Retail工具可见性 | 暂缓、保持公开评测独立路线 | 单train配对0→1且Agent Token+28.50%，不能直接变成产品九工具迁移机制 |
| Subagent/Skills/Planning/MCP | 不作为本轮必要缺口 | 尚无当前任务收益证据；基础迁移验收后停止新增机制 |

这些结论读取历史首次报告得出，未修改旧评分、报告、冻结hash或失败轨迹。求助报告仍是求助材料，不被当作真实调用批准。特别注意最新S3-D轨迹中存在“模型有数量问句但同时带坏工具、未交还用户轮次”的现象，后续若研究应按实际用户能否回答评价，不能仅匹配文本问号。

## 后续性能方案与当前停止点

参见 [同模型对照方案](../benchmarks/langchain-lite-performance-plan.md)。先比较精确Deep Agents基线与本迁移版本；同Prompt、工具、用户、评分、模型、相等逐题上限，各自新库/会话/Checkpoint。真实provider usage才可用于Token结论，包含失败题成本；不复用旧baseline结果，不把脚本调用次数当LLM质量。四题只是Pilot。

真实模型适配、当前额度、计划源码hash和批准需要另行准备。本轮不创建新真实批准、不执行历史入口、不回迁优化、不启动同模型实验。迁移目标已达到：一个独立、可运行、通过核心兼容验收的轻量Agent；没有宣称性能提升。

## 运行与复现

本轮已启动的迁移测试DB是55434；源码演示可以使用其独立demo库：

```bash
cd /Users/gehanyang/编程/aftercare-agent-workbench-langchain
export DATABASE_URL=postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55434/aftercare_demo
export ALLOW_REAL_MODEL_CALLS=false
export PYTHONPATH=src:.
.venv/bin/python -m aftercare doctor --offline
.venv/bin/python -m aftercare serve --offline --port 8131
# 另一终端设置相同环境后：
.venv/bin/python -m aftercare worker --provider scripted
```

离线验收（清空的仅是新55434/aftercare_test，不与其它测试同时运行）：

```bash
AFTERCARE_TEST_DATABASE_URL=postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55434/aftercare_test ALLOW_REAL_MODEL_CALLS=false PYTHONPATH=src:. .venv/bin/python -m pytest -m 'not live_model and not model_weights'
```

全新部署使用本分支Compose，默认55435/8130及新项目/数据卷；需另行构建本分支镜像，不能直接复用旧Deep Agents镜像来宣称轻量运行。本轮不启动新Compose app，当前旧8128工作台保持原版本。
