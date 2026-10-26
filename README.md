# 电商售后多轮决策与执行 Agent 工作台

> **当前分支：LangChain 轻量迁移版。** 从 `5b435ea` 独立建立，使用 `langchain.agents.create_agent()` + 原生 HITL + LangGraph/PostgreSQL Checkpoint，产品依赖已移除 Deep Agents。保留原九工具、业务规则、可信确认与事务回执；下文旧实验数据均为历史记录，不是本分支性能成绩。
>
> 当前运行与验收请先看 [轻量迁移交接](docs/handoffs/langchain-lite-migration.md)。本分支 Compose 使用独立项目/镜像及默认端口 55435、8130；迁移专用测试容器位于 55434。不要照抄历史命令连接 55432 的真实预算库。

> 当前进入收尾候选，停止新增业务功能和提示词迭代。最后固定六题真实验收尚待批准；本轮0真实请求。离线工作台8128已更新，237项测试通过；六题脚本预演两模式各5/6，保留“大一号”替身解析失败。
>
> [最后验收与预算](docs/closing/final-acceptance.md) · [启动说明](docs/operations.md) · [演示脚本](docs/closing/demo-script.md) · [结果总表](docs/closing/results-summary.md) · [面试交接](docs/closing/interview-handoff.md) · [事实来源核对](docs/closing/fact-sources.md)


已完成阶段 0—5：可运行三栏工作台、单主 Agent 工具循环、可信确认、真实 PostgreSQL 原子提交、幂等恢复、政策检索与任务队列。
普通界面默认是明确标记的**脚本模拟模式**；专用入口的真实开发回归结果按版本单列，不代表生产成功率。

![工作台实际浏览器截图](docs/screenshots/workbench-phase05.jpg)

## 运行

```bash
cd /Users/gehanyang/编程/aftercare-agent-workbench-langchain
export DATABASE_URL=postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55435/aftercare_demo
docker compose up -d db
.venv/bin/python -m aftercare db migrate
.venv/bin/python -m aftercare demo seed --database demo
.venv/bin/python -m aftercare serve --offline
# 第二终端
.venv/bin/python -m aftercare worker --provider scripted
```

本地源码服务默认 http://127.0.0.1:8000；本分支 Compose app 使用 http://127.0.0.1:8130。原工作台8128属于旧版本。
可体验“S1002 换大一码”、确认前改口、刷新/重连。取消只记录模拟退款请求，退换仅提交申请，不声称到账或寄出。

[操作说明](docs/operations.md)提供 Docker、重置、故障演示、依赖重建与验证命令。
[业务契约](docs/business-contract.md)、[架构](docs/architecture.md)、[决策](docs/decisions.md)、
[资源清单](docs/resources.md)、[阶段交接](docs/handoffs/)保留实现与验证边界。

阶段 0—5 禁止真实模型调用；阶段 6 必须另行批准精确模型、场景、请求数、Token 上限与停止条件。
阶段 5 已交付 60 条开发场景、100 条独立验收场景的公开 manifest、独立评分器和冻结清单。
独立答案未由实现角色读取或运行；开发脚本通过不代表真实模型成功率。

```bash
.venv/bin/python -m aftercare eval validate --split dev
.venv/bin/python -m aftercare eval run --split dev --provider scripted
.venv/bin/python -m aftercare eval verify-grader
.venv/bin/python -m aftercare eval plan --batch compatibility-smoke --dry-run
```

[评测说明](evaluation/README.md) · [开发结果](docs/experiments/development-final.md) ·
[阶段 5 交接](docs/handoffs/phase-05.md) · [真实试跑待批计划](evaluation/manifests/compatibility-smoke.json)


阶段 6A compatibility-smoke 已完成：精确模型 qwen3.5-plus-2026-02-15，6/6 协议检查通过，
6 次真实请求、0 重试，输入 1,001 / 输出 101 Token，未知预留 0，耗时 13.933 秒。
这不等于真实售后 Agent 评测；当前工作台仍为脚本模拟。
[实际兼容报告](docs/experiments/compatibility-smoke.md) ·
[6A 交接](docs/handoffs/phase-06A-compatibility-smoke.md) ·
[下一批待批计划](evaluation/manifests/development-pilot.json)。


阶段 6A 首轮 development-pilot 已结案，**0/4 任务完成**；24 次真实请求，输入 48,040 / 输出 1,831，
未知预留 0。没有实际业务操作；一次工具顺序错误，三题涉及数量歧义与脚本用户缺口。
改口及丢响应恢复未触发，不声称真实闭环通过。首轮证据不重写；后续真实批次按版本单列。
[首轮结果与归因](docs/experiments/development-pilot-first.md) ·
[交接记录](docs/handoffs/phase-06A-development-pilot.md) ·
[新回归草案，未批准](evaluation/manifests/development-pilot-regression-01.json)。


此前真实开发回归 business-loop-regression-v3：**2/4**，19 请求、输入 35,392 / 输出 1,141 Token，未知预留 0。
取消及事务后丢响应恢复通过；换码/改口停在重复澄清，改口未覆盖。
第二批数量澄清因门禁失败未运行。历史 0/4、v2 2/4 均保留，不计算跨版本提升。
批次结束后仅完成通用提示词候选和旁路诊断调整；离线两种模式各六题通过不代表真实失败已解决。
[本批逐题结果、预算与新待批计划](docs/experiments/business-loop-regression-v3-first.md) ·
[交接](docs/handoffs/phase-06A-business-loop-regression-v3.md)。
普通工作台真实入口保持关闭；本轮源码候选未更新到 8128 旧离线镜像。


最新同版本互补验证：**业务闭环 v4 4/4；数量组 v3 的 q1-A 失败、q1-B 因预算停止未运行**。
合计39请求、输入79,324 / 输出2,703 Token、未知预留0。改口和事务后丢响应实际触发；丢响应走图结束后的应用续跑。
q1-A猜数量的预览被拦；用户明确1件后实际提交，但提交后模型预算拒绝造成最终告知缺失。
原始结果保持不变；后续离线修复保留预算停止信号，并从原回执完成告知，不补发模型请求。
[逐题结果、归因与八题多样表达待批计划](docs/experiments/complementary-regressions-v1.md)。
新八题仅手工准备和静态校验，尚未授权或执行；普通工作台仍默认离线。


数量优先 quantity-priority-development-v1 已按批准执行：冻结评分3/4，数量过程干净3/4；40请求、85,600 Token、未知预留0。03换1件、04退2件均有可信回执告知；07脚本词面失配使等待分支未覆盖；08先猜量被拦后澄清，尊重无提交但暂停后重复提议/追问。
批后仅离线修复明确暂停的应用收尾；候选script-user-v3未接入冻结评测。真实调用已停止，不重跑、不扩批；普通工作台仍默认离线，8128仍是旧镜像。
[本批逐题结果与归因](docs/experiments/quantity-priority-development-v1.md) ·
[批后修复交接](docs/handoffs/phase-06A-quantity-priority-first.md) ·
[首次轨迹封存](evaluation/manifests/quantity-priority-development-v1-first-run.json)。
