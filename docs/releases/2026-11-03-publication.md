# 2026-11-03 首次公开提交

本次将 LangChain 售后与可靠换货履约系统发布到独立仓库 `gehanyang706-afk/aftercare-agent-workbench-langchain`，保留原 Deep Agents 仓库与全部既有历史。发布范围为 A—D4，schema 为 `0011_dispatch_intent`。

## 实际验证状态

- 发布准备时，D4 冻结的 198 项源码逐项 SHA256 与当前产品一致；本轮没有修改业务规则、Worker 执行权或仓储协议。
- 2026-10-30 历史 D4 回归为 934 passed / 4 skipped / 1 deselected；原报告和证据指纹不改写。
- 2026-11-03 首轮发布复验为 **925 passed / 9 failed / 4 skipped / 1 deselected，129.12 秒**。失败集中在 Agent/Worker 确认续跑及 pilot 预检，不能称为本轮全绿。固定日期夹具与当前真实时钟、环境变量隔离正在排查，修复结论须以新复验为准。
- 独立 JavaScript 队列测试 9 passed；Ruff、格式、pip check、git diff --check 通过。
- 4 项跳过仍为未安装的可选 tau2 依赖，1 项排除仍为模型权重推理。真实模型请求为 0，没有执行独立 holdout/Test。
- GitHub Actions 会在提交后运行，实际结果以仓库 Actions 记录为准；本文不预称云 CI 通过。

## 发布内容与本地保留

提交代码、迁移、测试、配置、当前 README 和已有 A—D4 报告。5 份未完成 D5 草稿完整保存在本地忽略的 publication 检查点，没有删除，也没有进入产品和测试路径。README 旧版另行备份，历史源码仍由 Git 保留。

`.env`、实际随机仓储密钥、runtime、SQL/SQLite 备份、运行日志、模型权重和独立答案不进入提交。公开前扫描当前候选文件与 HEAD 可达历史；真实密钥比对和常见密钥模式未发现命中。第三方许可及来源记录保持。

原始复验日志、JUnit、测试前专用数据库备份和扫描记录只保存在本地 `runtime/publication-20261103-154231/`。聊天中示例性的 200 场景、成功率和 Token 收益没有作为真实项目成果发布。

用户要求先提交 GitHub，因此在明确披露复验失败的前提下先公开当前 D4 实现；测试问题不通过删断言、扩大售后窗口或隐藏失败处理。
