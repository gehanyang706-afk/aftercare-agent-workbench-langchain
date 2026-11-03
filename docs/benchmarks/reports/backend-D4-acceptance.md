# D4 CI 与容器交付验收

2026-10-30。本地工程验收完成，迁移仍0011；未推送或执行GitHub云流水线。没有新增业务、Agent工具、基础设施中间件、仓储协议或模型调用。

[交接](../../handoffs/backend-D4.md) · [运行指南](../../operations-backend.md) · [契约](../../design/backend-D4-delivery-contract.md) · [验证清单](backend-D4-validation.json)

## 环境与构建

Docker Desktop 4.93.0 / Engine29.8.1，Linux/arm64；Python3.13.14、PG17.11，宿主macOS。专用aftercare-d4-test项目、55436/aftercare_test、HTTP8138，与原55434测试容器及旧demo隔离。只使用已缓存镜像，无网络构建新tag。

缓存基础运行时的87项Linux锁版本全部匹配，16个旧SDK/Deep Agents额外分发在新镜像层移除；最终 **87个锁分发+当前项目=88**，缺失/冲突/锁外分发均0，pip check通过。镜像133个源码/资产文件的完整集合和hash与宿主当前树相同，无旧文件残留。metadata核对不等于重验原wheel hash。

Dockerfile默认固定Python+hash安装路径保留；本地实际执行显式reuse构建。干净下载构建、Linux/AMD64和GitHub云job未实际执行，不把本地ARM64结果说成多平台/云CI通过。PG固定OCI index的amd64/arm64成员已从本地索引确认；远端metadata请求超时原样保留，没有拉取镜像。

## 实际验收

|检查|实际结果|证据（runtime/backend-D4/）|
|---|---|---|
|新卷启动|原CLI迁0011，DB/app/warehouse健康，非rootSQLite owner10001|compose-bootstrap-first.log、compose-services-first.log|
|有数据增量迁移|私有schema原0006→0010→0011；所有旧列投影不变，旧attempt LEGACY_UNKNOWN，模拟UNKNOWN用量保留|legacy-seed-first/、legacy-prepare-first/、legacy-verify-first/|
|PENDING整组重建|原申请v5、原回执、库存19可用/1预留、0 attempt的完整快照一致|pending-recreate-comparison.json|
|消费者完成|原Outbox/query容器完成同一请求，v6、CONSUMED、历史6条，库存19/0/消费1|container-success-first/|
|独立仓储重启|同键POST返回原事实，物理shipment1、事件2，完整行不变|warehouse-restart-comparison.json|
|成功态整组重建|PG/SQLite卷保留，业务完整快照和provider行相同，原actor幂等重放一致|success-recreate-comparison.json|
|独立回调容器|2持久事件DELIVERED，Inbox序号1/2均STALE，未重复推进/消费|container-callbacks-first/、provider-final.*|
|迁移历史跨重建|旧字段/账本和LEGACY_UNKNOWN仍一致|legacy-verify-after-recreate/|
|数据库故障就绪|消费者先停；DB停止时live200/ready503，恢复同卷后ready200|readiness-database-down.json、readiness-restored.json|
|可选原scripted Agent|容器启动及doctor ready，public jobs0、模型调用0；不是Agent任务成绩|agent-doctor-first.json|
|安全与资源|UID10001、只读rootfs、capabilities/权限/CPU/内存/PID/日志限制实配；正常栈仓储无host端口|container-inspect-final.json、compose-config-redacted.json|

全程只有一条公开真实HTTP换货申请：`app_46f92f7b105c48a4a8baa79e234ee0f7`。首次工具连接失败后继续同一请求，没有重下单绕过失败。重复POST包括手动仓储幂等验收，不能将“1份事实”写成“全程只发送一次POST”。两次整组down/up均未删除卷，也未把单服务app重建当作滚动升级。

回调到达时业务已成功，所以两个Inbox均STALE是合法的认证、持久接收及去重停止结果，不能写成回调新推进了业务。初始仓储PENDING事件2已送达，最终物理出库仍1，库存20=19可用+0预留+1消费。

私有迁移历史是结构性SQL fixture，包含模拟UNKNOWN调用/预留99，未发生真实模型请求；公开model_calls仍0。公开Worker不查询私有schema，旧CLAIMED历史不会被当成本轮活动任务接管。保留原未知用量，不清成成功。

## 回归与门禁

|批次|结果|
|---|---|
|初始CI/依赖/迁移专项|64 passed / 0.27秒|
|首轮完整回归|924 passed / 4 skipped / 1 deselected / 119.23秒|
|验收transport修复后的纯unit|91 passed / 0.17秒|
|最终完整回归|**934 passed / 4 skipped / 1 deselected / 123.05秒**|
|JUnit强制覆盖审计|938个case、934通过，18个关键模块齐全；只有4个已知tau2 skip|
|独立前端队列|9 passed|

新增92项=CI34+锁依赖29+验收工具28+迁移连接1；842+92=934。专项和Node不再叠加到完整Python计数。4个skip仍是可选tau2缺依赖，1个排除仍为模型权重推理。Ruff/format/pip check/diff、YAML/Bash静态检查通过。

GitHub配置固定4个Actions完整SHA、Python/Node、PG OCI index；先精确目标库→迁移→ready/空账本门禁，再完整回归和JUnit审计、Node、默认镜像构建/锁校验，always保存失败日志。云端未执行，不提供绿色检查或云耗时结论。

## 首次失败与修复

1. **真实prepare工具失败**：四个HTTP命令和重放都已成功，provider_lookup因宿主DNAT无法访问容器loopback而RemoteProtocolError。app/warehouse健康、OOM=false，最小探针证实host8138=200、host8153中断、容器内8152=200。仅修验收transport：固定docker exec、stdin传凭据/正文、无代理/重定向、有界超时，保留原HTTP状态/正文；产品bind/guard不动。红阶段10fail/18pass，绿阶段28pass；从首失败events恢复同一批准请求的state后验收通过。原失败/源码before/红绿记录均保留。
2. **Docker archive工具**无法读取tmpfs内已创建且integrity=ok的SQLite backup；保留诊断，使用exec文件流导出同一backup。重启前JSON与重启后所有provider行完全相同，不伪造不存在的首份二进制导出。
3. **归档器**误认为DB的可选HostConfig.Init必存在，KeyError；未改产品，改为可选字段读取后重新归档，之前输出保留。后续再次关栈，卷和业务事实保留。
4. **脱敏检查**发现Compose扩展节点中的环境变量被初版过滤器漏掉。交付前改为递归过滤所有节点，移除草稿中的明文测试密钥，复扫runtime仅权限600的本地env含这两个随机密钥；含密钥的旧配置不保留、不打印或发送。

静态格式、远端元数据超时等准备记录也保留，不记成应用通过或隐藏失败。最终没有未解决的业务/测试失败。

## 收尾与限制

原D3的184源码中只修改迁移环境入口，183项保持；Dockerfile及本批新增文件另列完整指纹。所有领域/恢复/Worker/前端字节不变，A/B/C/D1/D2/D3的原始报告与证据按hash核验，不回写历史成绩。

临时员工已禁用，本轮容器/网络关闭，保留三个命名卷、镜像及SQL/SQLite一致备份；旧容器未启动/停止，原55434测试库先备份后才运行现有pytest fixture。配置中的随机密钥只在权限600的本地env，不进入Git/报告/日志。

这是本地容器交付与恢复验收，不是生产高可用、透明滚动升级、跨平台认证或容量报告。资源采样是某一时刻而非峰值；停止服务不保证Docker VM缓存立即释放。完整HTTP→Agent→Inbox Trace/告警平台未在本轮扩建。**D4完成后停止，不自动发布或扩业务。**
