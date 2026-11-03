# D4 离线 CI 与 Docker 交接

2026-10-30。D4按原路线完成后停止，schema仍0011。交付可重复的离线门禁、独立后端Compose、受控镜像构建，以及有数据迁移/容器重建/持久回调验收；业务、Agent和仓储协议未扩展。

[验收报告](../benchmarks/reports/backend-D4-acceptance.md) · [运行指南](../operations-backend.md) · [契约](../design/backend-D4-delivery-contract.md) · [验证清单](../benchmarks/reports/backend-D4-validation.json)

## 交付入口

- `.github/workflows/offline-checks.yml`：固定输入、独立PG、完整离线回归及18模块/skip门禁，Node、canonical镜像构建与失败artifact。
- `compose.backend.yaml`：PG、one-shot迁移、app、独立仓储、Outbox/query/callback，另可选原scripted Agent；旧Compose不替换。
- `Dockerfile`、`scripts/backend_runtime_lock.py`：默认hash安装；本地显式缓存复用先验证再清锁外包，当前项目重装并严格复核，非root状态目录。
- `scripts/backend_d4_acceptance.py`：真实三角色HTTP、同actor幂等重放、namespace内provider HTTP、迁移旧列验证；每步新证据目录，失败不清业务。
- `scripts/backend_d4_schema_migrate.py` 与 `migrations/env.py`：限定新测试库和私有schema，由可信调用方持有Connection/版本表；正常迁移CLI原行为保持。

最终完整回归 **934 passed / 4 skipped / 1 deselected，123.05秒**；新增92项，另Node9通过。首轮924通过、实际工具连接失败、修复前红测试和修复后输出均保留；没有云CI执行或真实模型请求。

## 一个完整机制

真实仓库员工经HTTP批准 → 同事务持久申请v5/库存预留/出库请求/Outbox → 停消费者并整组移除容器（不删卷） → 用原PG和SQLite卷重建后完整PENDING快照相同 → 原Outbox Worker领取/发送许可/HTTP/事务完成 → v6、消费1件、仓储1事实 → 仓储及整组重建后同键POST仍返回原事实 → 独立HMAC sender发送持久事件 → Inbox将已完成业务的两个事件标STALE，确认送达且不重复消费。

同一申请 `app_46f92f7b105c48a4a8baa79e234ee0f7` 最终库存20=19可用+0预留+1消费，历史6条，原receipt和server_session仓库操作者保留。旧幂等命令重放返回原v5 receipt，当前业务仍v6，两者语义不同但都真实。

初始失败发生在验收工具的provider访问：宿主端口DNAT不能命中容器内loopback监听。修复只在固定provider验收transport进入容器发本机HTTP，凭据/正文走stdin，不放宽产品guard，也未新造申请替代失败。

## 当前环境与恢复

本轮仅新项目aftercare-d4-test，55436/aftercare_test、8138，独立三个命名卷；原55434专用测试库已在pytest前备份，旧demo/真实账本未连接。当前D4容器已关闭释放服务进程，卷与镜像保留，恢复命令见运行指南。临时员工均禁用，不保存可复用登录凭据。

本地镜像是87锁分发+project，无Deep Agents残留分发，133源码/资产文件集合和hash匹配。实际本地只测Linux/arm64缓存构建；metadata版本核对不是wheel hash复验，默认干净下载构建/AMD64/GitHub云job未执行。不能把已写workflow称为云CI已绿。

私有迁移schema保留结构性历史fixture：原0006→0010→0011，旧字段/回执/模拟UNKNOWN预留99不变，旧attempt LEGACY_UNKNOWN；重建后再次核验通过。公开model_calls0，私有模拟记录不是真实调用，不参与公开Worker执行。

## 已验证边界

实际DB停机时live200/ready503，恢复后ready200。共享network namespace的app重建必须协调全部共享服务；实测的是整组down/up保留卷，不是任意单服务透明滚动升级。原权限、内部snapshot/run_guard、发送许可、Outbox/Inbox/UNKNOWN和预算规则保持字节一致。

原D3源码仅迁移环境入口变化，其余183项不变；新增文件、Docker构建输入和最终证据完整hash在验证JSON。原A/B/C/D1/D2/D3证据不覆盖，SQL/SQLite备份只在runtime。Docker cp/tmpfs及归档器可选字段问题有原始失败与最终修复记录，不影响最终业务结论。

CI/Docker这一批到此结束。后续若做云CI首跑、发布或完整跨系统Trace，应另列范围与真实结果；不自动推送、公共部署或新增业务平台。
