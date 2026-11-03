# 后端容器运行与验收

适用D4交付，schema0011。普通配置是 `compose.backend.yaml`，与旧 `compose.yaml` 分开。默认项目aftercare-backend；本轮真实验收专用项目aftercare-d4-test。不要将本指南的测试命令改到旧demo、55432或55433。

## 构建与首次运行

默认Dockerfile使用固定Python基础镜像和hash锁依赖，正常构建会下载准备依赖。本轮本地实际执行的是已有缓存运行时复用，没有下载大型镜像；其metadata锁版本严格通过，不等于重验原wheel hash。GitHub配置包含默认干净构建，但云流水线未运行。

```bash
# 在项目根目录。仅当该本地缓存镜像已存在且完整匹配当前平台锁版本时使用。
docker build --pull=false --network=none \
  --build-arg BASE_IMAGE=aftercare-agent-workbench:local \
  --build-arg REUSE_LOCKED_RUNTIME=true \
  -t aftercare-agent-workbench-langchain:backend .
```

复用时缺包或版本不匹配会在卸载前失败。显式reuse只对新镜像构建层移除锁外包，不修改原镜像。不要忽略错误继续启动。无合适缓存时，默认 `docker build -t aftercare-agent-workbench-langchain:backend .` 走正常hash安装路径。

Compose的pull_policy为never：启动前也必须已有固定PG镜像 `pgvector/pgvector:pg17@sha256:ac08538c6f8b9904c33c8224c5e5706dbe760aca29db1d096972b4052c22a75d`。构建app不会准备PG镜像；新机器需要显式预备该镜像，属于在线准备，本轮未执行下载。

生成一次本地环境文件；已有文件不会覆盖，尤其不要在恢复原卷时无意替换原环境：

```bash
.venv/bin/python - <<'PY'
from pathlib import Path
import os, secrets
p = Path('runtime/backend-local.env')
p.parent.mkdir(parents=True, exist_ok=True)
fd = os.open(p, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
with os.fdopen(fd, 'w') as f:
    f.write('BACKEND_WAREHOUSE_TOKEN=' + secrets.token_urlsafe(40) + '\n')
    f.write('BACKEND_CALLBACK_SECRET=' + secrets.token_urlsafe(40) + '\n')
PY
docker compose --env-file runtime/backend-local.env -f compose.backend.yaml \
  up -d --no-build --pull never --wait --wait-timeout 90
```

应用仅宿主回环8138，数据库仅回环55436，库名aftercare_test。DB健康→迁移CLI成功→app健康→warehouse健康→履约Worker。三个命名卷分别保留PG数据、PG socket、仓储SQLite；没有自动seed、重置、真实模型或故障注入。

普通配置客户demo登录默认关闭，员工凭据仍用原staff CLI单独创建。仅在本地测试演示时叠加 `-f compose.backend.acceptance.yaml` 开启客户入口，再使用现有seed/员工CLI；不能把模拟客户登录作为真实认证。可选 `--profile agent` 启动原scripted Agent Worker，不调用云端模型。

## 停止、恢复和升级

```bash
# 停止并移除本项目容器/网络，保留三个数据卷。
docker compose --env-file runtime/backend-local.env -f compose.backend.yaml down
# 恢复仍使用原env、原项目名和卷；启动命令同上。
```

不执行 `down -v`、volume prune或demo reset。资源上限不是预留内存，也不能保证Docker虚拟机缓存立即回到macOS；停止本轮容器只保证这些服务进程结束。

warehouse和Worker共享app网络namespace。升级或重建app时应协调重建warehouse、outbox、query、callbacks和已启用的agent。使用完整项目down（不删卷）→up是本轮实测路径；任意单服务force-recreate app不是已验证的透明滚动升级。

schema未到0011时ready不会成功；migrate是增量升级，不删卷。`migrations/env.py`新增可信调用方Connection/私有版本表支持，正常CLI行为不变。D4历史验证使用独立私有schema，不能照抄到真实账本。

## 恢复本轮已验收数据

本轮容器已关闭，卷名为 `aftercare-d4-test_pgdata`、`aftercare-d4-test_pgsocket`、`aftercare-d4-test_warehouse_state`。随机测试密钥位于权限600的 `runtime/backend-D4/local.env`，不属于公开证据。恢复时不要重新生成或公开它：

```bash
docker compose --env-file runtime/backend-D4/local.env -p aftercare-d4-test \
  -f compose.backend.yaml -f compose.backend.acceptance.yaml \
  up -d --no-build --pull never --wait --wait-timeout 90
```

数据是一条已完成换货：v6、1份仓储事实、消费1件，两个已送达回调在Inbox为STALE；临时员工均禁用，无持久可复用登录凭据。私有迁移schema另保留结构性旧记录和模拟UNKNOWN用量，未参与公开Worker执行，不是真实调用。

## 验收工具与故障定位

`scripts/backend_d4_acceptance.py`严格限定55436、固定本轮仓储容器及本机HTTP；每一步使用新输出目录，失败不清业务。正常prepare→verify-pending→verify-success→replay-provider→verify-callbacks；容器生命周期由操作者在步骤间控制。原员工幂等键绑定actor，成功后重放使用同一临时actor ID、新随机密码，最后再次禁用；密码不写state。

宿主端口映射不能访问仓储仅绑定容器loopback的监听。provider验收通过固定 `docker exec -i` 在仓储namespace发真实HTTP，Bearer/正文走stdin，有界超时、无代理/重定向，不开放仓储宿主端口。生产Outbox/query仍用原BoundedWarehouseClient。

仓储SQLite应使用backup导出一致快照，不直接复制有WAL的主文件。此次Docker archive API无法读取tmpfs内backup，已改为exec文件流导出同一backup；首次错误和诊断保留。日志和SQL/SQLite快照都可能含本地业务/认证数据，仅保存在runtime，不推送。

CI脚本 `backend_d4_ci_check.py` 的target/preflight限定55434和显式关闭模型，audit限定完整JUnit/关键模块/已知skip。安装准备阶段有网络，pytest阶段使用离线保护。工作流未推送触发，因此没有GitHub云端运行链接或绿色检查结论。

详细结果见[D4交接](handoffs/backend-D4.md)、[验收报告](benchmarks/reports/backend-D4-acceptance.md)和[验证清单](benchmarks/reports/backend-D4-validation.json)。
