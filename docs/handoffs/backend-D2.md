# D2 外部快照、SSE与刷新合并交接

2026-10-29。D2 已完成后停止，后续D项未启动。迁移仍为0011。本批完成外部读写路径分离、SSE短批次与空闲退避、前端单飞/尾随刷新；没有修改业务、Agent、仓储协议或恢复功能。

[性能报告](../benchmarks/reports/backend-D2-performance.md) · [兼容契约](../design/backend-D2-read-stream-contract.md) · [验证清单](../benchmarks/reports/backend-D2-validation.json) · [D1历史交接](backend-D1.md)

## 实现与一个完整机制

新 `services/read_snapshot.py` 在单个RR/READ ONLY事务内完成客户归属、原任务状态推导、完整消息/预览/jobs/原回执读取；`api/routes.py::snapshot`直接返回该服务，不再先调用内部锁snapshot、再另session拼接元数据。

内部 `BusinessService` 整文件保持D1版本，包括 `_conversation FOR UPDATE`、run_guard、snapshot、可信确认和写命令。旧Worker与仓储恢复代码不变。原 `load_events` 与trace路由也未变。外部数据库版本一致不等于服务器时间冻结，预览仍逐项按原时钟判断；展示不是提交许可。

新 `services/event_reads.py` 每批3次应用SQL（SET事务、session/归属联合查询、最多100事件），完成并归还连接后才yield。SSE初始批次经to_thread在headers前读取，保留初始401/404；满批让出事件循环后立即取下批，非满批heartbeat，空闲0.5→1→2秒。once仍最多100，trace ID/data字段不变，Last-Event-ID与after取最大值。

流内权限或会话失效会发无id的stream_closed，随后结束；前端显式close，不推进重放游标。每批重验身份，不把临时数据库故障改成成功或身份失效。

一个完整过程：Writer持会话锁提交事件 → SSE短只读批次获取已提交前缀并关闭事务 → 浏览器收到多条trace并保留全部事件 → 60ms合并完整快照读取 → 若读取过程中又有事件/写后刷新则补一次尾随新读 → 同一MVCC快照渲染完整状态。切换会话改变epoch并中止旧请求，旧EventSource回调/响应不能污染新会话；确认/提交仍在原写锁与版本校验下执行。

前端新增 `web/static/refresh_queue.js`，`app.js`只接入队列、帧级事件渲染和连接终止处理；原页面结构及完整快照字段不变。读取失败和超时会释放队列，显式写后await不会错误复用写入前已开始的旧请求。

## 实测收益与代价

配对fixture、源码/资产指纹、PG/GUC及依赖冻结。先统一ANALYZE本次固定数据；前、中、后三份pg_stats SHA256一致。5快照+5有限SSE档位每侧300正式请求、30预热，无失败；完整静态snapshot与trace ID/data hash一致。普通/有限请求为顺序ASGI，空闲与连续回放才使用真实TCP。

| 场景 | 结果 |
|---|---|
| 写锁竞争：外部snapshot | 旧HTTP500/327.640ms且真实Lock等待 → 新HTTP200/22.041ms、未观察该等待 |
| 写锁竞争：SSE启动 | 旧HTTP500/323.571ms且真实Lock等待 → 新HTTP200/13.623ms、未观察该等待 |
| 内部snapshot保护 | 两侧仍55P03/真实Lock等待，约304.484/302.954ms |
| 3.2秒空闲1连接 | SQL21→10，会话FOR UPDATE 8→0 |
| 3.2秒空闲4连接组 | SQL84→40，会话FOR UPDATE 32→0 |
| 1000事件真实TCP回放 | 5139.033→1441.402ms，全部ID相同；SQL27→34 |
| 前端受控两波200事件回放 | 假API读取200→2，峰值并发100→1，最终revision均200 |
| 真实Chrome一次200事件提交 | 全部事件保留，页面snapshot GET=1；非TTI/通用节流收益 |

锁探针连接统一300ms/2s是测试条件，生产默认仍5s/10s；旧500是预期旧分支并原样记录。空闲窗口含启动/认证，不是长期QPS。backlog新SQL包含逐批身份校验、SET及最后多读空批，不把27→34隐藏为改善。

负向指标：外部快照多1条SET事务SQL，100预览p95 9.575→11.125ms、1000消息16.167→16.524ms；有限100事件p95 6.751→7.354ms，其他百事件/重连档位也有回退。完整响应体积未减小。D2收益是避免外部行锁阻塞、修复混合版本及减少重复刷新/积压等待，不是全面延迟下降。

## 验证结果

| 批次 | 结果 | 记录 |
|---|---|---|
| D2专项+旧web/D1读取保护 | **74 passed，11.26秒** | runtime/backend-D2/integration-first.xml/.log |
| 完整离线回归 | **804 passed / 4 skipped / 1 deselected，114.46秒** | runtime/backend-D2/full-first.xml/.log |
| 独立前端队列单测 | **9 passed** | runtime/backend-D2/refresh-unit-first.log |
| 真浏览器功能与视觉QA | PASSED，4条功能检查、2张截图 | runtime/backend-D2/browser-first/ |

Python新增43项：外部快照26、SSE17；761+43=804。Node9项独立计数，不把专项/功能检查再叠加到全量。4跳过仍为可选tau2依赖，1排除仍为权重推理，无新增D2跳过或应用回归失败。

快照专项覆盖15类状态/回执组合、旧展示后改口confirm409、内部失权guard仍拒绝、真实HTTP读中append+queue与commit+receipt保持单数据库版本。SSE专项覆盖真实双writer/全局ID交错、scope/过期、once/重放、满批立即排空、退避、慢流帧间不占连接及失效终止。

真浏览器用fresh自动ID会话，没有在高显式ID性能fixture上活写。检查登录、消息显示、无Worker取消pending job；200事件完整显示；旧fetch门闩暂停后切换会话不串数据；会话失效显式close后2.5秒内无重连。额外context.request断言GET不属于burst页面计数。两张截图已目检，无页面脚本异常。

## 必须保留的边界

- 事件前缀安全依赖受支持生产写入口在分配ID前持同会话锁，及实际events_id_seq CACHE=1；不覆盖手工ID、setval倒退、改CACHE或自定义未加锁writer。event()本身没有锁断言。真实双writer测试与配置原始记录均留存。
- 退避2秒只是空闲等待上限，不是任何网络/慢消费者下的通知或失效SLA；还要加调度、SQL、已加载批次发送时间。身份撤销在下一批读取点重验。
- 每批100限制条数，不是字节上限。发送/背压阶段不持DB连接；正在执行的to_thread查询不能因断连瞬间终止，仍受原数据库超时。两次0.7秒观察证明本条件下停止后续查询并归还连接，不外推任意故障。
- 连续wire包含不同heartbeat，只有trace语义相同；不宣称整个连续响应字节相同。读事务仍持正常数据库读锁，只是不使用会话FOR UPDATE。
- 外部/内部显示逻辑现在两份，未来变更须保留状态矩阵对照；RR统一数据库事实，不能替代命令确认或执行权。

## 证据与清理

开始时D1的170源码、83证据、6文档和历史C证据全部匹配。保留business/routes/app.js原文件和source-before；BusinessService、Worker、所有写路径、原load_events/trace保护检查通过。主测量harness前后同SHA，前后完整Web资产hash另列。

固定性能pair只清理自己的前缀并恢复原表计数；之后才做浏览器活写与pytest。浏览器/临时服务均结束，8136/8137关闭。当前专用库是完整pytest余留，最终health ready、schema0011、库存及订单数量违规0、sequence CACHE1。旧A/B/C/D1报告、原始性能/故障记录和仓储快照不覆盖。

工具准备曾有应用内webview挂载超时、Node已移除选项被拒；单列记录后改用已有Chrome/正确参数通过，不安装软件、不把工具错误算成应用成功或隐去失败。没有真实模型、独立holdout/Test、旧demo/真实账本访问、依赖下载、新容器或推送。

## 复现与停止点

`scripts/backend_d2_benchmark.py` 支持 prepare→measure before/after→compare→cleanup；只允许专用测试库、新输出目录和显式关闭模型。主pair已清理，不能当活数据重复测；复测须新pair，先协调无并行测试并保留当前数据。高ID fixture只读，不能为写入演示重置全局sequence。

```bash
AFTERCARE_TEST_DATABASE_URL=postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55434/aftercare_test \
ALLOW_REAL_MODEL_CALLS=false .venv/bin/python -m pytest -m 'not live_model and not model_weights'
```

Node用现有运行时执行 `node --test tests/web/test_refresh_queue.mjs`。浏览器脚本另要求已安装Playwright路径D2_PLAYWRIGHT_PATH与系统Chrome，不自动下载；其服务端/输出在本批runtime中。当前Node24直接识别.mjs及ES模块，无需已移除的默认模块实验参数。

**D2到此停止。多Worker容量、Trace/指标、CI/Docker及其余D项等待后续授权。**
