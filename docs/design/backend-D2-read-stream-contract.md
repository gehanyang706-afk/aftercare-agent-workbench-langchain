# D2 外部只读快照与 SSE 契约

2026-10-29。用户在 D1 完成后批准下一步，本批只做外部 snapshot、SSE 和客户端刷新合并；保持迁移0011，不进入后续并发容量、Trace或部署工作。

## 外部读取与内部执行分离

`GET /api/conversations/{id}` 改用独立 `ReadSnapshotService`。普通归属 SELECT 后，在一个 `REPEATABLE READ, READ ONLY` 事务内读取会话、消息、预览、当前状态、全部 jobs 和原操作回执。旧接口字段、列表顺序、标签和成功判断保持不变；静止数据下完整 canonical JSON 必须与 D1 相同。

`BusinessService` 整个文件、`_conversation` 的 FOR UPDATE、run_guard、内部 snapshot、所有写命令与 Worker 不修改。内部 Agent/Worker 继续调用原受保护入口。外部读取不代表执行许可；展示的预览随后可能过期或失效，confirm/commit仍在写锁下重新核对身份、版本、可信确认及库存。

数据库字段来自同一MVCC快照；逐预览时钟判断继续使用原业务时钟，不宣称把服务器时间冻结在事务开始。任务成功继续要求当前 revision 的实际 Operation及绑定的final notice，不能仅凭graph END或历史回执显示成功。新投影镜像内部显示逻辑，状态矩阵测试用于防止后续两份投影漂移。

## SSE 读取与游标

- 不再在 SSE 启动时调用完整内部 snapshot。初始 batch 在 `asyncio.to_thread` 中读取、提交后才发 headers，初始身份与归属错误保留正常HTTP状态。
- `EventReader.read_batch`：一个短RR/READ ONLY事务；SET、session/归属联合查询、最多100条 Event 查询，共3次应用游标执行。每批重新核对真实身份时钟的有效期及原主体，不用测试业务时钟代替身份时钟。
- 查完并关闭事务才发送帧。trace的id、事件字段、JSON内容和排序不变；`once=true`仍只读一个最多100条批次。游标取max(0,after,Last-Event-ID)，拒绝超signed BIGINT范围的值。
- 连续流遇满批时让出事件循环后立即取下一批；非满批才heartbeat/等待。空闲间隔0.5→1→2秒封顶，有数据复位0.5秒。减少空闲轮询的代价是新事件通知可能等待约2秒，不声称维持原0.5秒最坏等待。
- 流内发现SESSION_EXPIRED/NOT_FOUND时发送不带id的 `stream_closed` 后结束，客户端关闭EventSource。它不推进重放游标；其他数据库故障不能冒充身份失效或成功，按原网络错误重连规则处理。
- 原 `load_events` 和 `/api/runs/{id}/trace` 保持原路径，本批没有将所有外部读接口统一改造。

## 事件顺序的必要前提

当前受支持的生产写入口在分配/flush Event ID前持同一会话行锁，提交后才释放；新会话创建行在同事务提交前不可见。实际 `public.events_id_seq` CACHE=1。因而同会话事件可按已提交的递增ID前缀读取；不同会话的全局ID交错不影响单会话after。

这一契约不覆盖直接SQL插入、手工ID、sequence倒退、改变CACHE或自定义未加锁writer。`BusinessService.event`本身没有强制锁检查。专项通过真实两writer验证：低ID已flush但未提交时，第二writer仍在等同会话锁，未分配新ID；期间读不越过未提交项，提交后回放无缺项。性能fixture的高显式ID仅用于只读回放，不混入自动ID活写，也不setval修改全局序列。

## 前端刷新

事件渲染按动画帧合并；完整快照读取由独立队列合并。正常情况下同一时刻仅一次读取，事件触发60ms合并；读取途中又收到事件或显式写后刷新时，必须保留一次尾随新读。发送/确认/cancel的await不能只等待写入前已开始的旧读取。

会话切换更新epoch并中止旧请求，旧响应与旧EventSource回调不能污染新会话；返回同一会话也必须区分epoch。读取超时有界并允许队列继续处理。失效终止帧关闭连接、清除待确认入口并提示重新进入工作台，不自动执行写操作。

前端合并计数分别报告受控假网络回放和真实浏览器请求，不把二者当成TTI或任意事件节奏下的通用收益。持续有新事件时仍可能每次读取后追加尾随读取，这是保新鲜度的成本。

## 限制与验收

读事务仍持PostgreSQL正常读取所需锁，所谓移除锁仅指不再使用会话FOR UPDATE。每批100限制条数，不是任意payload的字节上限。慢消费者在yield阶段不占DB连接，但已开始的to_thread查询无法因客户端断连瞬间杀掉，仍受现有DB超时约束；只验证帧间停止后续轮询与观察窗口内连接归还。

同fixture/依赖/PG/GUC/脚本下保存优化前后SQL、延迟、字节、trace与完整snapshot hash、真实Lock等待、空闲1/4连接及积压回放。正常样本与预期超时探针分开列分母。完整回归覆盖B/C/D1执行保护；负向指标、增加的事务SQL、退避延迟和工具准备失败保留。只用55434/test、关闭真实模型、不覆盖旧证据，不增加消息基础设施。
