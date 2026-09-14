# sparse_copy 分步骤实施与阶段验收

更新：2026-09-12。阶段 1 已在 `main@38e6637` 之上的工作区实现并完成受限本机检查，真实 Linux/AArch64 构建与 RDMA 验证仍待执行；阶段 2/3/4 尚未编码。ubs-comm 基线保持 `oneside-msge-merge@9e4c035a5d68ccca02d05fade3b6f5907db24ef4`。

## 1. 交接结论与执行顺序

`main@38e6637` 的旧协议由 remote 自主发数据并计时，含 ROUND_ACK，不含每次 600 项地址请求。当前工作区已按本计划完成阶段 1 requester-driven 重构；**下一执行任务是目标机阶段 1 验证和同协议阶段 1.5 对照，不是立即推进双卡。**

顺序：

1. 在 main 重构阶段 1：local 发完整请求，remote 按请求 direct Put，local 等数据可用返回并计时，删除逐轮 ACK。
2. 复核迁移的阶段 1.5 A+B；保留优化，不复制旧 ACK 状态机。
3. 在阶段 1 新协议稳定后同步到其它分支，再迁移双卡的独立能力，重跑单卡回归。
4. 阶段 2 在相同协议/优化上比较单卡、双卡，独立 trace。
5. 阶段 3 加 SGL 8/16/30、staging、流水 scatter。
6. 阶段 4 最小改造 ubs-comm 的 WRITE_WITH_IMM，成对比较 Send/IMM。

当前阶段 1 修改了源码、脚本、JSON 示例和文档，但未修改库或其它 worktree。后续按本文件逐阶段交付，未完成验证不宣称阶段通过；无硬件时注明 HW_PENDING，不虚构数据。

## 2. 各阶段不可改变的契约

- local=调用者/目标端；remote=源数据/WRITE 发起端。新 CLI 角色 local/remote，remote 监听、local 连接。Python 只运行本地主机一个角色，不通过 Python 每轮发请求。
- 一次 sparse_copy 是 local 入口到 local 返回：包括校验/编码完整地址请求、请求传输、remote 接收复制/解析/WR 构造、数据传输、完成处理，以及 SGL 的全部 scatter。
- 每次 600×1024=614400 字节有效数据。阶段1/2 direct强制600对源/目标偏移，9600字节描述符、64字节头，COPY_REQ=9664字节；双卡只在rail0发一份完整请求。阶段3 SGL的目标地址仍为600个，只有调用方输入本身提供可验证的聚合源描述符时，source数量才可降为总chunk数；K=30时可为20 source +600 destination，描述符/请求分别为4960/5024字节。
- 建链、MR/区域交换、容量分配、稳定数据和调用方输入列表准备在计时外。local把自己的dst/staging基址、大小、区域ID和完整key发送给remote；remote只返回source区域ID、大小及能力，不向local导出source key。请求wire编码、remote本轮WR/iov填充在调用内；不能只发送generation重用预置地址。
- 每 session 一次调用在途，逐轮 ROUND_ACK=0；remote 只有收到下一 COPY_REQ 才能开始下一代。local 等数据 ready/全部 scatter 加本地请求 Send 完成后返回。
- remote 上代私有 callback 回收与下一请求可交错。一个 pending 请求槽和独立 active 存储避免覆盖；remote 排空旧 callback 后才复用 WR/源验证数据，该等待计入下一次调用。
- 完成通知覆盖同一真实 QP 上此前 WRITE；双 rail 分别通知，不能 rail0 一条全局通知覆盖两卡。普通通知仍用 hcom Send；阶段 4 才改 chunk 的 IMM。
- 失败、超时、内容错误或未安全drain使整个run无效。remote可用时通过仅失败路径的COPY_ERROR通知local立即结束等待，发送路径不可用时主动断链唤醒；成功路径仍ACK=0。保留callback所有权、ActiveCallbackGuard、异步失败处理和有界退出。
- 阶段 1/1.5/2 direct 不分配 staging、不做 scatter，不将 600 次异步 Put 变成逐个同步等待。

## 3. 文件、脚本与结果交付

沿用最小文件集合：`rdma_600.cpp`、`CMakeLists.txt`、`build.sh`、`run.py`、`hosts.example.json`、README、PROGRESS 和阶段报告。不引入通用 benchmark 框架，不复制 memfabric，不新建 Python RDMA 层。

### 3.1 新脚本契约（待阶段 1 实现，当前不可运行）

两台主机用同一配置，各运行一个本地角色；每个物理 repeat 重启两端并使用新输出目录。配置将旧 sender/receiver 改为 local/remote，每端字段为 binary、library_dirs、oob_ip、app_cpu、nics；nics 每项包含 rdma_ip、oob_port、worker_cpu。两端列表按 rail 对齐，remote.oob_ip/oob_port 是监听目标。协议模式/links/轮数等测试参数双方一致，并由 C++ 握手复核。

`run.py --suite stage1` 只选 SC-B1；stage2 选择 direct B1/B2；stage3 选择设计主矩阵；stage4 选择 SGL send/imm 成对测试。建议增加 `--case` 选择单 case，便于两个终端逐项配对执行，避免双端遍历配置不同导致误连。`--kind verify|measure|trace` 与参数校验同步实现；trace 在阶段 2 才开放。

```bash
# 目标命令，必须在阶段 1 重构后才运行；remote 主机先启动
python3 run.py --config hosts.json --role remote --suite stage1 \
  --case SC-B1 --kind verify --output results/<run-id>-remote
# remote LISTENING 后，local 主机启动
python3 run.py --config hosts.json --role local --suite stage1 \
  --case SC-B1 --kind verify --output results/<run-id>-local
# 正式测量：两端同时改 --kind measure，warmup=1000、rounds=10000
# 每个 repeat 新建进程/目录，重复 5 次；verify 默认 20 轮
```

阶段 2 的目标命令以 `--suite stage2 --case SC-B1` / `SC-B2` 分别执行；详细诊断单独 `--kind trace` 短跑（例如 100 轮），不能与正式 measure 混表。stage1 可以继续只接受一组 NIC；扩展两卡时再验证数组长度和 CPU 配置，不提前做冗余多卡抽象。

### 3.2 结果归属与校验

local 输出性能 JSON，remote 输出成功/失败状态及诊断，不输出伪造的 local 延迟。目录：

```text
results/<run-id>-local/  manifest.json, local.stdout.log, local.stderr.log,
                       result.jsonl, REPORT.md, [trace.jsonl]
results/<run-id>-remote/ manifest.json, remote.stdout.log, remote.stderr.log,
                        status.json, [trace.jsonl]
```

manifest 保留命令、配置、源码提交/dirty diff、二进制 SHA-256、静态 hcom 链接输入 hash、动态依赖/LD_LIBRARY_PATH、退出码、线程/设备/NUMA/队列/MTU。脚本只清理它启动的本机进程组，不 pkill 同名进程，不拿旧结果顶替失败运行。

local结果schema=3，必须包含设计文档的protocol、measurement、result_role、case、source_format、实际source/destination地址数、请求字节、布局/工作量、数据/通知WR expected、ACK=0、轮数和sparse_copy统计。verify的性能值为null；成功measure必须有限正值。禁止继续接受旧sender/ACK schema，也禁止仅重命名旧submit/e2e字段冒充新主指标。

local 进程仅在最终校验和 FINISH 成功后输出有效结果；汇总报告还需确认对应 remote 成功退出及双方证据。没有远端证据标 incomplete，而非在本地伪造。WR expected 与短诊断 measured 分开；网络包/端口字节不等于应用 WR 数。

## 4. 阶段 1：重构 local 发起的 direct baseline

### 4.1 先审阅再修改的代码位置

| 现有位置 | 具体调整 |
|---|---|
| Role / 参数解析 / 配置 | sender→remote、receiver→local 的语义迁移；remote 监听、local 主动连接；旧协议明确拒绝 |
| SetupService / ConfigureChannel | maxSendRecvDataSize 1024→16384，两端一致；保留 TLS 关闭、唯一 QP、禁用 split/RNDV、已有队列/CPU 参数 |
| HELLO / READY | version=3；local发送本rail dst/stage基址、大小、区域ID和完整key；READY只返回source区域ID、大小和能力，不返回source key；READY之后不清零已开放的内存 |
| BuildPutRequests | setup 只 reserve 容量；每次 remote 从 COPY_REQ 重新生成 600 个 req |
| RunSender / RunSenderRound | 改为 remote 请求驱动服务循环，删除自主轮次推进和 remote 主计时 |
| RunReceiver | 改为 local Verify/Warmup/Measure 调用 sparse_copy |
| OnIncoming / ROUND_READY / ROUND_ACK | 新增COPY_REQ、DATA_DONE和仅失败路径COPY_ERROR；删除逐轮成功ACK编解码、接收状态与SendRoundAck路径 |
| WaitData / counters / callback | 迁移现有 1.5 优化；新增 pending/active 所有权、请求 Send 完成；不破坏部分失败 drain |
| FINISH | local 最终检查后请求退出，remote 排空再回复；不进入主时延/wall |
| JSON / run.py / self-test | 结果转 local、新协议字段、完整请求编解码和错误用例 |

不为重构同时增加双提交线程、callback 池、NUMA 自动调度、SGL 或改库。

### 4.2 推荐编码顺序

1. 保存 main 现有源码/hash 和旧协议标识；阅读 README/历史报告，确认哪些代码可复用。
2. 实现direct CopyEntry、64字节头、600×16字节正文和32字节DATA_DONE/COPY_ERROR的显式编解码。头内显式携带source_format；按DIRECT_PAIRS/SPARSE_600/GROUPED分别复算计数、descriptor_bytes和实际接收长度，不能只信任wire长度字段。定义固定槽对齐/范围/目标互异契约，拒绝旧version=2。
3. 调整角色/setup/接收容量，完成无数据的 9664 字节 Send smoke。callback 复制接收内容到预分配 pending，不能保留 ctx 指针。
4. 提取 `sparse_copy(Session&, entries)`：本地校验、编码、Send、ready/请求 Send 完成等待。先发布 expected generation 再发请求。
5. 实现 remote 请求服务循环、pending→active 交接、按本次列表异步 600 Put、同 QP 一个 DATA_DONE。WR/key 使用对应区域和 rail，不能复用 setup 固定列表。
6. 删除逐轮成功ACK及其成功条件。下一请求隐含上一轮已消费，remote callbacks私有回收仍保留；补齐COPY_ERROR/断链唤醒，正常退出FINISH排空。
7. 将 verify/warmup/measure 驱动与 CLOCK_MONOTONIC_RAW 主时钟迁至 local。reserve/容量准备在 wall 外，逐调用序列化和 WR 填充在内。verify 用 g/source_slot/word 填完整源池；之后冻结最后 verify 的源模式作为 warmup/measure seed，避免阶段切换重填污染首轮。通知 generation 继续递增，measure 不逐轮填数据。
8. 更新 run.py、示例配置、help、README、schema 校验和 PROGRESS，交付可直接执行的双端脚本。不得将计划状态写成已硬件通过。

### 4.3 分层验收

| 层次 | 必须验证 | 通过标准 |
|---|---|---|
| 本地逻辑 | 请求 64+600×16=9664，通知=32；字节序、版本、截断、越界/溢出、目标重复 | self-test 真执行通过；旧消息不能误解码为新协议 |
| 目标构建 | 真实 Linux/AArch64 头、hcom_static/boundscheck 链接 | 记录命令/版本/退出码；Windows 受限语法检查不代替 |
| 请求 smoke | 只发完整请求，远端解码 600 项非顺序映射 | 长度/首尾及全项内容一致，无分片/RNDV 配置误用 |
| direct verify | 20 轮改变源/目标排列，600 块全 word + gap 校验 | remote 确实按本次列表写，generation/条目映射正确 |
| 无 ACK 复用 | 延迟 local 下一次调用；延迟 remote 上代回收 | remote 不自主新代，pending/active 不覆盖，下一调用含资源等待 |
| 通知顺序 | 唯一实际 QP，600 WRITE 后 DATA_DONE | local 数据可用才返回，每调用请求=1、完成通知=1、逐轮 ACK=0 |
| 错误/退出 | 错代、非法消息、post失败/COPY_ERROR/断链/超时、FINISH drain | local被及时唤醒并有界非零退出；无悬空callback/MR，不输出有效结果 |
| 性能 | trace 关闭，1000 warmup+10000 measure，独立 5 次 | local 主指标、完整请求成本；同配置原始结果可追溯 |

主指标为 sparse_copy_avg/p50/p95/p99、614400 字节/调用有效带宽、600 block/调用 Mops、请求字节吞吐及整段 wall。旧协议数据保留到历史部分，不作为新协议 speedup 分母。通过要求正确且证据完整，不预设更快。

交付 `STAGE1_REFACTOR_REPORT_CN.md`（编码阶段创建），记录状态/变更/验证/真实结果/未测项。只有源码检查完成时标 IMPLEMENTED/HW_PENDING，不能标阶段 HW_PASS。

## 4.5 阶段 1.5：优化迁移复核和同协议测量

A+B 已在 main 存在，这一阶段以复用、核验为主，不重新执行旧协议任务。

### A：数据面等待

- local 等 ready/请求本地完成、remote 等请求/上代回收使用 CPU relax 忙轮询，不逐次 `std::this_thread::yield()`。
- 每 256 次检查 deadline，每次检查 fatal 和 acquire 谓词；本次调用共享一个绝对 deadline，不让编码/等待/scatter 各获得新的 10 秒。
- app 与 worker 使用不同物理核心；记录 CPU 占用，不能只报延迟收益。
- setup、idle/退出和失败 drain 保留合适的有界控制等待。计时热路径不插入日志或逐次读时钟。

### B：计数和 callback 成本

- app 独占的 attempted/submitted 为普通 uint64_t 累计值；CQ 拥有的 completed、跨线程 pending/ready/active callback 为原子，保留 release/acquire。
- 按真实写入线程隔离缓存行，记录编译对齐值；READY Reply 等 CQ 发起路径不能误归 app 私有。
- 正确处理 callback 早于 API 返回、失败不保证 callback、上下文删除所有权；保留 ActiveCallbackGuard。不能为快而提前宣布完成。
- UBSHcomNewCallback 分配先保留，profile 确认主要成本后再提最小安全复用，不引入通用池或偷偷改 hcom。
- reserve 在 wall 外；完整请求编码、接收复制、解析和 WR/iov 填充仍在调用内。

验收：新协议 SC-B1 的功能、无 ACK 复用和错误路径回归。若报告 A/B 收益，构造**同为 sparse-copy-v3** 的 original/A/AB 临时变体，在相同请求/线程/队列/布局下各跑 5 次；保存准确 diff/hash。不能用旧协议 `2a14e00` 或历史报告数字直接比较。无硬件则明确未测；未评估 callback 池不影响本阶段基础优化交付。

## 5. 同步其它分支的边界

先在 main 完成阶段 1 新协议和上述复核，再记录可复用提交。其它分支先整合该公共基线，再迁移自身双卡/trace 改动；使用 merge/rebase/cherry-pick 哪一种由当时分支历史决定，不盲目套旧提交覆盖新状态机。

同步核对：角色/握手、9664 字节 COPY_REQ、请求驱动、pending/active、无 ACK 复用、local 主计时、JSON/脚本和 FINISH。即使 duo_card 已有两条 NIC，也必须在其分支重跑 SC-B1；通过后再启用 SC-B2。旧 SendRoundAck、sender 主结果、预构造 600 地址不能在合并时复活。

本次阶段 1 实现未操作其它分支或 worktree；完成目标机验证后再由明确任务执行同步。

## 6. 阶段 2：在新阶段 1 上只增加双 NIC direct

### 实现

1. 两端各两个 service，分别绑定不同 NIC/IP，各一个 worker、一个 channel/QP，禁用内部 multirail；remote 各 rail 不同 OOB 端口。
2. 同一逻辑source/dst池分别注册到各rail；local把每rail的dst key交给remote，remote source key留在本机。请求只经rail0一次发送完整600对；按请求索引前/后300项分rail，不根据源偏移分组。
3. 保持 remote 一个提交线程，按块交替向 rail0/rail1 各提交 300 个异步 Put；不先等 rail0 完成再启动 rail1。
4. 每 rail 在自己的数据后发一个 DATA_DONE；local 等齐两条 ready 和请求 Send 完成返回，无 scatter、无 ACK。
5. SC-B1/SC-B2 使用同一二进制、协议、请求格式及阶段 1.5 优化，增加设计文档定义的独立 trace。remote 提交时长只作远端诊断，不取代 local 接口时延。

### 验证与报告

- 两条真实 NIC/QP 的配置和端口计数证明均传约一半 payload，不仅凭 linkCount 或建链日志。
- 单条 rail 人为延迟、跨 rail 通知交错、key 误配/参数不一致、断链有界退出；local 不可提前返回。
- trace 按 generation/rail 汇总 local 请求编码/提交、两 rail ready、返回；remote 请求交接/解析/WR 提交/回收。不同主机时钟不相减。
- 正式运行 SC-B1/SC-B2 各 5 次，交替 case 顺序控制环境漂移；另跑短 trace。报告每次结果及跨 repeat 中位数/范围，不把五次 p99 平均当全样本 p99。
- 双卡收益基于同一新协议单卡；增加的 worker CPU 成本单列。若单 app 提交或请求处理先饱和，解释结果，不默认添加第二提交线程。

交付 `STAGE2_REPORT_CN.md`、双端脚本/配置、原始 JSON/trace、NIC 证据。无硬件时实现可交付，性能结论 pending。

## 7. 阶段 3：SGL 8/16/30 与流水 scatter

### 实现

1. 在SC-B1/B2之外新增stage=614400字节，只在SGL组分配注册；local主计时和600个最终目标地址不变。扩展sparse_copy输入视图，编码前固定`source_format`：`sparse-600`继续传600个源地址，或`grouped`传可验证的源组描述符；wire头必须显式携带该字段。
2. remote根据本次请求构造各chunk iov：sparse-600从K个源地址构造一组；grouped必须按声明的source layout把一个源组唯一展开为最多K个源块。stage目标按全局请求索引连续，相同rkey。一chunk异步PutV后，同QP发CHUNK_DONE。K=30时grouped请求可为20个source地址加600个destination地址；不能从600个任意源地址凭空压缩。
3. 查询实际 QP max_send_sge，K 不足明确 SKIP，未知 pending；不凭常量 30 声称能力，不静默拆分。
4. local CQ 发布 ready；local app 公平检查已就绪 chunk，按请求目标偏移执行 1 KiB memcpy，与后续 RDMA 传输重叠。全部 scatter 后返回，不发 chunk/round ACK。
5. ready generation 每 rail 容量 128；每个在途通知 buffer/iov 生命周期正确，pending 不覆盖 active。off 模式接收相同 chunk 通知，只等全部到齐才 scatter。

### 必做验收

- 9 个主 case：SC-B1/B2、S1/S2 的 K=8/16/30 和 S2-30-off。不支持的 case 有明确硬件依据。
- 单 rail K=16 尾 8；双 rail K=8 尾 4、K=16 尾 12；完整映射、跨代及延迟 scatter 校验。
- 短诊断证明每 chunk groupCount=1、一个 WRITE WR、正确 num_sge/地址/真实 QP，正式测量关闭诊断。
- destination地址始终600个，成功ACK始终0；结果记录source_format、source/destination计数及实际请求字节。grouped的source数与chunk数一致，K=30可为20；sparse-600仍为600。SGL数据WR/完成Send为75/38/20（单rail）、76/38/20（双rail）。
- direct回归无staging/scatter；SGL与direct比整体sparse_copy，可能同时包含请求压缩、WR和scatter变化。on/off必须使用相同source_format，只比较流水时机；要拆分请求压缩，成对比较sparse-600/grouped；要纯WR合并归因才加相同请求的plain-staged。
- local trace 记录请求→首 chunk、ready→scatter 调度、scatter CPU 时间、最后完成；不把重叠区间相加。

交付 `STAGE3_REPORT_CN.md`，支持配置各 5 次正式结果、独立 on/off trace 和 WR 核验证据。

## 8. 阶段 4：WRITE_WITH_IMM 最小改库

1. 保存阶段 3 程序/库基线和证据。在独立可审阅变更中增加 PutVWithImm(req,token,local_done) 和独立接收事件，仅 RDMA worker-poll、连续远端、同 key、单组/单 WR。
2. token 显式贯穿 service/channel→endpoint→worker→QP；upCtxData 的本地上下文语义不变。WR opcode 设 RDMA_WRITE_WITH_IMM，imm_data=htonl(token)，num_sge 仍为 K。
3. 接收 CQ 成功后按 RECV_RDMA_WITH_IMM/ WITH_IMM 分流，ntohl 还原，校验 channel/token/byte_len；不解析 receive buffer 的普通头。
4. 正确回收接收 context 并持续补投 WQE；同 RQ 还承载 COPY_REQ/HELLO/FINISH 等 Send，保持其有缓冲接收，不破坏既有 SEND_WITH_IMM 语义。
5. token 用非零 24 位 generation+8 位 chunk，rail 来自 channel；总轮次限制防回绕。每个 chunk 恰好一个事件，不泛化多组通知。
6. benchmark notify=imm时一次PutVWithImm替换PutV+CHUNK_DONE Send；与对应send case保持相同source_format和请求字节，scatter、local主计时、无成功ACK规则不变。调整本地通知Send callback期望计数，防止等待不存在的Send。
7. 两端程序/库全部重编、记录 ABI/静态库 hash；不支持 backend/input 明确拒绝，旧 Send/PutV/direct 正常。

专项验收：单 chunk 数据和事件、30 SGE 无第 31 项、尾块/byte_len、字节序、长时间接收补投（通知数远超预投量）、普通 Send 与 IMM 共存、错代/重复事件、断链/post 失败的上下文回收。无硬件不能宣称 RNR、DMA 可见性或补投通过。

同一改造库/二进制成对跑 send/imm，每支持配置各 5 次，回归 SC-B1/B2。SC-S2-30：send 为 `1 COPY_REQ + 20 WRITE + 20 Send`，imm 为 `1 COPY_REQ + 20 WRITE_WITH_IMM`；两者 chunk 接收事件=20、memcpy=600、ACK=0。接收开销仍在，不能称为零 CQ/RQ 开销。不夹带 callback 池或单组快路径来归因 IMM。

交付 `STAGE4_REPORT_CN.md`、可审阅库 diff、两端构建与专项验证、真实成对性能和回归。更快不是通过硬条件，正确但无收益也如实报告。

## 9. 阶段报告与可复制任务

每份报告记录：实现/验证状态；源码/diff/程序及库 hash；本阶段变化与比较条件；实际命令/退出码/设备；正确性证据；性能原始路径/重复次数/trace 模式；统计方法；观察事实与尚未验证事项；失败/SKIP 原因；下一阶段输入。未测填“未测”，不填 0。

### 阶段 1 重构任务（当前工作区已实施，供目标机复核）

> 读取 DESIGN_CN.md、IMPLEMENTATION_PLAN_CN.md、PROGRESS.md，在当前 main 重构已存在的 direct B1。实现 local 发起的同步 sparse_copy：每次编码并 Send 9664 字节完整 600 对地址偏移，remote 按请求重新填 WR 并异步 600 Put 直接写最终 dst，同一真实 QP 发 DATA_DONE。local 等数据 ready 和本地请求 Send 完成返回，在 local 入口/返回计时。删除逐轮 ROUND_ACK，下一请求授予复用权限；实现独立 pending/active 和 remote 私有回收，保留 1.5 A+B。建链/MR 在外，编码/解析/WR 构造在内。更新角色为 local/remote（remote 监听）、脚本/配置/schema/self-test/README，先完整请求 smoke 与多轮映射验证，再性能。只实现单卡，不加 staging/scatter/SGL/IMM，不修改 ubs-comm。无硬件标 pending，禁止虚构结果。

### 阶段 1.5 任务

> 在新 sparse-copy-v3 SC-B1 上复核已有 A+B：绑核忙轮询、每 256 次 deadline、正确新线程所有权、共享完成原子和缓存行隔离、ActiveCallbackGuard/部分失败 drain。完整请求成本保留在调用内，callback 分配先保留。同协议 original/A/AB 对照，不能以旧 sender+ACK 协议数字作为分母。交付证据与未测状态，不扩大到双卡/SGL。

### 阶段 2 任务

> 先整合 main 的新 sparse_copy 公共基线并回归 SC-B1，再迁移/实现双 NIC direct。两 service/两真实 QP，每 rail 300 项，单 app 交替提交；rail0 一份完整 600 项请求，每 rail 一个 DATA_DONE，local 等齐返回，无 ACK/scatter。所有主计时在 local，remote trace 只作本机诊断。交付同二进制 SC-B1/B2 对照、独立 trace、两 NIC 流量证据和报告；不把 1.5 或协议切换收益算作双卡收益。

### 阶段 3 任务

> 在新请求/无成功ACK协议上实现SGL 8/16/30：600个destination地址始终完整发送；source_format可为600个分散源地址，或调用方真实提供的聚合源描述符（K=30时20个source）。grouped描述符必须唯一展开对应源块，若需要额外gather/copy则计入调用。按本次请求写连续stage，同QP chunk Send通知，local按目标偏移流水scatter，全部完成才返回。查询实际SGE能力，验证每chunk单WR/尾块/生命周期，保留相同source_format和通知粒度的off。回归direct，记录实际请求长度，报告整体策略和流水收益，不宣称全部为纯gather。

### 阶段 4 任务

> 在阶段3上最小改造ubs-comm的PutVWithImm与接收事件，显式token、WRITE_WITH_IMM、CQ opcode/flags分流和持续RQ补投，保持普通消息协议。只替换chunk Send；成对send/imm使用相同source_format和COPY_REQ字节，local计时、scatter、成功ACK=0不变。先专项正确性，再同库成对send/imm和direct回归。两端全重编，单独报告ABI/库diff和未测项，不夹带其它性能优化。
