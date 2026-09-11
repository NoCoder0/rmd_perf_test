# ubs-comm：600 × 1 KiB RDMA 性能穿刺设计

状态：设计稿，尚未实现测试程序或修改 ubs-comm。日期：2026-09-11。

分阶段实施、跑测脚本契约、阶段验收与模型交接提示词见 [IMPLEMENTATION_PLAN_CN.md](C:/code/RDMA_DEMO/perf_test/IMPLEMENTATION_PLAN_CN.md)。

代码基线：`oneside-msge-merge`，提交 `9e4c035a5d68ccca02d05fade3b6f5907db24ef4`。本文依据当前本地源码，硬件能力、正确性与性能仍需在两台鲲鹏服务器上验证。

## 1. 要解决的问题与设计取舍

用一个独立 C++ 程序，回答三个问题：

1. 600 个分散的 1 KiB 源块，通过单链接与双链接分别写入接收端连续空间，端到端耗时相差多少？
2. 使用当前分支的 SGL 合并，把多个源地址组成一个 RDMA WRITE，取 8、16、30 个地址时，收益和限制是什么？
3. 接收端按 chunk 到达即 scatter，能否覆盖部分 CPU 拷贝时间？以 `WRITE_WITH_IMM` 代替独立通知后还能减少多少开销？

主路径固定为：**分散源地址 → RDMA 写入连续 staging → chunk 就绪通知 → CPU scatter 到分散目标 → ACK**。各组总有效数据都是 614400 字节，即 600 KiB；双链接总共传 600 块，每条传 300 块。

以下对原要求作明确解释：

- “循环跑 600 个小包”默认指循环异步提交 600 次 `Put(1 KiB)`。如果每次同步完成再提交下一次，主要测串行往返等待，不能作为批量吞吐主 baseline。可增加单独的 `serial` 诊断项，但不得混入主表。
- “双链接”定义为两张网卡各一条 RC QP。单 channel 设置 `linkCount=2` 并不能证明用了两张网卡，也会使数据与通知可能轮转到不同 QP。
- SGL 是发送侧 gather。一个 RDMA WRITE 的远端目标仍是一段连续地址；最终分散落点由接收端 CPU 完成。
- 主 baseline 也进行 staging 和 scatter，保证最终完成了相同工作。若另测 600 次直接写入最终分散地址，应标成 `direct`，它省略了 CPU scatter，单列结果。
- Python 负责启动和收集结果。QP 建立、MR 注册、地址交换仍使用 C++ 的 hcom 接口；把这些移到 Python 会增加 FFI 或自建协议，不能明显减少代码。
- 第一阶段采用 **同一真实 QP 上的数据 WRITE + 小型 Send 通知**。当前 hcom 已有接收缓冲、RQ 和 Send 接收回调，代码更少；无需应用轮询 RDMA 写入的内存 flag。
- 首版只允许一轮 600 KiB 在途，使用完整 staging；在轮内按 chunk 流水。无需跨轮 slot、chunk ACK、环形缓冲分配器和自定义线程池。轮次 ACK 同时控制下一轮复用。

这是一项 hcom 端到端穿刺，包括其 API、异步上下文、callback、提交和 CQ 路径。它不等同于只测网卡或 verbs 的峰值。

## 2. 当前代码能直接复用什么

| 代码位置 | 已确认行为 | 对用例的约束 |
|---|---|---|
| [hcom_service_channel.h](C:/code/RDMA_DEMO/ubs-comm/src/hcom/service_v2/api/hcom_service_channel.h:139) | `Put/PutV` 支持 callback；`nullptr` 表示同步调用 | 热路径传非空 callback |
| [hcom_service_def.h](C:/code/RDMA_DEMO/ubs-comm/src/hcom/service_v2/api/hcom_service_def.h:97) | OneSide 请求携带源/目的地址、key、size；SGL 是 iov 数组 | 每个 iov 填 1024 字节，目的地址连续 |
| [service_channel_imp.cpp](C:/code/RDMA_DEMO/ubs-comm/src/hcom/service_v2/service_channel_imp.cpp:352) | `NextWorkerPollEp` 轮转选择 endpoint | 每个 channel 固定一个 endpoint |
| [service_channel_imp.cpp](C:/code/RDMA_DEMO/ubs-comm/src/hcom/service_v2/service_channel_imp.cpp:836) | 异步 Send 通过 worker endpoint `PostSend` | 小通知禁用分片/RNDV，使用相同 channel |
| [service_channel_imp.cpp](C:/code/RDMA_DEMO/ubs-comm/src/hcom/service_v2/service_channel_imp.cpp:2203) | 异步 PutV 走 worker `PostWrite(sglReq)`，使用 key 数组第 0 项 | 每个 service 只包含一个设备，避免内建 multirail |
| [rdma_worker_io.cpp](C:/code/RDMA_DEMO/ubs-comm/src/hcom/transport/rdma/verbs/rdma_worker_io.cpp:446) | 按同 rkey、远端首尾连续的 iov 合并 | 一个 chunk 必须合并为一组 |
| [rdma_verbs_wrapper_qp.h](C:/code/RDMA_DEMO/ubs-comm/src/hcom/transport/rdma/verbs/rdma_verbs_wrapper_qp.h:301) | 每组合成一个 WRITE WR，多个源 SGE，当前全部 signaled | 满 chunk 的 `num_sge` 应是 8/16/30 |
| [hcom_service.h](C:/code/RDMA_DEMO/ubs-comm/src/hcom/service_v2/api/hcom_service.h:219) | 有设备绑定、SQ/RQ/CQ 深度、预投接收与 polling batch 设置 | 直接配置，不另造 verbs 建链层 |
| [service_helper.cpp](C:/code/RDMA_DEMO/ubs-comm/test/hcom/tools/perf_test/test_case/service_v2/service_helper.cpp:79) | 现有 perf 工具示范 Create/Bind/Start/MR/handler | 仅参考初始化，不搬入整个 perf 框架 |
| [service_write_bw_test.cpp](C:/code/RDMA_DEMO/ubs-comm/test/hcom/tools/perf_test/test_case/service_v2/service_write_bw_test.cpp:17) | 现有带宽测试循环异步 Put、每请求建立 callback | 可参考调用方式；需补充远端完成与 scatter 协议 |

当前分支没有完整的 `PutVWithImm` 公共接口。现有底层 `SEND_WITH_IMM` 用于 hcom 消息协议，不等于支持 `RDMA_WRITE_WITH_IMM`。

## 3. 最小实现边界

计划文件如下，本次仅创建本文：

```text
perf_test/
  DESIGN_CN.md            本设计
  rdma_600.cpp             后续：server/client 共用一个 C++ 程序
  CMakeLists.txt           后续：链接当前 ubs-comm 的构建产物
  run.py                  后续：标准库 subprocess，在每台主机启动本地角色并保存结果
```

不新增通用 benchmark 框架，不复制 memfabric 抽象，不做自动重连、动态负载均衡、通用内存池、Python 数据面或自定义 RDMA QP 管理。

首版保留必要的超时、错误退出、数据校验、计数和资源生命周期处理。每个失败 case 返回非零，不能输出看似有效的性能数字。

程序结构可以保持为：`setup → exchange → verify → warmup → measure → drain → teardown`。控制消息和数据测试均复用现有 hcom channel，不另建 TCP 协议。

## 4. 测试矩阵与准确的工作量

`plain` 表示每块一次 `Put`；`sgl` 表示每个 chunk 一次 `PutV`。`chunk_items` 同时决定一次就绪通知对应多少块，以及一次 scatter 的块数。

主 baseline 的 `chunk_items=30`，便于和 SGL=30 做完全相同通知粒度的比较。baseline 仍然是循环提交 600 次独立 1 KiB 写入，未将其合并为 SGL。

| case | 链接数 L | API | chunk_items | scatter | 数据 WR/轮 | DATA_READY 通知/轮 |
|---|---:|---|---:|---|---:|---:|
| B1 | 1 | 600 次 Put | 30 | 到达即执行 | 600 | 20 |
| B2 | 2 | 每条 300 次 Put | 30 | 到达即执行 | 600 | 20 |
| S1-8 | 1 | PutV | 8 | 到达即执行 | 75 | 75 |
| S1-16 | 1 | PutV | 16 | 到达即执行 | 38 | 38 |
| S1-30 | 1 | PutV | 30 | 到达即执行 | 20 | 20 |
| S2-8 | 2 | PutV | 8 | 到达即执行 | 76 | 76 |
| S2-16 | 2 | PutV | 16 | 到达即执行 | 38 | 38 |
| S2-30 | 2 | PutV | 30 | 到达即执行 | 20 | 20 |
| S2-30-off | 2 | PutV | 30 | 所有 chunk 就绪后执行 | 20 | 20 |

计算规则：每条 `Nrail=600/L`，`chunks_per_rail=ceil(Nrail/chunk_items)`。第 c 块的有效地址数 `min(chunk_items, Nrail-c*chunk_items)`。

- 单链接：SGL=8 无尾块；16 的最后一个 chunk 是 8 个地址；30 无尾块。
- 双链接：SGL=8 每条最后一个 chunk 是 4 个地址；16 每条是 12 个地址；30 无尾块。
- `WRITE+Send` 阶段的数据方向发送 WR 数为表中两列之和，例如 B1 为 620，S2-30 为 40，S2-8 为 152。
- 接收端每轮每条链接额外回一个 `ROUND_ACK`：单链接 1 个，双链接 2 个。以上均不含建链、校验控制、心跳和硬件重传。
- 地址块数量不是线上的网络报文数量；MTU 分包、协议头和 ACK 会另行影响端口计数。

`S2-30-off` 仍然接收同样的 20 个 chunk 通知，只推迟 scatter，保证流水开关仅改变消费时机。**不要把 off 改成仅发送一个轮次通知，否则无法区分流水和通知数量的影响。**

若要严格拆分 SGL=8/16 的 gather 收益与通知粒度影响，按需增加 `plain + chunk_items=8/16` 的对应项。首轮主矩阵不全部展开；8/16/30 的主表代表三种实际 chunk 配置的综合效果。

`serial`、`direct`、双提交线程属于诊断项，遇到相应瓶颈再运行，不作为首版必做功能。

## 5. 连接、线程与 NUMA

两端分别创建 L 个 service，每个 service：

```text
RDMA transport
SetDeviceIpMask({local_rdma_ip + "/32"})
SetMultiRailOptions(enable=false)
workerGroupThreadCount=1
workerGroupMode=NET_BUSY_POLLING
ConnectOptions.linkCount=1
ConnectOptions.mode=WORKER_POLL
ConnectOptions.cbType=CHANNEL_FUNC_CB
```

每个 service 名称唯一，例如 `rdma600_sender_0`。接收端每个 service 绑定不同 OOB 端口。OOB TCP 地址可以是管理网地址；它和实际 RDMA 网卡选择是不同配置。

实际数据路径：

```text
sender service[0] / QP0 / NIC0  ── receiver service[0] / QP0 / NIC0
sender service[1] / QP1 / NIC1  ── receiver service[1] / QP1 / NIC1
```

首版发送端一个应用提交线程，按 chunk 在两条 rail 间轮流提交；不能先等 rail0 全部完成再提交 rail1。接收端一个应用 scatter 线程扫描两条 rail 的就绪状态；每条 rail 的 hcom worker 只处理 CQ 与轻量 callback。

这样无需自定义线程池，单/双链接保持相同应用线程数。双链接仍多一个 hcom worker，应在结果中记录总 CPU 使用量。若发送线程先达到瓶颈，双链接不一定更快；此时再做每 rail 一个提交线程的诊断实验，避免直接把变化全部归因于网卡。

CPU 绑定规则：应用线程和 CQ worker 使用不同物理核心；分别记录网卡 PCIe NUMA 节点、线程 CPU 和源/staging/destination 内存 NUMA 节点。同 NUMA 双网卡时优先全部放本地；跨 NUMA 时保持各 case 一致并明确记录接收端单 scatter 线程的远端内存访问代价。不在首版隐藏加入 NUMA 自动调度。

## 6. 内存布局、描述符与初始化

以接收者为 local，发送者为 remote；代码使用 sender/receiver 避免“远端”随角色改变产生混淆。

```text
N = 600; B = 1024; stride = 4096; L = 1 or 2
Nrail = N / L

每条 rail 的 sender：
  src：Nrail * stride，源块 i 位于 src + i*stride
每条 rail 的 receiver：
  stage：Nrail * B，块 i 位于 stage + i*B
  dst：Nrail * stride，最终块 i 位于 dst + i*stride

全局块编号 global_i = rail*Nrail + i
```

每条 rail 一次申请、一组 MR 生命周期；源地址之间留间隔，确保实际使用多个地址。staging 必须注册，`dst` 只由 CPU 使用，不必注册。用 `posix_memalign` 或等价页对齐分配，提前触页，所有分配与注册在计时前完成。

每个 MR 的 `UBSHcomMemoryKey` 先清零再 `GetMemoryKey`。按对应 service 导出并交换完整 key，不自行把裸 verbs lkey/rkey 填入 hcom 的高层结构，也不跨 service 混用 MR key。

预生成并复用所有 `UBSHcomOneSideRequest` 与每个 chunk 的 SGL 视图：

```cpp
for (rail = 0; rail < L; ++rail) {
    for (i = 0; i < Nrail; ++i) {
        req[rail][i] = {
            .lAddress = src[rail] + i*4096,
            .rAddress = peer_stage[rail] + i*1024,
            .lKey = local_key[rail],
            .rKey = peer_stage_key[rail],
            .size = 1024
        };
    }
    for (c = 0; c < chunks_per_rail; ++c) {
        chunk[c].begin = c * chunk_items;
        chunk[c].count = min(chunk_items, Nrail - chunk[c].begin);
        chunk[c].sgl = {&req[rail][chunk[c].begin], chunk[c].count};
    }
}
```

以上及后续均为伪代码，不保证可直接编译。真正实现需遵守当前类型和构造函数签名。

首版不预先把源数据打包为连续 30 KiB；否则测不到 NIC gather 的价值。总源有效数据为 600 KiB，源与目标 stride 空间各约 2.34 MiB，staging 总共 600 KiB，内存容量很小。

## 7. 控制面与启动时序

复用 hcom 的 `Call/Reply` 做 MR/配置交换，复用 `Send` 做 chunk 通知与 ACK。控制消息使用明确 opcode：`HELLO`、`READY`、`DATA_READY`、`ROUND_ACK`、`FINISH`。不直接序列化指针持有的 C++ 对象。

握手交换：协议版本、case 参数、rail 编号、缓冲区大小、staging 基地址、完整 memory key、代码版本标识。固定字段采用显式编码；两端参数不一致立即拒绝测试。

```text
receiver：注册 handler → Bind/Start → 申请/注册/触页/初始化内存
sender：  Start → 申请/注册/初始化 src → Connect 每条 rail
sender：  HELLO/Call，获取每条 rail 的 staging 元信息
receiver：只有内存和就绪状态初始化完成才允许返回 READY
sender：  所有 rail READY，预生成描述符，然后开始验证和预热
```

某一条 rail READY 不代表整组 READY。连接建立后不得再清零正在接收数据的 staging；所有代际状态从 generation=1 单调递增，验证、预热、测量切换时不重置 generation。

Python 包装最小职责：receiver 主机以 `--role receiver` 启动本地 receiver 并实时输出 `LISTENING`；确认后，sender 主机以 `--role sender` 启动本地 sender。两端各自等待退出并保存本机 JSONL/stdout/stderr。`LISTENING` 只是进程编排信号，真正数据面 READY 由 C++ 协议保证。

不需要 Python 交换 QPN、PSN、GID 或 MR key，也不需要 Python 每轮发消息。可直接在两个终端手工启动 C++ 程序，结果应与 Python 包装一致。

## 8. 第一阶段通知协议：WRITE + Send

每个 chunk 提交顺序：

```text
plain：Put(block0), Put(block1), ... Put(blockK-1), Send(DATA_READY)
sgl：  PutV(K 个 iov，合为一个 WRITE WR), Send(DATA_READY)
```

所有调用异步，同一应用线程提交到该 rail 唯一 QP。提交数据后即可追加通知，不必等发送端数据 callback 才发送通知。

依赖条件是标准 RC QP 的有序操作、该 channel 唯一真实 QP，以及正常的 verbs 完成/内存可见性语义。不启用改变写入顺序的 relaxed ordering 扩展。接收端看到该 QP 的成功 Send 接收完成后，才发布对应 staging 的就绪状态。两条 QP 之间没有顺序保证，分别处理。

在两台鲲鹏机器上必须先以唯一数据模式验证这条路径；C++ `release/acquire` 用于 CQ 线程与 scatter 线程之间的状态发布，不能拿它代替设备 DMA 完成语义。若无法确认实际 QP 映射或后续代码改变路由，停止使用这个顺序假设，诊断时可改为等待该 chunk 数据完成后再 Send，并单列其额外等待开销。

建议控制消息固定为 24 字节，不计 hcom 自身头部：

```cpp
// wire 字段须显式编码，不依赖结构体隐式 padding
struct Notice {
    uint64_t generation;
    uint32_t chunk_id;
    uint32_t byte_count;
    uint16_t rail;
    uint16_t item_count;
    uint32_t reserved;      // 写 0，消息类型由 hcom opcode 区分
};
```

chunk 起始地址由参数和 chunk_id 推导，不在每个通知里重复传地址。接收时校验长度、rail/channel 对应关系、chunk_id、item_count 和 byte_count。

每条 rail 所有 chunk scatter 完成后，发送一个 `ROUND_ACK(generation, rail)`。ACK 只用于声明该 rail 本轮最终目标已可用；ACK 到达前，sender 不得开始复用这轮 staging。

为何不采用独立 flag WRITE：它虽然也可在同一 QP 追加，但需要额外 MR 地址、源 flag 生命周期、CPU 轮询及 DMA 可见性约定；当前 hcom 已有 Send 接收通路，首版无需增加这些工作。它可以是后续专门比较的通知方式，不与首版混用。

## 9. 最小流水状态与伪代码

首版 `rounds_in_flight=1`，不用 slot。每条 rail 至多 75 个 chunk，静态容量可取 128。

```cpp
struct RailState {
    atomic<uint64_t> ready_generation[128]; // CQ worker 写，scatter 线程读
    atomic<uint64_t> ack_generation;        // sender 的接收 callback 写
    atomic<uint64_t> data_done_total;       // sender 的本地写 callback 累加
    atomic<uint64_t> send_done_total;       // 本端通知/ACK 发送 callback 累加
};
atomic<int> fatal_error;
```

累计计数不每轮清零，提交线程维护累计期望值。所有状态和计数预分配。generation 标记无需在消费后写回 0，减少双方同时重置的竞态。

接收 CQ callback：

```cpp
OnDataReady(ctx) {
    if (ctx.Result() != OK) return Fail(ctx.Result());
    Notice m = DecodeAndCopy(ctx);       // callback 返回后不保留 ctx/消息指针
    ValidateNoticeAgainstCaseAndChannel(m);
    old = ready_generation[m.chunk_id].load(relaxed);
    if (m.generation != old + 1) return Fail(PROTOCOL_ERROR);
    ready_generation[m.chunk_id].store(m.generation, release);
    return OK;                          // 不 memcpy，不等待 ACK，不同步 Send
}
```

`ready_generation` 初始为 0，每个有效 chunk 每轮恰好收到一次通知。固定 case 内 chunk 划分不变。先到的下一轮通知允许发布，即使应用线程还在等待上一轮 ACK 的本地发送完成；不会因此错误清零或丢失通知。

发送端每轮：

```cpp
RunSenderRound(generation) {
    // 上轮已收齐 ACK，且本地数据/通知 callback 都已完成
    t0 = now();
    for (c = 0; c < chunks_per_rail; ++c) {
        for (r = 0; r < L; ++r) {        // 两条 rail 交替推进
            chunk = chunks[r][c];
            if (mode == PLAIN) {
                for (i : chunk.items) {
                    ++expected_data_done[r];
                    Check(ch[r]->Put(req[r][i], NewDataDoneCallback(r)));
                }
            } else {
                ++expected_data_done[r]; // 一次 PutV 的 API 完成
                Check(ch[r]->PutV(chunk.sgl, NewDataDoneCallback(r)));
            }
            FillPersistentNotice(r, c, generation);
            ++expected_send_done[r];
            Check(ch[r]->Send(notice_req[r][c], NewSendDoneCallback(r)));
        }
    }
    t_submit = now();
    while (!AllAckAtLeast(generation) || !AllLocalCallbacksDone()) {
        CheckFatalAndDeadlinePeriodically();
        // hcom worker 独立处理 CQ；这里不使用其 self-poll 接口
    }
    t_end = now();
    Record(t_submit-t0, t_end-t0);
}
```

callback 只检查 `Result()`，然后对相应累计计数 `fetch_add`，错误写入 `fatal_error`。所有 API 返回值同时检查；API 接受请求、发送端完成、接收端 scatter 完成是三个不同事件。

接收端应用线程每轮：

```cpp
RunReceiverRound(generation) {
    next_chunk[0..L-1] = 0;
    ack_posted[0..L-1] = false;
    if (scatter_mode == AFTER_ALL) {
        WaitUntilEveryChunkReady(generation); // 期间 CQ worker 持续工作
    }
    while (!AllRailsAckPosted()) {
        progressed = false;
        for (r = 0; r < L; ++r) {
            c = next_chunk[r];
            if (c == chunks_per_rail) continue;
            if (ready[r][c].load(acquire) < generation) continue;
            chunk = chunks[r][c];
            for (i : chunk.items)
                memcpy(dst[r] + i*4096, stage[r] + i*1024, 1024);
            // verify 模式在此完整验证对应数据；measure 模式不逐块校验
            ++next_chunk[r];
            progressed = true;
            if (next_chunk[r] == chunks_per_rail) {
                FillPersistentAck(r, generation);
                ++expected_ack_send_done[r];
                Check(ch[r]->Send(ack_req[r], NewAckDoneCallback(r)));
                ack_posted[r] = true;
            }
        }
        CheckFatalAndDeadlinePeriodically();
    }
    WaitForOwnAckSendCallbacks(); // ACK 消息缓冲复用前必须完成
}
```

每次扫描每条 rail 最多消费一个 chunk，避免慢 rail 阻塞快 rail，也避免 rail0 长时间独占 scatter 线程。同一 rail 按 chunk 顺序消费，不需要复杂就绪队列；只有单个生产 worker，状态数组已足够。

一次典型流水如下；具体是否重叠到足够多时间取决于实测，不能只因采用此结构就声称获得加速：

```text
NIC : [write c0][notify0][write c1][notify1][write c2][notify2] ...
CPU :                  [scatter c0]       [scatter c1]       ...
```

**缓冲和对象生命周期：**源数据保持到本地写完成；notice 保持到本地 Send 完成；staging 保持到对应 scatter 完成；ACK 保持到自身 Send 完成；MR、channel、状态对象保持到全部操作 drain 后。最后一轮结束仍执行 FINISH 握手并 drain，随后断链/停服务，按库要求销毁 MR 和服务，不能在 worker 仍有 callback 时释放状态。

### callback 的精简边界

第一版使用公共 `UBSHcomNewCallback`，每请求一个自删除 callback，与当前 service API 示例一致。请求描述符和消息缓冲全部预分配；callback 分配及 hcom timer/context 管理开销保留在测量中。

不直接把栈上 callback 传给 hcom，也不共享一个可被删除的 callback 给多次请求。当前异步错误路径可能删除 callback，应用不能在任意失败后再无条件 `delete` 同一指针。请求失败后记录错误并终止该次测试，具体资源回收按各 API 的实际所有权路径处理。

若 profile 证实 callback 分配占主导，再单独设计 callback 池或库内轻量异步接口，并让各组使用相同版本重新测量。首版不为了减少表面行数而依赖内部 `gEmptyCallback` 或切换全局 callback 模式绕过正常完成管理。

## 10. 参数与队列预算

| 参数 | 首版值 | 原因/约束 |
|---|---|---|
| blocks / block_bytes | 600 / 1024 | 固定有效数据量 |
| src_stride / dst_stride | 4096 / 4096 | 明确模拟分散块 |
| links | 1、2 | 一张 NIC 一个 service/channel/QP |
| sgl_items | 8、16、30 | 实际 QP 能力不足则 SKIP，不能静默降级 |
| plain chunk_items | 30 | 与 SGL=30 同通知粒度 |
| rounds_in_flight | 固定 1 | 完整 staging，轮内流水，无 slot 协议 |
| scatter | pipeline / after-all | 主表使用 pipeline，增加一个关闭项 |
| application sender/scatter threads | 各 1 | 降低实现和比较变量 |
| worker threads | 每 rail 1 | busy poll |
| SQ requested depth | 1024 | 单 rail plain 最多 600+20 个数据方向 WR/轮 |
| RQ requested depth | 256 | 控制消息与通知接收 |
| preposted receive | 128 | 单 rail SGL=8 最多 75 次通知/轮，另留控制余量 |
| CQ requested depth | 2048 | 容纳当前全 signaled 数据完成与接收完成；记录实际配置 |
| polling batch | 16 | 初始统一值；需要时单独扫 4/16/32 |
| service ctx store capacity | 保持库默认 2097152 | 各组一致；timer 对象回收可能滞后于 IO 完成，不按单轮 WR 数贸然缩小 |
| maxSendRecvDataSize | 1024 | 容纳小控制消息及库头部需求；不限制 RDMA WRITE 数据长度 |
| split / RNDV | 禁用 | 小通知必须走普通 Send |
| verify rounds | 20 | 带代际的完整数据校验，独立于性能统计 |
| warmup rounds | 1000 | 清除建链/触页等一次性影响 |
| measure rounds | 10000 | 各 case 相同；运行过短时包装脚本增加轮数 |
| independent repeats | 5 | 每次新进程，报告中位数与波动 |
| application progress timeout | 10 秒 | 用低频时钟检查，失败即退出 |
| hcom operation timeout | 5 秒 | 以当前 API 秒单位设置，并与应用 deadline 协调 |
| generation | uint64，初始 1 | 不随 warmup/measure 重置 |

SQ/RQ/CQ 表内是请求配置，实际创建值可能由 hcom 调整、受设备限制。不得仅凭设置值声称拥有对应硬件资源。记录设备 `max_sge`、实际 QP `max_send_sge`、队列大小和 active MTU；如果库暂未暴露实际 QP capability，可利用查询/诊断路径核实，不能把创建前日志当创建后结果。

当前 group 合并上限直接使用 30，尚未按实际 QP 的能力拆分。因此 SGL=30 是有前置条件的用例；能力不足则输出 `SKIP: max_send_sge < 30`。若今后允许自动拆分，应另报真实 WR 数，不能再称“一次 30 SGE WR”。

WR 数核验使用独立短运行：在 `PostOneSideSglGrouped` 提交前通过调试器或临时诊断计数观察 `groupCount=1`、`wr.num_sge`、目标地址和 QP；性能运行关闭这些诊断。仅从应用 PutV 次数推导的数值标为 expected，未经核验不能标为 measured。

第一版全 signaled，保持当前实现。暂不引入 selective signaling，它会改变 callback、上下文回收与队列信用协议，超出最小穿刺范围。

## 11. 正确性验证与性能口径

### 正确性：先验证，再计时

验证轮次将每个 64 位 word 填为由 `(generation, global_block_id, word_index)` 计算的确定值，保证完整 1 KiB 内容都参与校验。填充在验证阶段，不进入正式性能统计。

receiver 初始化 stage、dst 及间隙为不同哨兵；收到通知后 scatter，再检查全部 600 个块、块内所有 word 和应保持不变的间隙。发现错误报告 rail、generation、block、word、expected/actual 并失败退出。仅检查第一个字节不足以验证尾部、块偏移和跨轮覆盖。

额外的小规模验证：人为延迟 receiver scatter，确认 sender 不会提前开始下一轮；双链接人为延迟其中一条，确认另一条仍可 scatter；覆盖 8/16 的尾块；结束时确认所有本地 callback 和 ACK 计数匹配。失败注入仅用于验证运行，不在性能测量开启。

正式性能轮次使用提前准备好的稳定内容，禁止每轮生成 600 KiB 数据或完整扫描校验。结束后完整检查最终目标和哨兵；这验证最终状态，不能宣称每一个测量轮次均做了完整内容验证。每轮通知 generation、完成数量和错误码仍严格检查。

### 计时：全部使用本机单调时钟

发送端每轮记录：

- `submit_us`：首个数据 API 调用前，到最后一个 chunk 通知提交后。包含 hcom API、callback 创建和遇到的内部背压。
- `e2e_us`：首个数据 API 调用前，到收齐所有 rail 的 scatter ACK，且本地数据/通知 callback 完成。它是本用例的主完成口径。
- 可选诊断 `local_write_done_us`：所有数据 API callback 完成的时刻；不代表接收端 scatter 完成，不能用来替代 `e2e_us`。正式最小路径可不采集此项，避免增加 callback 时间戳开销。

接收端在独立诊断运行记录 `scatter_cpu_us`（所有 memcpy 区间之和）及首个 chunk 就绪到最后一个 scatter 完成的本机区间。正式计时默认不在每个 1 KiB memcpy 周围读时钟。不用未同步的两台机器时间戳直接相减。

```text
e2e_p50/p95/p99：measure 轮次的 e2e_us 分位数
effective_GBps = measure_rounds * 614400 / measured_wall_seconds / 1e9
block_Mops    = measure_rounds * 600 / measured_wall_seconds / 1e6
speedup       = reference_e2e_median / candidate_e2e_median
```

`measured_wall_seconds` 覆盖整个正式循环，包括轮次之间的调度、统计数组写入和 ACK 等待。分位数排序、JSON 输出都在计时结束后。数组提前分配；不逐轮打印日志。

主带宽是 **单轮在途、包含 scatter/ACK 的应用有效带宽**，不能标成 NIC 峰值。若 ACK 栅栏使端口明显空闲，再增加跨轮窗口实验；该扩展需要多套 staging/dst、slot 与 credit，本次先不实现。

报告每个 case 的有效字节、API 次数、预计及核验的数据 WR 数、通知数、ACK 数、CPU 利用率、版本、网卡/MTU/NUMA/绑核信息。硬件端口字节作为辅助，包含协议开销，不能直接替代有效数据带宽。不要期待双链接必然 2 倍；CPU 提交、scatter、PCIe、NUMA 和端口共享资源都可能先饱和。

## 12. 第二阶段：最小改造 WRITE_WITH_IMM

保留第一阶段所有数据布局、轮次 ACK、scatter 逻辑与计时口径，只替换 chunk 的就绪通知。先针对 SGL 用例增加能力，不在本次目标中顺带扩展 GetV、UB、UMQ 等后端。

### 12.1 最小 API 语义

拟新增 C++ 接口，名称为设计占位，当前不存在：

```cpp
int32_t PutVWithImm(const UBSHcomOneSideSglRequest &req,
                   uint32_t token_host_order,
                   const Callback *local_done);

RegisterWriteImmHandler(handler(channel, token_host_order, byte_len));
```

新 API 要求非空本地 callback、RDMA worker-poll 路径。首版明确限制：`iovCount <= actual max_send_sge`、所有目标连续、相同 rkey，一次调用对应一个数据 WR、一个接收通知。不满足条件返回参数/不支持错误；先不实现一个调用对应多组通知的泛化语义。

不往现有公共请求结构里直接塞字段，不借用已有 `upCtxData` 的 service seqNo 传远端 token。token 必须作为独立参数逐层传递。增加虚函数同样涉及 C++ ABI，两个端点、库和用例全部用新版本重编；第一阶段二进制保留以便对照。用例只使用 C++，这一阶段不必同时增加 C 包装。

### 12.2 sender 的底层变化

调用链：service API → channel → RDMA endpoint → worker 请求 → QP 的 grouped post。

在已经构造好的唯一 WR 上：

```cpp
wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
wr.imm_data = htonl(token_host_order);
wr.sg_list = data_sges;
wr.num_sge = chunk.count;           // 8/16/30 或尾块实际数量
wr.wr.rdma.remote_addr = remote_chunk_begin;
wr.wr.rdma.rkey = remote_key;
wr.send_flags = IBV_SEND_SIGNALED;
```

32 位 immediate 是操作携带的元数据，不是第 31 个 SGE，不写入 staging，也不占 staging 的一个数据槽。发送端完成仍按本地 RDMA write 完成路径回收上下文；它不表示 scatter 已完成。

若以后放宽成多 WR 的 PutV，可定义只有最后一个 WR 带 immediate，利用同一 QP 顺序通知整个调用；那时接收 CQ 的 `byte_len` 仅属于带 immediate 的那个 WRITE，不能当整个 PutV 的总长度。本次严格单 WR 约束避免这项歧义。

### 12.3 receiver 必须新增的处理

当前 [rdma_worker_core.cpp](C:/code/RDMA_DEMO/ubs-comm/src/hcom/transport/rdma/verbs/rdma_worker_core.cpp:255) 按接收 context 类型分发，随后 [net_rdma_driver_oob.cpp](C:/code/RDMA_DEMO/ubs-comm/src/hcom/transport/rdma/verbs/net_rdma_driver_oob.cpp:1645) 根据 immediate 是否为零进入普通/RAW 消息解析。WRITE_WITH_IMM 的数据在 staging，不在接收 WQE 的消息缓冲，不能复用该消息解析。

因此需在成功 CQE 上首先识别 opcode：

```cpp
if (wc.status != IBV_WC_SUCCESS) {
    ExistingErrorAndContextCleanup(wc);
} else if (wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
    Check(wc.wc_flags & IBV_WC_WITH_IMM);
    Event ev = {ChannelOfReceiveContext(wc.wr_id),
                ntohl(wc.imm_data), wc.byte_len};
    RePostReceiveOrFail();          // 使用原接收 context/缓冲，先复制 event
    PublishWriteImmEvent(ev);       // 不读取 receive buffer 中的消息头
} else {
    ExistingCompletionDispatch(wc);
}
```

真实实现必须同步处理已有 op context 移除/重新挂入、QP 引用与接收 WQE 计数，避免双重回收；以上伪代码只表示分流次序。callback 只发布 ready generation，不直接 scatter。

需要接收 WQE 信用：每个 WRITE_WITH_IMM 消耗一个接收 WQE，缺少时会触发 RNR/重试甚至超时。沿用当前 hcom 预投的有缓冲 receive WQE，因为同一 RQ 还承载 HELLO、ACK、FINISH 等普通 Send。该缓冲承载普通 Send；WRITE_WITH_IMM 数据落点仍由 remote_addr/rkey 指定。不在混合消息的 RQ 上盲目改成零 SGE 接收。

当前旧 SEND 协议对 immediate 有自己的解释。新分支单独做 `ntohl`，不要全局改变旧 RAW 消息 immediate 的编码，否则会破坏旧消息路径。

### 12.4 最小 token，不引入 slot

只有一轮在途、最多 75 chunk，因此 token 可用：

```text
31                 8 7                 0
+-------------------+-------------------+
| generation:24 bit | chunk_id:8 bit    |
+-------------------+-------------------+

token = (generation << 8) | chunk_id
```

rail 由接收 channel 得到，无需重复占 token 位；协议类型由 CQ opcode 得到；chunk 有效块数由配置推导。没有跨轮 slot，因此不为 slot 预留字段。

generation 从 1 开始。单进程包含验证/预热/测量的总轮数必须小于 `2^24`，达到上限则在运行前拒绝参数，首版不处理回绕。若以后跑长时多窗口测试，再扩展代际协议。

接收端检查 chunk 范围、generation 递增以及 `byte_len == chunk.count*1024`，然后复用第一阶段 `ready_generation`。每轮每条 rail 的 scatter ACK 保持不变。

### 12.5 对比与最小修改范围

在同一份改造后的库中同时保留 `notify=send` 和 `notify=imm`，同样的 SGL/链接/队列/线程/计时配置对比，再与原始库的 send 结果核对回归。

S2-30 的数据方向从 20 个 WRITE + 20 个 Send 变为 20 个 WRITE_WITH_IMM；接收侧仍然有 20 次就绪 CQE，仍做 600 次 memcpy，仍发 2 个 ACK。预期减少通知 WR、Send API/callback 和消息处理开销，不能声称消除了接收 CQ 或 scatter 开销。

最小修改区域：公共 C++ 声明与 handler 类型、service/channel 实现、内部单边 SGL 请求元数据、RDMA endpoint/worker/QP 传递、接收 CQ opcode 分流与事件上送。旧 PutV 和 Send 保持原行为；不支持的 transport 明确返回 unsupported。测试至少覆盖单/双 rail、8/16/30/尾块、普通 Send 与 immediate 共存、接收补投和错误清理。

## 13. 当前分支风险对用例的影响

1. **硬件实际 max_send_sge。** 当前设备限制与 group=30 常量可能不一致。用例先核实能力并跳过不支持的 case；后续库改造应在 post 前校验实际 QP cap。
2. **部分提交失败。** 当前 grouped post 对 `bad_wr` 的处理不完整，失败时上层会归还整批上下文。本设计每次 PutV 恰好一组/一个 WR，避免多组 WR 的成功前缀场景，但不代表通用问题已修复。若后续扩大输入范围，必须修复此路径。
3. **callback 与超时。** 当前错误路径存在库内部删除 callback 的行为；不得依照示例简单套用“失败后一律应用 delete”。测试首版直接失败退出，不做重试恢复。库内部资源紧张可能出现等待/重试，计时应包含它，并将相关错误视为无效结果。
4. **QP 和设备映射。** `/32` 过滤、唯一 service 设备、`linkCount=1` 和关闭 multirail 是必要约束；启动时核实实际设备，双链接用端口计数验证两条路径都有流量。
5. **调试日志。** 当前分支数据路径有多处日志调用。性能构建关闭 debug/trace 输出，但保留错误输出；记录构建配置，不能一个 case 开日志另一个关日志。
6. **stack/context 成本。** 当前 grouped QP 路径为最多 30×30 SGE 的数组清零，即使本例只有一组；先测当前真实成本。若 profile 显示明显，再做单组快路径，作为独立优化对比，不与 WRITE_WITH_IMM 的收益混在一次变更中。

这些是测试解释与最小输入约束，不要求在实现首版用例前先重构整个 ubs-comm。

## 14. 运行接口、结果格式与实施顺序

以下命令为计划接口，当前不存在可执行文件：

```bash
# receiver：两张网卡的本机 RDMA IP 分别绑定两个 service
./rdma_600 --role receiver --links 2 \
  --rdma-ips <receiver_nic0_ip>,<receiver_nic1_ip> \
  --listen <receiver_oob_ip>:19000,<receiver_oob_ip>:19001 \
  --mode sgl --sgl 30 --scatter pipeline --notify send \
  --app-cpu <scatter_cpu> --worker-cpus <cq0_cpu>,<cq1_cpu>

./rdma_600 --role sender --links 2 \
  --rdma-ips <sender_nic0_ip>,<sender_nic1_ip> \
  --peer <receiver_oob_ip>:19000,<receiver_oob_ip>:19001 \
  --mode sgl --sgl 30 --scatter pipeline --notify send \
  --app-cpu <submit_cpu> --worker-cpus <cq0_cpu>,<cq1_cpu> \
  --verify-rounds 20 --warmup 1000 --rounds 10000
```

plain 参数使用 `--mode plain --chunk-items 30`；单链接只提供一组 IP、端口和 worker CPU。两端交换并校验最终解析出的参数，避免脚本漏传导致不同配置运行。

结果每 case 输出一行 JSON，建议字段：

```json
{
  "case": "S2-30", "status": "ok", "commit": "9e4c035...",
  "links": 2, "blocks": 600, "block_bytes": 1024,
  "mode": "sgl", "chunk_items": 30,
  "scatter": "pipeline", "notify": "send", "rounds_in_flight": 1,
  "data_wr_per_round": 20, "notify_wr_per_round": 20,
  "ack_wr_per_round": 2, "measure_rounds": 10000,
  "e2e_p50_us": null, "e2e_p95_us": null, "e2e_p99_us": null,
  "submit_p50_us": null, "effective_GBps": null,
  "verify_passed": true
}
```

示例中的 null 是尚未实测的占位，不是结果。设备、NUMA、队列、MTU、编译和 CPU 元信息可输出一次独立 JSON，避免每行重复大量内容。

实施顺序：

1. 实现一个 C++ 程序的单 rail plain、HELLO/READY、chunk Send、scatter、轮次 ACK 和完整校验；先通过验证再采性能。
2. 复用相同结构扩展到双 rail；确认总数仍是 600、网卡映射正确、没有跨 QP 顺序依赖。
3. 增加 PutV 分支与 8/16/30 尾块处理，运行主矩阵及 scatter-off；核验一个 chunk 实际只产生一个数据 WR。
4. 用短验证运行或独立诊断记录确认“早期 chunk scatter 发生在本轮最后 chunk 到达前”；如果没有重叠，报告事实并定位提交/调度瓶颈。
5. 加 Python 包装完成重复运行和结果归档；最初可完全手工双端运行。
6. 在独立代码变更中增加最小 PutVWithImm 路径，复用相同 benchmark 做 Send/IMM 对照与旧路径回归。

验收不预设加速倍数：要求所有支持的 case 数据正确、无超时/队列错误、WR/通知数符合设计、两条 NIC 确实参与、统计口径一致，能明确说明时间消耗在提交、传输、scatter 还是 ACK 栅栏。

参考语义资料：[rdma-core ibv_post_send](https://github.com/linux-rdma/rdma-core/blob/master/libibverbs/man/ibv_post_send.3)、[rdma-core verbs 类型与完成定义](https://github.com/linux-rdma/rdma-core/blob/master/libibverbs/verbs.h)、[NVIDIA RDMA 编程手册](https://docs.nvidia.com/rdma-aware-networks-programming-user-manual-1-7.pdf)。设备相关能力以实际鲲鹏机器的网卡、驱动/provider 和 QP 查询结果为准。
