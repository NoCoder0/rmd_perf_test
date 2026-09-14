# ubs-comm requester-driven sparse_copy：双 rail direct 设计

更新日期：2026-09-14。当前实现覆盖 direct B1/B2；SGL、staging、scatter 和 IMM 不在本次范围。目标 Linux/RDMA 验证仍为 HW_PENDING。

## 语义与计时

local 是调用者、最终 destination 拥有者和唯一主计时端；remote 是 source 拥有者和 RDMA WRITE 发起端。remote 监听，local 连接。建链、MR 注册和 key 交换在调用外；每次调用内包含 600 对 offset 的生成、校验、编码和 COPY_REQ 传输，remote 接收复制/解析、本轮 WR 构造、600×1024 回写，以及 local 数据 ready。

成功路径无逐轮 ACK。下一 COPY_REQ 表示 local 已消费上一代并授予 remote 下一代写权限。remote 仍必须回收本代 Put/DATA_DONE callbacks，才能复用 WR、source 验证数据和通知缓冲。

## wire

协议名 `sparse-copy-v4-dual-rail`，version=4；HELLO/READY/COPY_REQ/DATA_DONE/FINISH 均使用新 magic，避免旧 sender-driven v3 被接受。

direct COPY_REQ 固定 9664 字节：64 字节头和 600 个 `(remote_source_offset, local_destination_offset)`，正文 9600 字节。B1/B2 都只在 rail0 发一份完整请求。两端所有 service 的 `maxSendRecvDataSize=16384`，split/RNDV threshold 为 `UINT32_MAX`。

每条 rail 在本 rail 的最后一个数据 Put 后发送 DATA_DONE。local 返回条件为：所有 rail 的 DATA_DONE generation 到达，且本端 rail0 COPY_REQ Send callback 完成。失败路径可用 COPY_ERROR；发送不可用时由断链唤醒。

## rail 与内存布局

B1：一个 rail MR，600 槽。B2：每端每 rail 独立注册一个 300 槽 MR。每槽 4096 字节，前 1024 字节有效，gap 使用 sentinel 校验。

rail 由请求索引唯一决定：B2 的 0..299 为 rail0、300..599 为 rail1。offset 是对应 rail MR 内偏移，不用 offset 大小选择 rail，不跨 rail 混用 key。每 rail 的 destination permutation 单独保证唯一；source 可重复，当前生成器提供非顺序 permutation。

local 在每 rail HELLO 中发送本 rail destination 基址、大小、region id 和完整 MemoryKey。remote READY 只返回本 rail source region id、大小和对齐，不导出 source key。

## pending/active 与线程

remote rail0 worker 收到 COPY_REQ 后，先占用预分配 pending 槽，复制完整消息，再以 release 发布。remote app acquire 后复制到独立 active，才释放 pending。local 可在上一代 DATA_DONE 后发下一请求；即使 remote 上代 callbacks 尚未全部处理，新请求也只进入 pending，不覆盖 active。

B2 的 rail0 使用主线程，rail1 使用常驻线程。rail1 从 service 创建、MR 注册开始，贯穿连接/握手、每轮 WR 构造/Put/DATA_DONE、callback drain、FINISH 到 teardown；不会为每轮创建线程，也不会跨线程调用 rail1 service。命令参数和 active 请求经 issued release/acquire 发布，completed release/acquire 归还协调权。callback 在各 HCOM worker 执行，仅发布完成状态。

阶段 1.5 规则保留：数据等待使用 CPU relax，每 256 次检查 deadline；attempted 由固定 app 线程以普通计数维护，completed 和 active callbacks 使用原子量并缓存行隔离；每请求 callback 分配保持不变。异步失败进入有界 drain，不能假设失败请求一定回调。

## trace 与结果

local trace 记录 `local_begin/request_posted/data_done[rail]/local_end`；remote 记录 `request_received/posted[rail]/data_callbacks_done[rail]/done_posted[rail]`。trace 仅在独立 trace run 开启。不同主机时钟不相减，remote 诊断不替代 local sparse_copy，重叠区间不相加。

结果 schema=4，case 保持 B1/B2。每调用 payload 614400 字节，请求 9664 字节，data WR=600，DATA_DONE WR=L，success ACK=0。

## 验证边界

本机 self-test 和语法检查不能证明真实 MR/key、DMA 可见性、RQ/CQ、同 QP WRITE→SEND 顺序、双 NIC 分流或性能。目标机必须保存编译输入/hash、命令、两端日志、NIC/QP/流量、CPU/NUMA/MTU 和错误注入证据。B2 受当前 HCOM 多 Service 契约限制，定位为诊断穿刺。
