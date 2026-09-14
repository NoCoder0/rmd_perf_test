# requester-driven sparse_copy：阶段3实现设计摘要

更新日期：2026-09-14。详细且具优先级的规范为 [STAGE3_DESIGN_CN.md](STAGE3_DESIGN_CN.md)；本文记录最终实现形态。

## 语义与模式

local 拥有最终 destination，并从生成 600 对真实 offset 开始计时，直到数据通知、CPU scatter（SGL）和本端 COPY_REQ callback 全部完成。remote 拥有 stride=4096 的 source，按请求索引分 rail。单次有效载荷 600×1024B，COPY_REQ 固定为 64B 头加 600 对 u64 offset，共 9664B。

direct B1/B2 保持 600 个普通 Put 和每 rail DATA_DONE。SGL S1/S2 为每 rail `ceil((600/links)/K)` 个 chunk：每 chunk 一个 PutV，所有 iov 使用同一 rkey 且远端 stage 地址连续；成功 post 后立即在同一 channel 发送一条 CHUNK_DONE。local 以 acquire 观察 generation-ready 后，从连续 stage scatter 到请求指定的 destination。pipeline on 随 ready 交错 scatter；off 等齐后调用相同 scatter 函数。

## wire 与内存

协议为 `sparse-copy-v5-dual-rail-sgl`，version=5，新 magic。Parameters=40B；HELLO=252B，含 destination 与可选 stage 描述符/80B key；READY=64B；COPY_REQ=9664B；CHUNK_DONE=40B。所有整数显式网络字节序，精确长度和 reserved=0 均校验。direct 的 K=0、pipeline=off、stage 全零；SGL 使用 sparse-600、stage region 3。

仅 local/SGL 额外按固定 rail 线程分配、触页并注册每 rail `R*1024` 连续 stage；destination MR 仍为 `R*4096`。remote 每 rail 的全部 iov、PutV request 和逐 chunk 通知缓冲具有跨异步 callback 的稳定生命周期。checked arithmetic 同时验证 offset、region 和 base+length。

## 并发、deadline 与能力

两 rail 架构仍为两个固定应用线程各自拥有一个 service/channel/MR 生命周期；local 主线程独占所有 CPU scatter，只读取 rail1 stage，不调用 rail1 service。remote pending/active 请求分离；active/iov/source/通知缓冲在本代全部 data/Send callbacks 回收前不复用。

local 在发请求前 release 发布 expected generation；CHUNK_DONE handler 校验 role/mode/rail/generation/chunk/first/count/bytes 后以 CAS release 发布 ready，重复、迟到、未来和越界均使整次 run 失败。scatter 以 acquire 读取 ready，consumed 与 ready 独立。每次 SparseCopy 使用从入口 t0 派生的统一绝对 deadline；direct 也使用该口径。

K 来自严格环境变量 `RDMA_600_SGL_ITEMS`，设计范围 1..30，默认16。可运行条件同时受公共头、实际链接库和真实创建 QP cap 限制。当前公共头上限16，因此 K30 为 UNSUPPORTED。公共 API 无真实 QP cap getter；`RDMA_600_QP_MAX_SEND_SGE` 只作为 `external-declaration` 登记，不伪称自动验证，SGL measure 缺失时拒绝。

## 结果与安全边界

schema=5 明确记录 mode/K/pipeline/source format、9600B descriptor、stage/chunk/预期 WR 与通知数、cap 来源和 pending 状态。正式 measure 不记录逐 chunk trace；trace 另跑。成功 JSON 只在 FINISH、callback drain、MR/service teardown 和固定线程退出后输出。

依赖 PutV 失败路径存在 callback 所有权风险，本 demo 不删除失败 callback、不重试，转为失败并有界 drain；无法证明 quiesce 时 `_Exit`，避免释放仍可能被 DMA/callback 使用的存储。真实 QP 顺序、DMA 可见性、linked-library ABI、双 NIC 和性能仍为 `TARGET_BUILD_AND_HW_PENDING`。
