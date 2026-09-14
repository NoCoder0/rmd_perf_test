# duo_card requester-driven sparse_copy 迁移报告

日期：2026-09-14  
状态：IMPLEMENTED / LOCAL_PROTOCOL_PASS / TARGET_BUILD_AND_HW_PENDING

## 迁移基线与范围

目标为 `duo_card@8970f08115c0c44219c6a83581ef1f317c61b202`。参考 main 的 requester-driven 阶段 1 提交 `4719c50d22a7fcb8cc7f1cb93b0511c3b4087105`，并保留 duo_card 的 B1/B2、两 service、两真实 rail 配置和固定线程设计。只修改目标 worktree；main 与 `ubs-comm@e709a37e71bc2493d2a195a14eaaefe86c2dfb28` 只读。没有恢复 `run.py`，没有修改或提交依赖，也没有启动硬件测试。

## 代码结果

- 角色改为 local/remote：remote 监听，local 连接、拥有最终 dst 并计量完整同步调用。
- 协议升级为 `sparse-copy-v4-dual-rail`；新 magic、version 和 wire 长度拒绝旧 v3。
- 每次 local 在计时内生成、验证、编码 600 对 offset；direct 请求固定 64+9600=9664 字节，B2 仅 rail0 发送一次。
- B2 由请求索引分 rail：0..299/300..599；offset 为本 rail MR 内偏移。每 rail 分别验证 300 个 destination 槽唯一，生成非顺序 permutation。
- remote rail0 callback 复制 pending；应用线程交接到独立 active。下一请求可以占 pending，但 active/WR/source/通知在上代 callbacks 回收前不复用。
- remote B1 提交 600 Put；B2 两个固定线程各提交 300 Put。每条真实 QP 的 Put 后分别发 DATA_DONE。
- local 返回 gate 为全部 rail DATA_DONE 加 rail0 COPY_REQ Send callback。逐轮成功 ACK 已删除；COPY_ERROR 仅用于失败。
- rail1 常驻线程覆盖 service/MR setup、连接/握手、每轮构造/提交/回收、FINISH 和 teardown；rail0 始终由主线程负责。
- 阶段 1.5 的 busy-poll relax、256 次 deadline、线程私有 attempted、callback 原子完成、缓存行隔离、ActiveCallbackGuard 和每请求 callback 保留。
- trace 分为 local 主调用时间线和 remote 本机诊断。正式 measure 不启用 trace，不跨主机相减，也不把重叠区间相加。
- local 结果 schema=4，case 继续使用 B1/B2；记录 9664 字节请求、600 data WR、L 个 DATA_DONE 和 0 ACK。

## 本地验证

1. `git diff --check`：最终复核见交付摘要。
2. 使用当前 ubs-comm 公共头的 Windows/MSYS2 受限全文件语法检查：PASS。临时兼容层只补本机缺少的 Linux 声明，未进入目标 worktree。
3. 以 `RDMA_600_SELF_TEST_ONLY` 编译并运行：PASS。覆盖 B1/B2 wire round-trip、截断拒绝、每 rail destination 重复拒绝、非顺序映射、DATA_DONE 和数据/gap 校验。

这些检查不等于目标 Linux/AArch64 构建，更不能证明 RDMA 硬件正确性。

## 未验证

- 与目标机实际静态 `libhcom_static.a` 的 Linux/AArch64 编译和链接。
- 9664 字节真实 Send、MR/key、DMA 可见性、WRITE→SEND 同 QP 顺序、RQ/CQ 和断链/部分 post 失败。
- B2 两个 QP 分别落到两张 NIC 且各承担约一半 payload 的计数证据。
- 20 轮双端 verify、独立 trace、1000 warmup+10000 measure 的至少 5 次 B1/B2 对照。
- HCOM 同协议多 Service 的目标机行为；B2 仍是诊断穿刺。

因此本报告不提供迁移后 B2 性能数字。旧 `38e6637` sender-driven 数据和 main 单卡数据只作历史背景，不能作为双 rail 收益。

