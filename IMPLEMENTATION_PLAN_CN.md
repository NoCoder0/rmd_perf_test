# requester-driven sparse_copy 双 rail 实施计划

更新日期：2026-09-14。

## 已完成迁移范围

1. 从 main 阶段 1 引入 local/remote requester-driven 语义、完整 9664 字节 COPY_REQ、pending/active、DATA_DONE、无逐轮成功 ACK、local 主计时和 schema。
2. 保留 duo_card 的 B1/B2 同二进制、每 rail 独立 service/NIC/QP/MR/key 和固定 rail 应用线程；rail1 生命周期扩展到 setup→drain/teardown。
3. B2 按请求索引 0..299/300..599 分 rail；offset 是每 rail MR 内偏移，各 rail 分别做范围和 destination 唯一校验。
4. 保留阶段 1.5 busy-poll relax、256 次 deadline、线程私有 attempted、callback-owned 原子完成、缓存行隔离、ActiveCallbackGuard 和每请求 callback。
5. 保留独立 trace，但改为 local sparse_copy 时间线及 remote 本机诊断；正式 measure 不采样。
6. 不恢复 Python `run.py`，不修改 ubs-comm，不实现 staging/scatter/SGL/IMM。

## 目标机验收顺序

1. 两端确认目标分支、源码 hash、`ubs-comm` 提交和实际链接静态库 hash。
2. Linux/AArch64 Release 构建；运行 `./rdma_600 --self-test`。
3. B1/B2 各跑 20 轮 verify，检查非顺序地址、gap、generation、错误退出、完整 9664 字节请求及所有 rail ready gate。
4. B2 记录两张 NIC、两个真实 QP 和各约一半 payload 的端口计数证据；不能只凭 `links=2`。
5. 单独 trace 短跑，按主机、generation、rail 检查事件；禁止跨主机时钟相减。
6. B1/B2 各至少 5 次 `1000 warmup + 10000 measure`，交替顺序。保存每次原始 JSON 和 wall，报告跨 repeat 中位数/范围，不平均 p99。
7. 做参数不一致、交换 endpoint/key、单 rail 延迟、断链和部分提交失败测试。local 不可提前返回，失败不得输出有效性能。

## 后续阶段

阶段 3 才新增 staging/SGL/scatter；阶段 4 才最小扩展 WRITE_WITH_IMM。任何后续优化都必须保持 local 主计时、600 个 destination 地址、请求驱动的复用信用和 B1/B2 回归。callback 池只在目标机 profile 证明分配是主要瓶颈后单独实验。

旧协议数据只能解释历史：它不是本次迁移后 B2 性能。现阶段不能根据旧 submit/e2e 或 main 单卡结果声称双卡收益或重大回归。
