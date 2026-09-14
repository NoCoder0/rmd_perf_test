# 实施进度

更新日期：2026-09-14。

## 当前状态

目标 worktree：`duo_card@8970f08115c0c44219c6a83581ef1f317c61b202` 上的未提交迁移。

| 项目 | 实现 | 验证 |
|---|---|---|
| requester-driven direct B1 | IMPLEMENTED | LOCAL_PROTOCOL_PASS / HW_PENDING |
| requester-driven direct B2 | IMPLEMENTED | LOCAL_PROTOCOL_PASS / HW_PENDING |
| 固定 rail 线程 setup→drain 所有权 | IMPLEMENTED | STATIC_REVIEWED / HW_PENDING |
| 详细 trace 与 schema 4 | IMPLEMENTED | STATIC_REVIEWED / HW_PENDING |
| SGL/staging/scatter/IMM | NOT_IN_SCOPE | NOT_RUN |

## 已实现

- 角色改为 local/remote，remote 监听、local 连接；协议升级为 `sparse-copy-v4-dual-rail` 并更换 magic/长度，拒绝旧 v3。
- local 每次调用内生成、校验、编码完整 600 对 offset；rail0 只发送一份 9664 字节 COPY_REQ。
- B2 由请求索引连续分 rail；每 rail offset 定位本 rail 300 槽 MR，分别验证 destination 唯一并保留非顺序映射。
- remote callback 将请求复制到 pending；应用线程复制到独立 active 后释放 pending。active、WR、source 验证数据和 DATA_DONE 缓冲在本地 callbacks 回收前不复用。
- remote 两条固定线程各构造并提交 300 个 Put，并在各自真实 QP 上发送 DATA_DONE；B1 提交 600 个。
- local 等所有 rail DATA_DONE 和 rail0 请求 Send callback 才返回。成功路径没有逐轮 ACK，失败可发 COPY_ERROR。
- rail1 常驻线程负责自己的 service 创建、MR、连接/握手、每轮提交/回收、FINISH 和 teardown；rail0 始终由主线程负责。
- 保留阶段 1.5 忙轮询、每 256 次 deadline 检查、线程私有 attempted、acquire/release 完成发布、缓存行隔离、ActiveCallbackGuard 和每请求 callback。
- trace 分为 local 接口时间线和 remote 本机诊断；measure 不启用详细 trace。
- 保留 duo_card 已删除 `run.py` 的状态。

## 已执行检查

- `git diff --check`：PASS。
- 使用 `ubs-comm@e709a37` 当前公共头进行 Windows/MSYS2 受限全文件语法检查：PASS；兼容头仅补 Windows 缺少的 Linux 声明，不进入仓库。
- `RDMA_600_SELF_TEST_ONLY` 本机编译并运行：PASS，输出 `SELF_TEST: PASS (sparse-copy-v4, B1/B2, 9664-byte request, 600 direct blocks)`。
- 目标 Linux/AArch64 完整构建/链接：NOT_RUN。
- 双机 RDMA/hardware verify/measure：NOT_RUN。

## 下一步

在两台目标机使用相同源码和相同 ubs-comm 产物构建，先跑 self-test，再分别跑 B1/B2 的 20 轮 verify。核对两个真实 NIC/QP、每 rail 流量、完整请求容量、非顺序映射、故障退出和绑核/NUMA。正确性通过后，才运行 `1000 warmup + 10000 measure` 的多次 B1/B2 对照。旧 sender-driven 数据不是迁移后 B2 结果。
