# 实施进度

代码依赖基线：只读 ubs-comm `oneside-msge-merge`，提交
`9e4c035a5d68ccca02d05fade3b6f5907db24ef4`。阶段 2 工作目录为独立
`perf_test_duo_card` / `duo_card` worktree；已快进同步 `main@38e6637`，未写入主 worktree。

| 阶段 | 实现 | 验证 | 说明 |
|---|---|---|---|
| 1 — 单链接 B1 | IMPLEMENTED | LOCAL_PASS_HW_PENDING | 保留为同一阶段 2 二进制的 `--links 1` 回归路径。 |
| 1.5 — direct 热路径优化 | IMPLEMENTED | LOCAL_PASS_HW_PENDING | 已从 main 同步 A+B，并同时应用到 B1/B2；每请求 callback 分配仍保留。 |
| 2 — direct 双链接与 trace | IMPLEMENTED | LOCAL_PASS_HW_PENDING | 双 service/NIC/MR/channel/QP、每 rail 300 块、交替提交、独立 ready/ACK 和预分配 trace 已编码；无硬件结论。 |
| 3 — SGL | NOT_STARTED | NOT_RUN | 本阶段未实现。 |
| 4 — WRITE_WITH_IMM | NOT_STARTED | NOT_RUN | 未修改 ubs-comm。 |

## 阶段 2 已实现范围

- 每端最多两个独立 service，service 名带 rail；每个 service 只配置一个 `<rdma-ip>/32`，
  worker 数为 1，`linkCount=1`、worker poll、TLS 关闭、内建 multirail 关闭。
- B1 rail0 负责 600 块；B2 rail0/rail1 各 300 块。每 rail 单独分配并注册 stride-4096
  source/destination MR，pattern 使用 `global_block=rail*blocks_per_rail+local_block`。
- 一个 sender 应用线程按 local block 交替向 rail0/rail1 异步 `Put(1024)`；数据完成后每 rail
  各在其 QP 上投递一个 `ROUND_READY`。总数据 WR 仍为 600，B2 ready/ACK 各为 2。
- receiver 应用线程扫描两 rail ready，先到 rail 可先校验/ACK，不等慢 rail，也不等待某 rail
  ACK 本地 callback 才投递另一 rail ACK。sender 仍在两 rail ACK、本地数据与 Send callback
  全部满足后才开始下一轮。
- HELLO/READY/ROUND_READY/ACK/FINISH/FINISH_ACK 都携带并校验 rail；协议参数包含 trace 轮数，
  双方参数不一致即失败。每 rail 分别交换该 service 注册的 destination MR key。
- verify、warmup、measure、trace 的 generation 连续；verify 每 rail 全量校验，measure/trace
  结束后最终全量校验，再在两 rail 完成 FINISH drain。
- `--kind trace` 最多 64 轮，固定数组预分配。实现 `S0/S_post/S1/S_data/S_ack/S2` 与
  `R_ready/R_ack`；callback 中先写时间戳再 release 发布相关状态，不逐条打印，drain 后批量
  输出 JSONL。正式 measure 不记录详细 trace。
- `run.py` 已按当前直接运行二进制的工作流删除；配置示例仅记录双 NIC、双端口和双 worker CPU
  的对应关系，程序不读取 JSON 配置。

## 已执行的本地检查

| 检查 | 结果 | 证据/限制 |
|---|---|---|
| worktree/依赖身份 | PASS | `duo_card` 已同步到 `main@38e6637`；只读 ubs-comm 为要求的分支/提交且干净。 |
| 启动方式 | PASS（静态） | `run.py` 已删除；README 只保留直接运行 `rdma_600` 的双端命令。 |
| 参数防错 | PASS（源码检查） | B2 拒绝重复 NIC IP、OOB endpoint、worker CPU，measure 要求显式绑定 app 与所有 rail worker CPU。 |
| C++ 公共头语法检查 | PASS（受限） | MinGW `g++ -fsyntax-only` 配合仅用于补齐 Windows 缺少的 Linux 声明，实际包含要求提交的 HCOM service 公共头；不替代目标 Linux 编译/链接。 |
| 构建脚本语法 | PASS | `bash -n ./build.sh` 退出码 0。 |
| 配置记录示例 | PASS | PowerShell `ConvertFrom-Json` 成功，双端各 2 个 RDMA IP、receiver 2 个 OOB 端口。程序不读取此文件。 |
| Linux CMake/链接 | NOT_RUN | 当前 Windows 没有目标 Linux/AArch64 HCOM `dist` 产物。 |
| `rdma_600 --self-test` | NOT_RUN | 需先在目标 Linux 链接出二进制。 |
| 双机 B1/B2 verify/trace/measure | NOT_RUN | 未提供两台服务器地址、双 NIC、驱动/provider、NUMA、凭据或端口统计访问。 |

## 硬件待验证项

1. 两端用同一源码和 HCOM 静态库输入构建，运行 `--self-test`，记录二进制/HCOM hash。
2. 分别跑 B1/B2 20 轮 verify，确认每 rail pattern、stride gap、严格 generation、ACK 栅栏、
   callback drain 与 FINISH 正常；补做单 rail 延迟/断链失败检查。
3. 用设备/QP 日志和两端两张 NIC 的测试前后端口字节/包计数，证明 rail0/rail1 实际走不同 NIC。
   两个 service/channel 本身不是流量证据。
4. 单独跑 B1/B2 trace，核验事件完整性、同机区间和 B2 ACK imbalance；禁止跨主机减时间戳，
   禁止把重叠区间相加当 e2e。
5. 关闭 trace，以同一二进制、同一阶段 1.5 A+B 设置交替跑 B1/B2 各 5 次并反转顺序；保存原始 JSON、
   CPU 使用量、网卡/MTU/NUMA/绑核和端口计数后才能形成性能结论。

完整命令与输出契约见 [README.md](README.md)。

## 明确非目标

- 阶段 1.5 已共同应用于 B1/B2，但尚无目标硬件上的 original/A/AB 或 B1/B2 性能数据。
- 未实现 SGL、staging/scatter、IMM、跨轮窗口、双提交线程、callback 池或 SSH 编排。
- 未产生或推断任何真实性能、双 NIC 吞吐或单向网络时延数字。
