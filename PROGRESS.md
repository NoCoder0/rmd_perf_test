# 实施进度

基线：ubs-comm `oneside-msge-merge`，参考提交 `9e4c035a5d68ccca02d05fade3b6f5907db24ef4`。本文件记录代码实施和真实验证两个独立维度；未执行的硬件步骤不会以本地静态检查替代。

| 阶段 | 实现 | 验证 | 说明 |
|---|---|---|---|
| 1 — 单链接 B1 | IMPLEMENTED | NOT_RUN | 已提供最小 C++、CMake、双机本地角色脚本、配置示例和 README；尚未在目标 Linux/RDMA 环境构建或运行。 |
| 1.5 — direct 热路径优化 | IMPLEMENTED | LOCAL_PASS_HW_PENDING | A+B 已实现：数据面忙轮询、每 256 次检查 deadline、按线程所有权精简 attempted 计数并隔离写入缓存行；callback 分配保留。目标 Linux 构建、profile 与 RDMA 跑测待执行。 |
| 2 — direct 双链接与 trace | NOT_STARTED | NOT_RUN | 在阶段 1 direct 结构上扩展双链接；B1/B2 采用相同阶段 1.5 优化，无 staging/scatter。 |
| 3 — SGL | NOT_STARTED | NOT_RUN | 不在阶段 1 二进制中实现。 |
| 4 — WRITE_WITH_IMM | NOT_STARTED | NOT_RUN | 未修改 ubs-comm。 |

## 阶段 1 已实现范围

- 固定 B1 参数：单 service/device/channel/QP，`linkCount=1`，worker poll，内部 multirail 关闭。
- `600 × 1 KiB` 异步普通 `Put`；第 `i` 个请求从 `src + i * 4096` 直接写到最终 `dst + i * 4096`。没有 staging 分配、staging MR 或 CPU scatter。
- 每轮 600 个 Put 后在同一 channel/QP 发送一个 `ROUND_READY`。verify 轮先完整校验再发本轮 ACK；warmup/measure 轮直接发本轮 ACK，所有测量轮结束后才最终校验，成功后 FINISH_ACK。generation 与 FINISH/FINISH_ACK drain 保留。
- 显式 HELLO/READY、ROUND_READY、ACK、FINISH wire 编解码；READY 只在接收端最终 destination 初始化及 MR 注册后回复。
- HCOM service 在 `Start()` 前显式 `SetTlsOptions(enableTls=false)`，不初始化 TLS context，也不要求证书、私钥或 PSK 回调。
- 发送和接收 callback/API 返回/超时/消息合法性检查；正常路径在释放 MR、channel 或状态前等待所有本地 callback。错误路径若无法在有界时间内 drain，则进程直接退出而非释放仍可能被 callback 使用的状态。
- verify 使用 generation/block/word 全字校验和 dst 间隙哨兵；measure 预生成稳定数据、每轮不做 CPU 拷贝或扫描，并在结束时作全量最终检查。
- `run.py` 在两台主机分别以 `--role receiver` / `--role sender` 启动一个本地角色并归档证据，不承担 QP、MR 或数据面工作；失败时仅清理本机本次调用启动的进程组。

## 已执行的本机准备检查

| 检查 | 结果 | 证据/限制 |
|---|---|---|
| `run.py` AST 解析 | PASS | Windows 本机仅检查 Python 语法。 |
| `python run.py --help` | PASS | 已确认 CLI 参数说明可输出；未启动本地测试进程。 |
| 示例配置 / B1 参数生成及结果契约 | PASS | `hosts.example.json` 可通过脚本校验；verify argv 固定 `--rounds 0`，且不再传 chunk/scatter/notify 参数；direct B1 JSON 能通过脚本校验。 |
| C++ 语法检查 | PASS（受限） | 用实际 ubs-comm service 公共头执行 `g++ -fsyntax-only`；Windows 缺失 Linux 平台符号时仅临时注入兼容声明。该检查覆盖本文件/API 名称，不替代目标 Linux 的真实头、链接或 RDMA 构建。 |
| C++ / CMake 构建 | NOT_RUN | 当前机器没有目标 Linux/AArch64 ubs-comm `dist/hcom` 产物可供链接。 |
| `rdma_600 --self-test` | NOT_RUN | 需要先在目标 Linux 构建二进制；该检查本身不验证 RDMA。 |
| 双机 RDMA verify / measure | NOT_RUN | 尚未提供两台目标 Linux 主机、网卡、驱动/provider、NUMA 或可用 RDMA 路径。 |

2026-09-12 阶段 1.5 本机复查：`git diff --check`、`run.py` AST/`--help` 和阶段 1.5 结果契约检查通过；使用依赖仓库 `9e4c035a5d68ccca02d05fade3b6f5907db24ef4` 的真实 service 公共头完成受限 `g++ -std=c++17 -fsyntax-only`，仅为 Windows 缺失的 Linux 声明使用临时兼容头且未保留进仓库。未执行链接、`rdma_600 --self-test`、profile 或 RDMA 测试。阶段 1.5 结果与双边消息精简分析见 `STAGE1_5_REPORT_CN.md`。

## 下一步：真实硬件验证

2026-09-12 更新：阶段 1.5 代码已实现但硬件未验证。执行时须保留当前 B1 原版与 A-only 证据，再完成 original/A/AB 对照；阶段 2 只增加 direct 双链接并测量收益。以下命令仍使用共用 `--suite stage1`，实际版本由源码/diff、二进制 hash 和结果 JSON 的优化元数据识别。

1. 在 sender/receiver 两台目标 Linux 主机上以同一 ubs-comm 版本构建并部署 `rdma_600`；记录两端二进制和库 hash。
2. 填写相同的 `hosts.json`（从 `hosts.example.json` 复制）并复制到两台主机，确认两端 OOB IP、RDMA IP、独立 app/worker CPU 和库目录。
3. 先在 receiver 主机运行：

   ```bash
   python3 run.py --config hosts.json --role receiver --suite stage1 --kind verify --output results/<run-id>-verify-receiver
   ```

4. receiver 输出 `LISTENING` 后，在 sender 主机运行：

   ```bash
   python3 run.py --config hosts.json --role sender --suite stage1 --kind verify --output results/<run-id>-verify-sender
   ```

5. verify 成功后，按相同顺序进行 measure；每个物理 repeat 使用新的 receiver/sender 进程和新的两端输出目录。
6. 保存两端脚本生成的 manifest/log/JSON，并补充实际 QP/NIC 映射、端口计数、MTU、NUMA、绑核与 CPU 使用量。只有这些证据齐全时，阶段 1 验证才可从 `NOT_RUN` 更新为 `HW_PASS`。

## 非目标 / 未解决项

- 未实现双 NIC、SGL、staging/scatter 对照、跨轮窗口、callback 池、自动重连、内存 doorbell 或 `WRITE_WITH_IMM`。
- 未确认实际 QP `max_send_sge`；阶段 1 plain B1 不依赖它，阶段 3 必须单独查询并核验。
- 未产生性能结果、trace、NIC 端口计数或硬件正确性结论。
