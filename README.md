# RDMA 600 × 1 KiB：direct 与阶段3 SGL 流水 scatter

本分支 `duo_card_sgl` 在双 rail 固定线程 direct 基线上完成阶段3。local 是调用者、最终 destination/stage 拥有者和唯一主计时端；remote 拥有 sparse source 并发起 RDMA WRITE。状态为 `IMPLEMENTED / PRODUCTION_SYNTAX_PASS / HISTORICAL_LOCAL_SELF_TEST_PASS / TARGET_BUILD_AND_HW_PENDING`，不能据本地检查声称 Linux 链接、真实 QP 顺序、双 NIC 或性能已通过。

协议统一为 `sparse-copy-v5-dual-rail-sgl`。HELLO 严格为256B（两个104B region descriptor），每次调用都在计时内生成、校验并发送 600 个 source/destination offset，COPY_REQ 始终为 9664B；成功路径没有 round ACK。

- `--mode direct`：B1/B2；600 个普通 Put，per-rail DATA_DONE，同 v5 wire 回归。
- `--mode sgl --pipeline on|off`：S1/S2；remote 按 K 项构造 PutV，写入 local 每 rail 连续 stage；每个 PutV 成功 post 后立即在同一 channel 发送独立 CHUNK_DONE；local 主线程按 on/off 策略 scatter 到最终稀疏 destination。
- `RDMA_600_SGL_ITEMS`：严格十进制 1..30，默认 16。当前公共头 `NET_SGE_MAX_IOV=16`，因此 K=30 明确报 `UNSUPPORTED`，不会静默降为 16。
- `RDMA_600_QP_MAX_SEND_SGE`：每 rail 一个正整数的外部声明，例如 `16,16`。它只登记部署时对真实已创建 QP 的 query 结果来源，不是自动查询。SGL measure 缺失该声明会拒绝；verify/trace 可运行但结果标 `QP_CAP_PENDING`。

## 构建

目标 Linux/RDMA 主机：

```bash
bash ./build.sh --ubs-root /absolute/path/to/ubs-comm
```

阶段开发时曾以内嵌 C++ self-test 完成协议、映射、调度和边界回归，证据保留在 `STAGE3_REPORT_CN.md`；该宏、CLI 入口和专用测试代码已于2026-09-16删除，当前程序只提供真实 local/remote RDMA 运行模式。无硬件检查使用真实公共头对完整生产源码执行 `-fsyntax-only`。

## 启动示例

direct B1/B2 沿用 `--mode direct`；direct 不接受 `--pipeline`，也不受 SGL 环境变量改变工作量。示例 B1 remote：

```bash
./build/rdma_600 --role remote --rdma-ip <remote_nic0_ip> \
  --listen <remote_oob_ip>:19000 --links 1 --mode direct --kind verify \
  --verify-rounds 20 --warmup 0 --rounds 0 --timeout-sec 10 \
  --app-cpu <app_cpu> --worker-cpu <worker_cpu>
```

B1 local 将 `--role remote --listen` 换为 `--role local --peer` 并使用 local RDMA IP。B2 使用两个逗号分隔值：`--rdma-ips`、`--listen/--peer`、`--app-cpus`、`--worker-cpus`，且 `--links 2`。

SGL S2/K16/on verify（两端设置相同 K；cap 可暂缺但会标 pending）：

```bash
export RDMA_600_SGL_ITEMS=16
export RDMA_600_QP_MAX_SEND_SGE=16,16
./build/rdma_600 --role remote \
  --rdma-ips <remote_nic0_ip>,<remote_nic1_ip> \
  --listen <remote_oob_ip>:19000,<remote_oob_ip>:19001 \
  --links 2 --mode sgl --pipeline on --kind verify \
  --verify-rounds 20 --warmup 0 --rounds 0 --timeout-sec 10 \
  --app-cpus <app0>,<app1> --worker-cpus <worker0>,<worker1>
```

local 使用相同 workload 参数和环境变量，改为 `--role local --peer ...`。off 对照只改 `--pipeline off`。正式 measure 使用 `--kind measure --verify-rounds 20 --warmup 1000 --rounds 10000`，必须显式绑核并提供真实 QP cap 声明。

## 结果、trace 与边界

local 输出 schema 5，case 为 B1/B2 或 `S1-K-on/off`、`S2-K-on/off`。verify/trace 的性能字段为 null；measure 记录完整 sparse_copy 分位数、有效 GB/s、block Mops、request GB/s 和 wall。结果区分预期 PutV/WRITE/通知计数与未采集的 verbs trace，并记录公共头 cap、QP cap 声明来源及验证状态。

trace 只能独立以 `--kind trace --trace-rounds 1..64` 运行。SGL 记录每 chunk 的 ready、scatter begin/end，以及 remote PutV/CHUNK_DONE post；不同主机时间戳不能相减。

目标机仍须验证：源码/二进制/静态库 hash，一致公共头与实际链接库，创建后真实 QP `max_send_sge`，每 chunk `groupCount=1/num_sge=count` 和 WRITE→SEND 同 QP，stage 地址，双 NIC 流量、NUMA/CPU/MTU、断链/部分 post/callback 延迟，以及 K8/K16 on/off 的多次 verify/measure。B2/S2 仍是 HCOM 多 Service 契约外的诊断穿刺。

详细规范见 [STAGE3_DESIGN_CN.md](STAGE3_DESIGN_CN.md)，实现与验证台账见 [STAGE3_REPORT_CN.md](STAGE3_REPORT_CN.md)。`docs/main_reference/` 是 main 历史原文，只读保留；本分支不含 `run.py`。
