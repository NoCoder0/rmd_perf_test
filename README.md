# RDMA 600 × 1 KiB — requester-driven direct B1/B2

阶段3工作分支为 `duo_card_sgl`，起点 `duo_card@b3f4e4e43ff87ffe2a1544a648bc0b01904b3806`。新增设计见 [STAGE3_DESIGN_CN.md](STAGE3_DESIGN_CN.md)，main的完整历史设计文档见 [参考索引](docs/main_reference/REFERENCE_INDEX_CN.md)。下文记录继承的direct基线；阶段3实现状态由后续报告单独登记。

本分支在 `duo_card` 的双 rail 固定线程实现上迁入 requester-driven `sparse_copy`。当前代码状态为本地协议/语法检查通过，目标 Linux/AArch64 构建和双机 RDMA 验证仍为 `HW_PENDING`。

协议细节见 [DESIGN_CN.md](DESIGN_CN.md)，实施顺序见 [IMPLEMENTATION_PLAN_CN.md](IMPLEMENTATION_PLAN_CN.md)，迁移证据见 [STAGE2_SPARSE_COPY_REPORT_CN.md](STAGE2_SPARSE_COPY_REPORT_CN.md)。

角色固定：

- `local`：调用者、最终 destination 拥有者和主计时端；主动连接。
- `remote`：source 拥有者和 RDMA WRITE 发起端；监听连接。
- B1：单 rail 600 个 `Put(1024)`。
- B2：双 rail，各 300 个 `Put(1024)`；每 rail 一个 service/NIC/QP 和一个从 setup 到 drain 固定的应用线程。

一次调用由 local 完整生成、校验并编码 600 对源/目标偏移，rail0 发送一份 9664 字节 `COPY_REQ`。remote 复制到 pending，交接到独立 active，按请求索引分 rail 重建本轮 WR，并在每条真实 QP 的数据 Put 后分别发送 `DATA_DONE`。local 等齐所有 rail 的 `DATA_DONE`（数据 ready）和本端 COPY_REQ Send callback 后返回。成功路径没有逐轮 ACK；下一次 COPY_REQ 才授予下一代复用权限。

协议为 `sparse-copy-v4-dual-rail`。version、magic、消息长度均与旧 sender-driven v3 区分，旧新二进制不会误握手。

## 构建与自测

目标 Linux 主机：

```bash
bash ./build.sh --ubs-root /absolute/path/to/ubs-comm
./build/rdma_600 --self-test
```

自测覆盖 B1/B2 的 HELLO/READY、9664 字节请求、截断/重复目标拒绝、请求索引分 rail、每 rail 非顺序映射、DATA_DONE 和数据/gap 校验。它不验证 MR、DMA、CQ/RQ、真实 QP 顺序或双 NIC。

## B1 直接启动

先在 remote：

```bash
./build/rdma_600 --role remote \
  --rdma-ip <remote_nic0_rdma_ip> --listen <remote_oob_ip>:19000 \
  --links 1 --mode direct --kind verify --verify-rounds 20 --warmup 0 --rounds 0 \
  --timeout-sec 10 --app-cpu <remote_app_cpu> --worker-cpu <remote_worker_cpu>
```

再在 local：

```bash
./build/rdma_600 --role local \
  --rdma-ip <local_nic0_rdma_ip> --peer <remote_oob_ip>:19000 \
  --links 1 --mode direct --kind verify --verify-rounds 20 --warmup 0 --rounds 0 \
  --timeout-sec 10 --app-cpu <local_app_cpu> --worker-cpu <local_worker_cpu>
```

## B2 直接启动

remote：

```bash
./build/rdma_600 --role remote \
  --rdma-ips <remote_nic0_ip>,<remote_nic1_ip> \
  --listen <remote_oob_ip>:19000,<remote_oob_ip>:19001 \
  --links 2 --mode direct --kind verify --verify-rounds 20 --warmup 0 --rounds 0 \
  --timeout-sec 10 --app-cpus <remote_app0>,<remote_app1> \
  --worker-cpus <remote_worker0>,<remote_worker1>
```

local：

```bash
./build/rdma_600 --role local \
  --rdma-ips <local_nic0_ip>,<local_nic1_ip> \
  --peer <remote_oob_ip>:19000,<remote_oob_ip>:19001 \
  --links 2 --mode direct --kind verify --verify-rounds 20 --warmup 0 --rounds 0 \
  --timeout-sec 10 --app-cpus <local_app0>,<local_app1> \
  --worker-cpus <local_worker0>,<local_worker1>
```

正式测量两端使用 `--kind measure --verify-rounds 20 --warmup 1000 --rounds 10000`。每端所有 app/worker CPU 应使用不同物理核心。详细 trace 必须单独以 `--kind trace --trace-rounds N` 运行；正式 measure 不采集事件时间。

本分支保持删除 Python 编排的状态：没有 `run.py`，也不应从 main 恢复。各主机直接启动二进制并自行保存 stdout/stderr。`hosts.example.json` 仅作为参数记录模板，程序不读取。

## 地址与结果口径

B2 请求索引 `0..299` 属于 rail0，`300..599` 属于 rail1。每个 offset 都是对应 rail MR 内的字节偏移，范围为 `0..299*4096`；两 rail 分别校验 300 个 destination 槽唯一。B1 同理使用 rail0 的 600 槽 MR。offset 不用于选择 rail。

local 输出 schema 4，case 仍为 `B1`/`B2`，主指标为 `sparse_copy_avg_us/p50/p95/p99`、`effective_GBps`、`block_Mops`、`request_GBps` 和 measured wall。每次预期 1 个 COPY_REQ、600 个 data WR、L 个 DATA_DONE、0 个 success ACK。remote 只输出状态和独立本机 trace；跨主机时间戳不能相减，重叠区间不能相加。

## 当前限制

- 尚未在目标 Linux/AArch64 对实际 `ubs-comm@e709a37` 产物完成编译/链接。
- 尚未跑双机 verify、故障注入、真实 NIC/QP 流量证明或 B1/B2 measure；没有迁移后 B2 性能结论。
- ubs-comm 明示的同协议多 Service 限制仍使 B2 属于诊断穿刺，不代表正式产品 multirail 方案。
- direct 无 staging/scatter/SGL/IMM；callback 仍按请求分配，本次不修改 ubs-comm。
