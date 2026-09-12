# RDMA 600 × 1 KiB — 阶段 2 direct B1/B2（固定 rail 线程）

本目录实现同一二进制下的 direct 单链接 B1 与双链接 B2。它基于 ubs-comm
`oneside-msge-merge` / `9e4c035a5d68ccca02d05fade3b6f5907db24ef4` 的公共 service API；
没有修改 ubs-comm、memfabric 或主 worktree。

本分支已同步 `main@38e6637` 的阶段 1.5 A+B 实现，并将它同时应用到 B1/B2：数据面使用
CPU relax 忙轮询、每 256 次检查 deadline；应用线程独占的 attempted 计数使用普通整数，
callback 侧完成计数仍为原子量并保留 `ActiveCallbackGuard`；每请求 callback 分配保持不变。
B1/B2 比较只改变链接数，不能拿旧未优化 B1 与当前 B2 比较。

没有目标服务器、双 NIC 地址、设备映射和端口计数，因此本文不提供性能数字，也不把“创建了
两个 channel”当作双 NIC 流量证据。

## 数据路径与协议

- 总工作量固定为 600 个 1024 字节块，有效数据 614400 字节，source/destination stride
  都是 4096。
- B1：一个 service、一个显式 RDMA IP、一个 `linkCount=1` worker-poll channel/QP，
  rail0 负责 600 块。
- B2：每端两个独立 service，各自 `SetDeviceIpMask(<nic-ip>/32)`，各一个 worker 和
  `linkCount=1` channel/QP；内建 multirail 在每个 service 上关闭。rail0/rail1 各负责
  300 块，global block id 为 `rail*300 + local_id`。
- B1 保持一个应用线程。B2 使用两个从启动到 drain 都常驻的应用线程：主线程固定只操作
  rail0/service0，第二线程固定只操作 rail1/service1；两线程同时各投递 300 个 Put，随后在
  自己的 QP 上追加 `ROUND_READY`。不在每轮创建线程。
- B2 receiver 同样由两个固定 rail 线程分别观察 ready、校验（verify）并通过自己的 service
  投递 ACK；任一 rail 不等待另一 rail。整轮仍须等两 rail ACK、数据 callback 和 ready Send
  callback 后才能复用缓冲。
- 固定线程是针对当前 HCOM `thread_local` pool 亲和假设的快速性能穿刺方案。HCOM 源码同时
  明示上层应禁止两个同协议 Service 并存，因此本用例必须标记为诊断实现，而非受支持的最终
  产品架构；后续正式方案应在单 Service 下增加“小请求整包选 rail”能力并修复 pool key。
- verify 每轮检查全部 word 与 stride gap；warmup/measure 不逐轮扫描，结束后完整检查最终
  destination，再在每 rail 上完成 FINISH/FINISH_ACK drain。
- generation 从 1 开始，跨 verify/warmup/measure/trace 单调递增。协议版本为 3，双方必须
  使用同一版二进制。
- 没有 staging、scatter、SGL、IMM、跨轮窗口或 callback 池。

## 构建与本地自检

在两台目标 Linux/RDMA 主机上先按 ubs-comm 自身说明构建 HCOM RDMA 静态库，然后：

```bash
PERF_ROOT=/absolute/path/to/perf_test_duo_card
UBS_ROOT=/absolute/path/to/ubs-comm
cd "$PERF_ROOT"
bash ./build.sh --ubs-root "$UBS_ROOT"
./build/rdma_600 --self-test
```

也可显式指定产物：

```bash
bash ./build.sh \
  --hcom-include /absolute/path/to/dist/hcom/include \
  --hcom-lib /absolute/path/to/dist/hcom/lib \
  --boundscheck-root /absolute/path/to/dist/hcom_3rdparty/libboundscheck
```

`--self-test` 只验证 B1/B2 分区、614400 字节 direct 布局、stride gap、generation pattern、
HELLO/READY/token 编解码和 rail 字段；它不建立 RDMA 连接，不能替代 Linux 链接或硬件验证。

## 双 NIC 配置

程序不读取配置文件，所有参数直接通过命令行传入。[hosts.example.json](hosts.example.json)
仅作为两端地址、端口和 CPU 对应关系的记录模板。运行前确认：

- sender/receiver 的 `rdma_ips[0]` 与 `[1]`，每个地址分别属于预期的不同 RDMA NIC；
- receiver 可从 sender 到达的 OOB IP，以及两个不同 OOB TCP 端口；
- B2 每端两个不同 app CPU 和两个不同 worker CPU；任一 app CPU 不得与任一 worker 重合；
- 两端本机的 `binary` 和 `library_dirs` 绝对 Linux 路径。

OOB IP 不要求是 RDMA IP。程序用 `/32` 过滤绑定 RDMA 设备，并拒绝 B2 中重复的 RDMA IP、
端口或 worker CPU；仍须用设备/QP 日志和两张网卡测试前后端口计数证明真实映射和流量。

## 手工运行

以下示例先启动 receiver。逗号分隔列表按 rail0、rail1 对应；B1 可继续使用单值别名
`--rdma-ip`、`--app-cpu` 和 `--worker-cpu`。B2 必须使用 `--app-cpus` 为两个固定 rail
线程提供不同 CPU。

若 `libboundscheck` 等依赖不在系统搜索路径，两端先按各自实际安装位置设置并核验：

```bash
export LD_LIBRARY_PATH=/absolute/path/to/libboundscheck/lib:${LD_LIBRARY_PATH:-}
ldd ./build/rdma_600
```

```bash
# receiver / B2 verify
./build/rdma_600 --role receiver \
  --rdma-ips <receiver_nic0_ip>,<receiver_nic1_ip> \
  --listen <receiver_oob_ip>:19000,<receiver_oob_ip>:19001 \
  --kind verify --verify-rounds 20 --warmup 0 --rounds 0 \
  --timeout-sec 10 --app-cpus <rail0_app_cpu>,<rail1_app_cpu> \
  --worker-cpus <cq0_cpu>,<cq1_cpu> \
  --links 2 --mode plain

# sender / B2 verify（receiver 输出 LISTENING 后）
./build/rdma_600 --role sender \
  --rdma-ips <sender_nic0_ip>,<sender_nic1_ip> \
  --peer <receiver_oob_ip>:19000,<receiver_oob_ip>:19001 \
  --kind verify --verify-rounds 20 --warmup 0 --rounds 0 \
  --timeout-sec 10 --app-cpus <rail0_app_cpu>,<rail1_app_cpu> \
  --worker-cpus <cq0_cpu>,<cq1_cpu> \
  --links 2 --mode plain
```

B1 使用列表中的第一个 NIC/端口/worker，并把 `--links` 改成 1。measure 使用
`--kind measure --warmup 1000 --rounds 10000`。正式性能运行不启用 trace。

独立 trace 诊断示例：

```bash
./build/rdma_600 ... --kind trace --verify-rounds 20 \
  --warmup 0 --rounds 0 --trace-rounds 32 --links 2 --mode plain
```

trace 先执行正常的 20 轮完整 verify，再以 generation 连续的稳定 pattern 跑 32 个诊断轮；
最终仍完整校验 destination 并完成 FINISH drain。

`run.py` 已删除。建议直接为每端保存 stdout/stderr，例如在上述命令末尾追加
`> results/<run-id>-<role>.stdout.log 2> results/<run-id>-<role>.stderr.log`。sender 正常结束时
输出一行结果 JSON；trace 模式在正常 drain 后先批量输出 trace JSONL，再输出 sender 结果 JSON。

## Trace 输出契约

每行都是 `record_type=trace`、`trace_schema=rdma600-stage2-v1` 的 JSON，包含：

```text
host_role, case, generation, rail, event, timestamp_ns
```

全局 sender 事件 `S0/S1/S2` 的 `rail` 为 `null`；每 rail 事件为数字。事件定义：

- `S0`：主线程发布本轮命令前；`S_post[r]`：该 rail 固定线程的数据和 ROUND_READY API 都
  成功返回；`S1=max(S_post[r])`，表示最后一个 rail 完成提交的时刻。
- `S_data[r]`：该 rail 最后一个数据 API 的成功 callback；`S_ack[r]`：收到且验证该 rail ACK；
  `S2`：两 rail ACK、本地数据和 Send 完成都满足。
- `R_ready[r]`：callback 验证 ready 后、release 发布 ready 前；`R_ack[r]`：receiver 应用线程调用
  对应 ACK Send 前。

记录槽固定预分配，callback 不打印；时间戳写入后才 release 发布相关 ready/ACK/完成状态，进程在
正常 drain 后批量输出。时间来自各主机自己的 `CLOCK_MONOTONIC_RAW`：只计算同一主机区间，绝不
相减 sender/receiver 时间戳。`submit`、`local finish`、`ACK observed` 是重叠区间，不能相加
冒充 e2e；direct B2 没有 scatter 指标。

## B1/B2 正式比较与硬件证据

用同一二进制、相同配置和相同阶段 1.5 A+B 实现，trace 关闭，B1/B2 各跑 5 次并交替顺序，下一 repeat
反转先后。B1 是一个提交线程，B2 是两个提交线程，当前结果只能作为小包双 rail 快速穿刺，不能将
全部差异归因于第二张 NIC。保存原始 e2e p50/p95/p99、submit p50、有效 GB/s、CPU 使用量、网卡/MTU/NUMA/绑核、
构建与 HCOM 静态库 hash。聚合规则固定为各 run 指标的中位数：

结果 JSON 的 `commit` 来自构建时源码 HEAD；存在未提交的 tracked diff 时会带 `-dirty`。它不能替代
二进制和 HCOM 静态链接输入的 hash，正式跑测仍须把这些标识与完整 diff 一并保存。

```text
latency_speedup = median(B1 run 的 e2e_p50) / median(B2 run 的 e2e_p50)
bandwidth_ratio = median(B2 run 的 effective_GBps) / median(B1 run 的 effective_GBps)
```

另跑 B1/B2 trace 诊断，按同机时间戳计算提交、post-submit wait、local finish、ACK observed 和
B2 rail ACK imbalance。双 NIC 验收还必须包含两端每 rail 的实际设备/QP 映射及两张网卡测试前后
端口字节/包计数；端口字节含协议开销，不能替代 614400 字节口径的应用有效带宽。

当前状态和本地检查见 [PROGRESS.md](PROGRESS.md)，详细设计与阶段边界见
[DESIGN_CN.md](DESIGN_CN.md) 和 [IMPLEMENTATION_PLAN_CN.md](IMPLEMENTATION_PLAN_CN.md)。
