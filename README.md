# RDMA 600 × 1 KiB — 阶段 1（B1）

这是 ubs-comm RDMA 穿刺测试的最小独立实现。当前代码只实现阶段 1 的单链接 baseline：`B1`。设计、协议边界和后续阶段请见 [DESIGN_CN.md](DESIGN_CN.md) 与 [IMPLEMENTATION_PLAN_CN.md](IMPLEMENTATION_PLAN_CN.md)。

本仓库没有附带或伪造任何硬件性能数字。实际 RDMA 正确性和性能验证需要两台 Linux 鲲鹏服务器、可工作的 RDMA 路径、匹配版本的 ubs-comm 构建产物及可用 SSH 登录。

## B1 的固定工作量

- 一条 RDMA path：一个 service、一个 device、一个 `linkCount=1` 的 worker-poll channel/QP；内部 multirail 关闭。
- 每轮总有效数据严格为 `600 × 1024 = 614400` 字节。
- sender 的源块位于 `src + i * 4096`；receiver 先写入连续 `stage + i * 1024`，再由一个应用 scatter 线程复制到 `dst + i * 4096`。
- 每 30 个异步 `Put(1024)`，在同一 channel/QP 上异步追加一个 `Send(DATA_READY)`；每轮为 600 个数据 WR、20 个通知 WR。
- receiver 按 chunk 收到通知后立即 scatter；20 个 chunk 完成后异步 `Send(ROUND_ACK)`。收到 ACK 且 sender 的本地 data/Send callbacks 均完成后才复用下一轮缓冲。
- `generation` 从 1 递增，覆盖 verify、warmup 与 measure，不会在阶段切换时清零。

阶段 1 明确拒绝双链接、SGL、`scatter=after-all` 和 `WRITE_WITH_IMM` 参数；它们属于后续阶段，不能混入 B1 结果。

## 构建

以下命令应在目标 Linux 机器上执行。本项目的 CMake 只会链接已构建的
ubs-comm / HCOM 产物；它不会自动下载依赖、构建 ubs-comm 或安装软件包。

### 1. 构建 ubs-comm 的 HCOM RDMA 产物

先确保目标机上的 RDMA 驱动、设备和 ubs-comm 所需依赖已经按该项目要求就绪，
然后在 ubs-comm 根目录运行其自带构建脚本：

```bash
UBS_ROOT=/absolute/path/to/ubs-comm
cd "$UBS_ROOT"

HCOM_BUILD_TYPE=release \
HCOM_BUILD_SERVICE=on \
HCOM_BUILD_RDMA=on \
HCOM_BUILD_SOCK=on \
HCOM_BUILD_SHM=on \
HCOM_BUILD_TESTS=off \
HCOM_BUILD_EXAMPLE=off \
BUILD_HCOM=ON \
bash ./build.sh
```

该脚本会创建（并在每次构建前重新生成）`$UBS_ROOT/tmp_build_dir` 和
`$UBS_ROOT/dist/hcom`。如需清理这些生成目录，可显式执行
`bash ./build.sh clean`；不要在其中保留未备份的手工文件。

构建成功后，至少确认本测试需要的 HCOM 和第三方产物存在：

```bash
test -f "$UBS_ROOT/dist/hcom/lib/libhcom_static.a"
test -f "$UBS_ROOT/dist/hcom/include/hcom/hcom_service.h"
test -f "$UBS_ROOT/dist/hcom_3rdparty/libboundscheck/lib/libboundscheck.so"
test -d "$UBS_ROOT/dist/hcom_3rdparty/umdk/urma/include"
```

### 2. 构建 rdma_600

```bash
PERF_ROOT=/absolute/path/to/rmd_perf_test

cmake -S "$PERF_ROOT" -B "$PERF_ROOT/build" \
  -DCMAKE_BUILD_TYPE=Release \
  -DHCOM_INCLUDE_DIR="$UBS_ROOT/dist/hcom/include" \
  -DHCOM_LIB_DIR="$UBS_ROOT/dist/hcom/lib"
cmake --build "$PERF_ROOT/build" -j
```

`CMakeLists.txt` 从 `HCOM_INCLUDE_DIR` 推导 `dist/hcom_3rdparty`。若部署目录不同，显式传入：

```bash
-DHCOM_3RDPARTY_DIR=/absolute/path/to/dist/hcom_3rdparty \
-DURMA_INCLUDE_DIR=/absolute/path/to/umdk/urma/include
```

构建所需的已验证依赖名称与 ubs-comm 自带 perf CMake 一致：`hcom_static`、`boundscheck`、pthread、dl 与（存在时）rt。配置失败时先核实实际 `dist` 路径和目标机器的构建产物；不要把 Windows 路径复制到 Linux 命令中。

## 无硬件逻辑检查

构建出的二进制可运行不接触 RDMA 网卡的本地检查：

```bash
./build/rdma_600 --self-test
```

它验证 614400 字节布局、600 个 block、20 个 chunk、显式 HELLO/READY/DATA_READY 编解码、memory-key 编码、generation 数据模式和 destination stride 间隙。它不是建链、DMA、CQ、RQ、MR 或性能验证。

## 手工双端运行

先在 receiver 机器启动。`LISTENING` 仅供外部进程编排；真正的 READY 只会在 receiver 已分配、触页并注册 staging MR 后通过 HCOM HELLO/Reply 返回。

```bash
export LD_LIBRARY_PATH="$UBS_ROOT/dist/hcom/lib:$UBS_ROOT/dist/hcom_3rdparty/libboundscheck/lib:${LD_LIBRARY_PATH:-}"

./build/rdma_600 --role receiver \
  --rdma-ip <receiver_nic0_rdma_ip> \
  --listen <receiver_oob_ip>:19000 \
  --kind verify --verify-rounds 20 --warmup 0 --rounds 0 \
  --timeout-sec 10 --app-cpu <scatter_cpu> --worker-cpu <cq_cpu> \
  --links 1 --mode plain --chunk-items 30 --scatter pipeline --notify send
```

在 sender 机器启动匹配参数：

```bash
export LD_LIBRARY_PATH="$UBS_ROOT/dist/hcom/lib:$UBS_ROOT/dist/hcom_3rdparty/libboundscheck/lib:${LD_LIBRARY_PATH:-}"

./build/rdma_600 --role sender \
  --rdma-ip <sender_nic0_rdma_ip> \
  --peer <receiver_oob_ip>:19000 \
  --kind verify --verify-rounds 20 --warmup 0 --rounds 0 \
  --timeout-sec 10 --app-cpu <submit_cpu> --worker-cpu <cq_cpu> \
  --links 1 --mode plain --chunk-items 30 --scatter pipeline --notify send
```

用 `--kind measure`、`--warmup 1000 --rounds 10000` 进入正式计时；该模式仍会先跑完整 verify。sender 只在正常结束时输出一行 JSON。`--kind verify` 中所有正式带宽/延迟字段为 `null`，避免将正确性运行误读为性能结果。

## SSH 跑测脚本

从示例生成本地私有配置，填入真实主机、绝对部署路径、OOB IP、RDMA IP 和 CPU；不要向该文件加入密码：

```bash
cp hosts.example.json hosts.json
chmod 600 hosts.json
```

`run.py` 假定二进制和库已部署、无交互 SSH 已可用。它启动 receiver，等待已 flush 的 `LISTENING`，再启动 sender；失败/超时时只会根据该 run 的唯一远端 PID 文件清理本次进程，绝不会 `pkill` 同名进程。

```bash
python3 run.py --config hosts.json --suite stage1 --kind verify --output results/20260911-b1-verify

python3 run.py --config hosts.json --suite stage1 --kind measure --repeat 5 \
  --output results/20260911-b1-measure
```

每个 repeat 使用新的远端进程和新的本地目录。输出中包含：

```text
results/<run-id>/
  manifest.json
  repeat-001/B1/
    manifest.json
    sender.stdout.log
    sender.stderr.log
    receiver.stdout.log
    receiver.stderr.log
    result.jsonl
  REPORT.md
```

脚本会记录二进制 SHA-256、真实路径、`LD_LIBRARY_PATH` 下的 `ldd` 输出、命令参数、退出码和日志；只接受参数与 B1 约束完全匹配的 sender JSON。任一进程失败、超时、缺少/重复结果或参数不匹配都会使该 repeat 失败，且不会用旧结果替代。

## 计时与验证口径

sender 使用本机 `CLOCK_MONOTONIC_RAW` 记录：

- `submit_us`：第一个 `Put` 前至最后一个 DATA_READY `Send` 提交成功后；
- `e2e_us`：第一个 `Put` 前至收齐 ROUND_ACK，且本地 data/Send callback 均完成；
- 有效带宽：正式循环的总 614400 字节/轮除以整段正式 wall time。

不同机器的时间戳不会相减。receiver 的 1 KiB memcpy 不逐条读时钟；阶段 2 才会增加独立 trace。正式 measure 每轮只检查通知 generation 和完成状态，结束后完整检查最终 destination 与 stride gap；verify 轮则检查所有 64-bit word 的 generation/block/word 数据模式。

## 运行前清单

- 每个 `--rdma-ip` 必须是本机预期网卡 IP；OOB 管理网地址可以不同。
- app CPU 与 HCOM worker CPU 应使用不同物理核心，并记录 NIC/内存 NUMA 关系。
- 检查双方部署的是同一源码提交和同一套 HCOM 动态库；脚本的 manifest 会保留可追溯信息。
- 先运行 `verify`，再运行 `measure`。真实结果应同时保存端口计数、设备/QP 映射和 CPU 使用量；当前脚本不把建链日志当作 NIC 流量证据。

当前实现和验证状态见 [PROGRESS.md](PROGRESS.md)。
