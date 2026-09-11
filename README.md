# RDMA 600 × 1 KiB — 阶段 1（B1）

这是 ubs-comm RDMA 穿刺测试的最小独立实现。当前代码只实现阶段 1 的单链接 baseline：`B1`。设计、协议边界和后续阶段请见 [DESIGN_CN.md](DESIGN_CN.md) 与 [IMPLEMENTATION_PLAN_CN.md](IMPLEMENTATION_PLAN_CN.md)。

本仓库没有附带或伪造任何硬件性能数字。实际 RDMA 正确性和性能验证需要两台目标 Linux 主机、可工作的 RDMA 路径及匹配版本的 ubs-comm 构建产物。`run.py` 只在启动它的当前主机上运行由 `--role` 指定的一个角色，不会通过 SSH 连接、部署或启动另一台机器。

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
HCOM_BUILD_UB=off \
HCOM_BUILD_SOCK=off \
HCOM_BUILD_SHM=off \
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
test -f "$UBS_ROOT/dist/hcom/include/hcom/hcom_service_context.h"
test -f "$UBS_ROOT/dist/hcom_3rdparty/libboundscheck/lib/libboundscheck.so" || \
  test -f "$UBS_ROOT/dist/hcom_3rdparty/libboundscheck/lib/libboundscheck.a"
```

### 2. 构建 rdma_600

`CMakeLists.txt` 不写死任何本地的 HCOM 路径；`build.sh` 会把路径作为 CMake
配置参数传入。常规部署只需传入已构建 ubs-comm 的根目录：

```bash
PERF_ROOT=/absolute/path/to/rmd_perf_test
UBS_ROOT=/absolute/path/to/ubs-comm
cd "$PERF_ROOT"

bash ./build.sh --ubs-root "$UBS_ROOT"
```

脚本默认创建 `$PERF_ROOT/build`，并通过 `nproc` 自动选择并行度。可按需覆盖：

```bash
bash ./build.sh --ubs-root "$UBS_ROOT" \
  --build-dir /tmp/rdma_600-debug \
  --build-type Debug \
  --jobs 16
```

若此前复用过旧构建目录，其中的 `CMakeCache.txt` 可能仍显示已经从工程中删除的
缓存项。请改用新的 `--build-dir`，或先清理旧构建目录再重新配置；当前源工程不会
读取这些旧缓存项。

若 HCOM 安装目录不是标准的 `$UBS_ROOT/dist` 布局，可将每个路径作为脚本参数传入：

```bash
bash ./build.sh \
  --hcom-include /absolute/path/to/dist/hcom/include \
  --hcom-lib /absolute/path/to/dist/hcom/lib \
  --boundscheck-root /absolute/path/to/dist/hcom_3rdparty/libboundscheck
```

也可通过环境变量 `UBS_ROOT`、`HCOM_INCLUDE_DIR`、`HCOM_LIB_DIR`、
`BOUNDSCHECK_ROOT`、`BUILD_DIR`、`CMAKE_BUILD_TYPE` 和
`JOBS` 提供同样的构建参数。若要传递其他 CMake 配置参数，将它们置于 `--` 后，
例如 `bash ./build.sh --ubs-root "$UBS_ROOT" -- -G Ninja`。

本程序只使用 HCOM 的 RDMA service 公共 API。直接编译依赖为 HCOM 公共头文件、
`libhcom_static.a` 和 `boundscheck`（公共头 `hcom_service_def.h` 直接包含
`securec.h`），链接依赖为 pthread、dl 与平台存在时的 rt。程序不包含 URMA
头文件；配套的 ubs-comm 构建也只启用 service/RDMA，显式关闭 UB、SOCK 和 SHM。
配置失败时先核实实际 `dist` 路径和目标机器的构建产物；不要把 Windows 路径复制到 Linux 命令中。

## 无硬件逻辑检查

构建出的二进制可运行不接触 RDMA 网卡的本地检查：

```bash
./build/rdma_600 --self-test
```

它验证 614400 字节布局、600 个 block、20 个 chunk、显式 HELLO/READY/DATA_READY 编解码、memory-key 编码、generation 数据模式和 destination stride 间隙。它不是建链、DMA、CQ、RQ、MR 或性能验证。

## 手工双端运行

先在 receiver 主机启动 receiver 角色，再在 sender 主机启动 sender 角色。`LISTENING` 仅供进程编排；真正的 READY 只会在 receiver 已分配、触页并注册 staging MR 后通过 HCOM HELLO/Reply 返回。

```bash
export LD_LIBRARY_PATH="$UBS_ROOT/dist/hcom_3rdparty/libboundscheck/lib:${LD_LIBRARY_PATH:-}"

./build/rdma_600 --role receiver \
  --rdma-ip <receiver_nic0_rdma_ip> \
  --listen <receiver_oob_ip>:19000 \
  --kind verify --verify-rounds 20 --warmup 0 --rounds 0 \
  --timeout-sec 10 --app-cpu <scatter_cpu> --worker-cpu <cq_cpu> \
  --links 1 --mode plain --chunk-items 30 --scatter pipeline --notify send
```

确认 receiver 已输出 `LISTENING` 后，在 sender 主机启动匹配参数的 sender：

```bash
export LD_LIBRARY_PATH="$UBS_ROOT/dist/hcom_3rdparty/libboundscheck/lib:${LD_LIBRARY_PATH:-}"

./build/rdma_600 --role sender \
  --rdma-ip <sender_nic0_rdma_ip> \
  --peer <receiver_oob_ip>:19000 \
  --kind verify --verify-rounds 20 --warmup 0 --rounds 0 \
  --timeout-sec 10 --app-cpu <submit_cpu> --worker-cpu <cq_cpu> \
  --links 1 --mode plain --chunk-items 30 --scatter pipeline --notify send
```

用 `--kind measure`、`--warmup 1000 --rounds 10000` 进入正式计时；该模式仍会先跑完整 verify。sender 只在正常结束时输出一行 JSON。`--kind verify` 中所有正式带宽/延迟字段为 `null`，避免将正确性运行误读为性能结果。

## 双机本地角色脚本

从示例生成同一份配置并复制到两台主机，填入 sender/receiver 各自的绝对二进制/库路径、OOB IP、RDMA IP 和 CPU。
HCOM 已静态链接进 `rdma_600`。`library_dirs` 只需列出实际的动态运行时依赖；默认构建使用共享 boundscheck 时，只保留其库目录即可。脚本会按当前角色的列表组装本机 `LD_LIBRARY_PATH` 并记录实际路径。旧的单值 `library_dir` 仍可使用，但建议迁移为 `library_dirs`：

```bash
cp hosts.example.json hosts.json
chmod 600 hosts.json
```

`sender` 字段描述 sender 主机，`receiver` 字段描述 receiver 主机。两端都要保存同一份配置，但各自执行时只会检查并使用本机角色的 `binary`、`library_dirs`、RDMA IP 和 CPU。`receiver.oob_ip` 必须能从 sender 主机连接。

在 receiver 主机先执行，终端会实时显示 `LISTENING`：

```bash
python3 run.py --config hosts.json --role receiver \
  --suite stage1 --kind verify --output results/20260911-b1-verify-receiver
```

确认 `LISTENING` 后，在 sender 主机执行：

```bash
python3 run.py --config hosts.json --role sender \
  --suite stage1 --kind verify --output results/20260911-b1-verify-sender
```

measure 时两端使用相同的 `--kind measure`，每个物理 repeat 都要先启动新的 receiver，再启动新的 sender，并使用新的输出目录：

```bash
# receiver 主机
python3 run.py --config hosts.json --role receiver \
  --suite stage1 --kind measure --output results/20260911-b1-measure-001-receiver

# sender 主机，待 receiver 输出 LISTENING 后执行
python3 run.py --config hosts.json --role sender \
  --suite stage1 --kind measure --output results/20260911-b1-measure-001-sender
```

每个脚本调用只启动一个本地角色。失败或超时时只会终止本次调用启动的本地进程组，绝不会 `pkill` 同名进程。receiver 输出目录包含其 manifest 与日志；sender 输出目录还包含经过校验的结果和报告：

```text
results/<run-id>-receiver/
  manifest.json
  receiver.stdout.log
  receiver.stderr.log

results/<run-id>-sender/
  manifest.json
  sender.stdout.log
  sender.stderr.log
  result.jsonl
  REPORT.md
```

脚本会记录本机二进制 SHA-256、真实路径、`LD_LIBRARY_PATH` 下的 `ldd` 输出、命令参数、退出码和日志。sender 只接受参数与 B1 约束完全匹配的 JSON；失败、超时、缺少/重复结果或参数不匹配都会使本次 sender 调用失败，且不会用旧结果替代。

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
