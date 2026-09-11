# 分阶段实施与验证任务书：ubs-comm RDMA 600 × 1 KiB

状态：实施任务书，未执行其中的编程、构建或硬件跑测。日期：2026-09-11。

配套设计：[DESIGN_CN.md](C:/code/RDMA_DEMO/perf_test/DESIGN_CN.md)。本文规定实施顺序、交付物和阶段验收，协议与内存布局以设计文档为准。执行模型若发现两份文档与源码存在冲突，应说明依据并记录最小修正，不应默默改变测试口径。

原始代码基线：ubs-comm `oneside-msge-merge`，`9e4c035a5d68ccca02d05fade3b6f5907db24ef4`。

本机项目位置为 `C:/code/RDMA_DEMO`；实际构建、跑测在两台目标 Linux RDMA 主机完成。文中的 `PERF_ROOT`、`UBS_ROOT` 是各自主机上的绝对目录，由实施者根据实际环境指定，不照搬 Windows 路径。

## 1. 交给实施模型的总约束

1. 先读设计文档、本文及工作目录适用的 `AGENTS.md`，查看现有代码与 git 状态，保留已有修改。先复用已有阶段产物，再继续实现。
2. 实现一个小型 C++ 可执行程序，不引入 memfabric，不搬入整个 hcom perf 框架。Python 只用标准库在各自主机启动一个角色并负责本机结果归档。
3. 顺序为：**单链接 baseline → 双链接与打点 → SGL → WRITE_WITH_IMM**。每阶段保持前一阶段用例可运行，逐阶段形成可独立审阅的 diff 和报告。
4. 两端总有效数据固定为每轮 600 × 1024 字节。双链接每条 300 块；源/目标 stride=4096；staging 连续；最终完成口径包含 scatter 和 ACK。
5. 所有主 case 使用异步提交；一个 service 对应一个 RDMA 设备，每个 channel `linkCount=1`、worker poll、关闭内建 multirail；应用提交线程和 scatter 线程各一个。
6. 从第一阶段就保留 `chunk_items=30`、按 chunk 通知和流水 scatter。它仍是 600 次独立 Put；不得直到 SGL 阶段才给 baseline 增加 scatter，导致历史结果不可比较。
7. 首版单轮在途；第一至第三阶段使用同一 QP 的 WRITE + Send 通知。实现局部函数、固定数组和必要的状态，不增加跨轮窗口、通用线程池、动态调度、自动重连或 selective signaling。
8. 第一至第三阶段优先只改 `perf_test`；第四阶段改 ubs-comm。若前期发现库缺陷确实阻塞正确性，单独提交最小修复并重新跑 baseline，不将修复收益归入双链接/SGL/IMM。
9. API 返回码、本地 callback 错误、接收消息合法性、超时、缓冲复用和退出 drain 都必须处理。不要为“代码最少”省略这些正确性条件。
10. 硬件结果只来自真实运行。编译通过、mock 通过、脚本可启动都不等于 RDMA 验证通过；不得生成示意数字冒充测量结果。

每次只交办一个阶段时，实施模型完成该阶段及可执行验证后交付，不擅自扩展到下一阶段。若收到四阶段整体实施指令，可按顺序推进；阶段验收是技术条件，不是额外的人工审批流程。

无目标硬件时，继续完成不依赖硬件的代码、构建检查、脚本和交付文档，将硬件验收明确保留为 pending。后续本地准备可以继续，但不能将后续性能结论建立在未验证的上一阶段之上。

## 2. 交付目录与状态记录

建议产物保持精简：

```text
perf_test/
  DESIGN_CN.md
  IMPLEMENTATION_PLAN_CN.md
  rdma_600.cpp
  CMakeLists.txt
  run.py
  hosts.example.json          sender/receiver 主机参数、绝对路径、绑核配置示例
  README.md                   已验证的编译/手工运行/脚本运行命令
  PROGRESS.md                 阶段状态、验证命令、证据路径与遗留问题
  results/<run-id>/
    manifest.json             参数、版本、库路径、构建、机器元信息
    sender.stdout.log
    sender.stderr.log
    receiver.stdout.log
    receiver.stderr.log
    result.jsonl              正式计时结果
    trace.jsonl               第二阶段开始：单独诊断运行的打点
    REPORT.md                 本次结果、比较、失败/跳过原因
```

不是每个 run 都必须生成 trace。不为脚本增加另一套插件或配置框架。原始日志和数据使用新的 run-id 保存，不覆盖上一阶段结果。

`PROGRESS.md` 为每阶段分别记录两个维度：

| 维度 | 状态 | 含义 |
|---|---|---|
| 实现 | NOT_STARTED / IN_PROGRESS / IMPLEMENTED | 是否已经有对应代码与脚本 |
| 验证 | NOT_RUN / LOCAL_PASS_HW_PENDING / HW_PASS / FAIL | 是否真实完成硬件正确性与该阶段跑测 |

示例：“阶段 2：IMPLEMENTED / LOCAL_PASS_HW_PENDING；缺少第二条 RDMA 路径；尚无双链接性能结论。”

硬件不支持某一 SGL 上限时，该 case 单独 SKIP 并附能力证据；不能把设备不支持伪装为实现通过，也不能把静默拆分后的结果标成 30 SGE 单 WR。

## 3. 共用脚本契约：阶段 1 就交付

以下为待实现的运行接口，不是已经存在的脚本。实施者可以对参数命名作小幅调整，但最终 README 必须给出复制即可执行的实际命令。

```bash
# receiver 主机
python3 "$PERF_ROOT/run.py" --config "$PERF_ROOT/hosts.json" \
  --role receiver --suite stage1 --kind verify \
  --output "$PERF_ROOT/results/<run-id>-receiver"

# sender 主机，待 receiver 输出 LISTENING 后执行
python3 "$PERF_ROOT/run.py" --config "$PERF_ROOT/hosts.json" \
  --role sender --suite stage1 --kind verify \
  --output "$PERF_ROOT/results/<run-id>-sender"
```

measure 使用相同的双主机顺序和 `--kind measure`。每个物理 repeat 使用新的两端输出目录；脚本不跨主机协调 repeat。后续只增加 `--suite stage2|stage3|stage4` 和第二阶段的 `--kind trace`，不复制出四套启动脚本。`--kind verify` 可以只做验证、不做正式计时；其 JSON 不应出现可误读的正式带宽。

配置示例的语义如下，IP、CPU 和路径均由实际机器填写：

```json
{
  "sender": {
    "binary": "/absolute/path/on/sender/rdma_600",
    "library_dirs": ["/absolute/path/on/sender/lib"],
    "rdma_ips": ["<sender_nic0_ip>", "<sender_nic1_ip>"],
    "app_cpu": 2,
    "worker_cpus": [3, 4]
  },
  "receiver": {
    "binary": "/absolute/path/on/receiver/rdma_600",
    "library_dirs": ["/absolute/path/on/receiver/lib"],
    "oob_ip": "<receiver_oob_ip>",
    "oob_ports": [19000, 19001],
    "rdma_ips": ["<receiver_nic0_ip>", "<receiver_nic1_ip>"],
    "app_cpu": 2,
    "worker_cpus": [3, 4]
  }
}
```

阶段 1 配置只需要每端数组中的第一项。阶段 2 验证第二张 NIC、第二个端口和第二个 worker CPU 存在且不重复。应用不假定 OOB IP 就是 RDMA IP。

脚本工作流程：两台主机各运行一次，receiver 的 `LISTENING` 由人工或外部调度器作为 sender 的启动信号。

```python
cfg = load_and_validate_config()
role = parse_role_from_cli()
create_fresh_local_result_directory()
record_local_binary_and_library_identity(role)
process = start_locally(role_argv(role), log_files)
stream_local_stdout_and_stderr(process)
wait_for_exit_with_deadline(process)
if role == "sender":
    validate_sender_result_record()
    save_sender_report()
```

最少必须做到：

- receiver 的 `LISTENING` 必须明确 flush 后输出；接收进程报错退出或启动超时，人工/外部调度器不得继续启动 sender。C++ 的 HELLO/READY 才是实际数据准备条件。
- 使用 `subprocess` 参数数组直接启动本地进程。持续读取或重定向两端 stdout/stderr，不能因为管道未消费导致测试阻塞。
- 两端退出码均检查；结果必须有 case、参数、轮数、校验状态。缺少记录、重复记录或参数不匹配均失败，禁止拿旧文件代替。
- 失败/超时只清理当前主机、本次调用启动的本地进程组；不能 `pkill` 全部同名程序。
- 第一版可以要求二进制提前部署，不自动编译/拷贝/安装系统依赖。README 给出手工部署和两终端运行方法，便于排除包装脚本问题。
- 记录二进制和库的 hash、加载路径、源码 commit 及 dirty diff 标识，避免两端或两轮实际加载不同库却显示同一版本。

构建依据：[ubs-comm build.sh](C:/code/RDMA_DEMO/ubs-comm/build.sh) 支持 service/RDMA 构建开关，默认产物位于 `dist/hcom`；[现有 perf CMakeLists.txt](C:/code/RDMA_DEMO/ubs-comm/test/hcom/tools/perf_test/CMakeLists.txt) 可参考 include、hcom、boundscheck 等依赖。实施者需核实实际产物，不凭记忆杜撰 include 路径或库名。

计划中的独立构建命令如下，`HCOM_INCLUDE_DIR/HCOM_LIB_DIR` 由新 CMakeLists 明确定义：

```bash
cmake -S "$PERF_ROOT" -B "$PERF_ROOT/build" \
  -DCMAKE_BUILD_TYPE=Release \
  -DHCOM_INCLUDE_DIR="$UBS_ROOT/dist/hcom/include" \
  -DHCOM_LIB_DIR="$UBS_ROOT/dist/hcom/lib"
cmake --build "$PERF_ROOT/build" -j
```

boundscheck 等额外依赖按实测构建需求补充显式路径。链接动态库时固定该 run 的库搜索路径并核验实际加载对象；使用静态库时记录链接输入的 hash。不要为了第一阶段引入不必要的构建系统改造。

## 4. 阶段 1：单链接 baseline 跑通

### 4.1 本阶段唯一主 case

`B1: links=1, mode=plain, chunk_items=30, scatter=pipeline, notify=send`。

每轮：600 次异步 `Put(1024)`，每 30 次后同一 QP 追加一次 `Send(DATA_READY)`；接收者每个 chunk 做 30 次 memcpy；全部 20 chunk scatter 后发 1 个 ACK。

预期每轮 600 个数据 WR、20 个通知 WR、1 个反向 ACK WR；有效数据 614400 字节。第一版即可记录 `submit_us`、`e2e_us` 和整体有效带宽，为下一阶段提供稳定参考。

### 4.2 实现步骤

1. 实现参数解析、单 service 的 Create/Bind/Start/Connect、handler 和错误退出。先用 HELLO/Reply 确认同一实际 RDMA 设备上的链接建立成功。
2. 分配源、staging、dst，触页、注册源/staging MR、交换地址和 key。接收端所有初始化完成后才返回 READY。
3. 根据设计预生成 600 个 Put 描述符、20 个 notice；实现 generation、chunk readiness、本地完成累计计数和 ACK 状态。
4. 实现异步 Put + Send 提交循环、接收 CQ 发布 ready、应用线程 scatter、ACK、等待与 drain。不要用空 callback 指针模拟异步。
5. 实现 verify/warmup/measure 三个阶段；generation 连续递增，正式数据和描述符提前准备，日志输出在测量之后。
6. 交付 `run.py`、配置示例和 README，必须同时提供手工双端命令与脚本命令。

### 4.3 验证顺序与通过条件

| 检查 | 操作 | 通过条件 |
|---|---|---|
| 本地逻辑 | 验证 600 块分区、20 chunk、地址边界、消息编码和错误参数 | 无越界/尾块/编码错误；用小型自检即可，不搭建 mock RDMA 框架 |
| 目标构建 | Linux/AArch64 构建与加载依赖检查 | 有完整命令、退出码、二进制与库标识 |
| 小运行 | 1 个验证轮次 | 首次建链、写入、20 通知、scatter、ACK、退出均成功 |
| 正确性 | 20 轮，每个 word 含 generation/block/word 模式 | 全部有效数据及间隙哨兵正确，通知不丢失、不重复 |
| 复用 | verify 模式延迟 scatter；观察 generation | 下一轮数据提交不早于本轮 ACK；无覆盖 |
| 失败退出 | 接收进程中途退出或连接无法建立 | 有界退出、非零状态，脚本保存错误，不输出有效带宽 |
| 性能 | warmup=1000，measure=10000，repeat=5，trace 关闭 | 五次有效运行；报告中位数及波动，不能预设目标带宽 |

每次独立正式运行都先做正确性验证；若正式阶段轮数很少或运行时间过短，在报告中标出，并统一增加轮数重测。

阶段 1 交付：可构建代码、可用脚本、README、B1 原始结果与报告、PROGRESS 状态。没有硬件时交付上述可完成部分和准确的待执行命令，性能字段保留未测。

## 5. 阶段 2：双链接、性能对比与打点

### 5.1 双链接实现

1. 将单 rail 状态放入长度不超过 2 的数组；每个 rail 独立 service、设备、MR、channel、就绪和 ACK 状态。
2. 每条分配 300 块，global_block_id 为 `rail*300 + local_id`。块总量、stride 和有效数据量保持不变。
3. 一个提交线程按 chunk 在 rail0/rail1 之间交替提交；每条 10 chunk。一个 scatter 线程轮询两条 rail，单轮扫描每 rail 最多消费一个 chunk。
4. 等待两条 rail 的 ACK 和本地完成后才能开始下一轮；不能将 rail0 ACK 当整轮完成。
5. 保留 `--links 1` 路径，使用同一个二进制重测 B1 与 B2。不能只用阶段 1 的历史 B1 数字与修改后的 B2 比较。

预期 B2 每条 300 数据 WR + 10 通知 WR + 1 个反向 ACK；两条总数据 WR 仍是 600，总通知仍是 20。

### 5.2 打点契约

第一阶段基本指标继续保留。新增 `trace` 诊断模式，记录最多固定数量轮次（建议 64 轮）的事件；正式 measure 默认关闭详细 trace。事件写入预分配数组，运行结束后批量输出，禁止在 callback 中逐条打印。

| 主机/线程 | 时间点 | 精确定义 |
|---|---|---|
| sender 应用 | S0 | 本轮首个数据 API 前 |
| sender 应用 | S_post[r,c] | 对应 chunk 的数据和通知 API 均返回成功后 |
| sender 应用 | S1 | 本轮最后一个通知提交成功后 |
| sender CQ | S_data[r] | 该 rail 本轮最后一个数据 API callback 成功的时刻 |
| sender 接收 callback | S_ack[r] | 收到并校验该 rail 本轮 ACK 后 |
| sender 应用 | S2 | 全部 ACK 与本地 data/Send 完成均已满足 |
| receiver CQ | R_ready[r,c] | 成功收到并校验 chunk 通知，发布 ready 之前 |
| receiver scatter | R_begin[r,c] | 对应 chunk 第一条 memcpy 前 |
| receiver scatter | R_end[r,c] | 对应 chunk 最后一条 memcpy 后 |
| receiver 应用 | R_ack[r] | 该 rail 的 ACK Send 调用前 |

每条记录带 `host_role, case, generation, rail, chunk, event, timestamp_ns`。用同机 `CLOCK_MONOTONIC_RAW` 或一致的单调时钟；不得拿不同机器的 timestamp 相减。

打点对象与并发要求：

- CQ 线程先写 `R_ready` 再 release 发布 ready，scatter acquire 后读取；不要反过来发布状态再填时间戳。
- 完成计数到达阈值与对应时间戳发布必须一致；例如最后 callback 写好时间戳后再发布独立的 trace-ready 标记，不能让统计线程先看到“完成”而读取未初始化时间戳。
- 按采样 generation 预分配记录，不循环覆盖尚未读取的 trace 槽；不同线程使用各自字段或数组，不同时 append 同一个 vector。
- trace 模式仅有诊断轮次，保证 generation 和回调生命周期仍遵循正常协议。新增 trace 不应改变非 trace 的协议、提交顺序和线程数。

可从这些打点得到：

```text
sender submit        = S1 - S0
sender local finish  = max(S_data[r]) - S0
sender ACK observed  = max(S_ack[r]) - S0
sender e2e           = S2 - S0
sender post-submit wait = S2 - S1
receiver scheduling  = R_begin[r,c] - R_ready[r,c]
receiver scatter     = R_end[r,c] - R_begin[r,c]
receiver scatter sum = sum(receiver scatter)
rail ACK imbalance   = abs(S_ack[0] - S_ack[1])
```

这些区间会重叠，**不能把 submit、local finish、ACK observed 和 scatter sum 直接相加作为 e2e 分解**。本地 callback 可以在全部请求提交完之前发生，不要假定其时间点必定晚于 S1。

`R_begin[first] < max(R_ready)` 可证明应用 scatter 在最后一个 chunk 通知处理完成前已经开始；这是软件观察到的流水证据，不能单凭它声称准确测出了 NIC DMA 与 CPU 的重叠时长。未同步跨机时钟时，不输出“单向网络时延”；ACK 区间也包含网络、CQ 调度和接收者处理。

### 5.3 验证和比较

1. 重跑阶段 1 正确性与 B1；再做 B2 的完整校验和延迟单 rail 的验证，确认快 rail 仍能 scatter。
2. 记录实际设备、端口、QP 对应关系，以及两张网卡测试前后端口计数。只有建立两个 channel 的日志不足以证明双 NIC 正常承载数据。
3. 关闭 trace，在同一配置/同一二进制下交替运行 B1/B2，各 5 次；下一次 repeat 反转顺序，减少温度/频率与运行顺序偏差。
4. 单独开启 trace，再运行 B1/B2，产出上述指标的分位数或分布摘要；记录 trace 对 e2e 的影响，不将它混入正式对照表。
5. 提交对比表：B1/B2 的 e2e p50/p95/p99、submit p50、有效 GB/s、CPU 使用量、两条 ACK 不平衡和 receiver scheduling/scatter 的诊断结果。

```text
latency_speedup = median(B1 每次运行的 e2e_p50) / median(B2 每次运行的 e2e_p50)
bandwidth_ratio = median(B2 每次有效 GB/s) / median(B1 每次有效 GB/s)
```

聚合方式在报告中固定；不要一组平均值、另一组最优值。不要求 B2 必须更快。若更慢或无收益，结合打点、CPU 与 NIC 证据分析，标清“观察事实”和“待验证解释”；不为追求 2 倍收益直接改成两个应用提交线程。

阶段 2 通过条件：B1/B2 都正确、实际双 NIC 证据充分、两组性能原始数据齐全、打点可解释且无跨机器时间相减。单纯多建一条链不算完成本阶段。

## 6. 阶段 3：RDMA SGL 版本

### 6.1 实现步骤

1. 保留 plain 分支，增加 `mode=sgl`、`sgl_items=8|16|30`；每 chunk 指向预生成描述符中的连续 iov 段。
2. 每个 iov 的源地址仍按 4096 stride 分散，目标地址按 1024 连续，key 相同；一次异步 PutV 替代 chunk 内的 K 次 Put，通知 Send 和 scatter 协议不变。
3. 核实每个参与 QP 的实际 `max_send_sge`。不足则明确 SKIP；实际能力暂时无法核实应标 pending，不能只读公共常量 30。
4. 处理尾块，按实际地址数和实际字节发通知、scatter 与校验。
5. 增加 `scatter=after-all`：接收同样的 chunk 通知，等全部到齐后才 scatter，不改变通知数量。
6. 保留阶段 2 的打点，适配 chunk 数量即可；不要新增另一套时钟和指标。

| case | 数据 WR/轮 | Send 通知/轮 | 每 rail 尾块有效地址数 |
|---|---:|---:|---|
| S1-8 | 75 | 75 | 8，整除 |
| S1-16 | 38 | 38 | 8 |
| S1-30 | 20 | 20 | 30，整除 |
| S2-8 | 76 | 76 | 4 |
| S2-16 | 38 | 38 | 12 |
| S2-30 | 20 | 20 | 30，整除 |

数据 WR 数以一个 chunk 确实合并成一个 WR 为前提。双链接 SGL=8 是 76，不是 75。

### 6.2 必做验证

- 所有硬件支持的配置先运行完整内容校验，重点验证尾块长度、global block 编号、staging 边界和 dst 间隙。
- 在独立短诊断中观察 `PostOneSideSglGrouped`：`groupCount=1`、WR opcode=WRITE、`num_sge` 为正确的 K/尾块数、目标地址和 QP 正确。保留证据，正式测量移除诊断开销。
- 用同一二进制重跑 B1/B2，加 6 个 SGL case 和 `S2-30-off`，共 9 个主 case；不支持的项附证据 SKIP。
- 对 `S2-30` 与 `S2-30-off` 做 trace 对照，证实消费时机改变且通知数量相同。若能力不支持 30，选择最高支持的 K 做 on/off 附加对照并明确命名，原 30 项仍 SKIP。
- SGL=8/16 同时改变 WR 和通知粒度，报告中说明这是综合配置对比。需要严格分离变量时再加 plain chunk=8/16，不默认展开无关矩阵。

重点分析：相对同链接数 baseline 的 e2e/submit 收益、receiver scatter 时间是否接近、CQ 排队变化、SGL=30 是否受提交内部开销或 CPU scatter 限制。callback 次数减少也是当前 hcom 路径的真实收益，但不能全部归为网卡 gather。

阶段 3 通过条件：支持的 SGL 配置正确、尾块正确、单 chunk/单 WR 核验、B1/B2 无回归、on/off 有明确数据与解释。仅成功调用 PutV 不算验证了 RDMA SGL 合并。

## 7. 阶段 4：WRITE_WITH_IMM 优化

### 7.1 收窄接口再逐层实现

先保存阶段 3 的代码、二进制/库标识与结果。新增最小 C++ `PutVWithImm(req, token, local_done)` 和独立接收 handler，严格限定当前用例：RDMA、worker poll、连续远端、同 rkey、一个 chunk 合为一个 WR。

1. 新增显式 immediate 参数的内部传递路径：service/channel → endpoint → worker → QP。既有 `upCtxData` 保留 service 本地上下文语义。
2. 在已经构造好的唯一数据 WR 上设置 `IBV_WR_RDMA_WRITE_WITH_IMM` 与 `htonl(token)`，num_sge 不增加。维持本地 signaled 完成及原上下文回收。
3. 在成功接收 CQE 路径按 `IBV_WC_RECV_RDMA_WITH_IMM` 分流，检查 `IBV_WC_WITH_IMM`，用 `ntohl` 取 token。
4. 复用已有接收 WQE 并持续补投，先复制事件再回收/补投 context；不从普通接收 buffer 解析 WRITE 数据。该 RQ 仍接收普通 Send，保留其正常接收缓冲。
5. 将事件上送为 `channel/token/byte_len`，复用 benchmark 的 ready 发布与 scatter。不能让此 CQE 落入当前普通/RAW 消息解析。
6. benchmark 增加 `notify=imm`：一次 PutVWithImm，删除该 chunk 的额外 DATA_READY Send；本地 Send 完成期望数相应减少。ACK、布局、校验和打点口径不变。
7. 全部相关类型与库两端重编。新增 C++ 虚接口也影响 ABI；不要以“没改请求结构”为由忽略 ABI。不同后端明确 unsupported，旧 Send/PutV 保持正常。

token 使用设计约定：`(generation << 8) | chunk_id`，24 位 generation、8 位 chunk，rail 由 channel 得到。运行前检查 verify+warmup+measure 总代数不会耗尽有效 generation；不允许回绕。

本阶段首版只为 SGL 路径使用 IMM。plain baseline 的 WRITE+Send 保留，便于回归；不必顺带增加单块 PutWithImm、C API、Read/GetV 或非 RDMA 后端能力。

### 7.2 专项验证：先完成事件正确，再跑性能

| 验证 | 要求 |
|---|---|
| 单 chunk smoke | 支持能力内的 K 个源 SGE，远端数据正确，恰好一次 IMM 接收事件 |
| 30 SGE 边界 | 硬件支持时 num_sge=30，未增加第 31 个 SGE |
| 事件与长度 | receiver opcode/flags/token/channel/byte_len 正确；长度等于该唯一 WRITE 的字节数 |
| 网络字节序 | 编码/解码已知 token，真实 CQ 与应用 token 一致；旧 Send immediate 解释未被全局改写 |
| 接收信用 | 连续通知总数远大于预投数量，仍正常运行，证明持续补投；诊断记录接收消费/补投匹配 |
| 共存 | HELLO/ACK/FINISH 等普通 Send 与 WRITE_WITH_IMM 共用连接，不误入 RAW/普通头解析 |
| 复用/尾块 | 沿用 generation 校验、延迟 scatter 与 8/16 尾块校验 |
| 错误路径 | post 失败、QP 错误/超时或断链时有界退出，无双重回收；能用现有 UT 验证则增加针对性测试 |
| 非支持输入 | 非 RDMA、不连续远端、超实际 SGE 上限等被明确拒绝，不能悄悄换成旧通知 |

接收信用测试可以在独立验证运行调低预投数量到库允许的范围，并连续跑大量轮次；不在正式测量里人为饿死 RQ。没有硬件时只能检查编码、构造与分流逻辑，不能宣称补投、RNR 或 DMA 可见性已经验证。

### 7.3 性能归因

同一份改造库、同一 binary、相同队列/线程配置，对每个支持的 SGL case 成对运行 `notify=send` 与 `notify=imm`，各 5 次；详细打点另跑。

以 S2-30 为例：

| 工作量/轮 | Send 通知版本 | IMM 版本 |
|---|---:|---:|
| 数据方向 WR | 20 WRITE + 20 Send | 20 WRITE_WITH_IMM |
| receiver chunk 就绪 CQE | 20 | 20 |
| CPU memcpy | 600 | 600 |
| 反向 ROUND_ACK | 2 | 2 |

报告 e2e、submit、CPU、receiver scheduling/scatter，以及独立通知 API/WR 被消除后的收益。接收 CQE、RQ 信用和 scatter 都仍然存在，不将 IMM 描述成没有接收开销。

另用改造库的 `notify=send` 路径重跑 B1/B2 和 SGL，与阶段 3 检查回归。若改造时还做了单组快路径、callback 池等优化，拆分变更和结果，不能合并声称都是 IMM 收益。

阶段 4 通过条件：新事件链路及接收补投正确、旧路径回归通过、全部支持配置有成对真实结果。更快不是硬性验收条件；正确且没有收益也必须如实报告。

## 8. 每阶段交付报告模板

```text
阶段：
实现状态 / 验证状态：
源码提交或 diff 标识；两端程序与库 hash：
本阶段改了什么；保留了哪些比较条件：
已执行命令、退出码、机器与设备：
正确性检查及证据路径：
性能原始数据路径、重复次数、计时/trace 模式：
比较表与统计方法：
结论：观察事实；可能原因；尚未验证事项：
失败或 SKIP 的 case 和依据：
下一阶段所需输入或遗留问题：
```

阶段性报告应能让另一个模型仅靠文件继续工作，不依赖聊天记录。每个性能结论都能追溯到命令、配置和原始结果；未跑的表格填写“未测”，不要填 0。

## 9. 可直接复制给其它模型的阶段任务

以下提示词中的目录需替换为实现机器的实际路径。收到提示词后先读取已有 PROGRESS，避免重新覆盖已完成工作。

### 阶段 1 提示词

> 请读取 perf_test/DESIGN_CN.md 和 perf_test/IMPLEMENTATION_PLAN_CN.md，实施阶段 1：单 RDMA 链接 baseline。只使用当前 ubs-comm，循环异步 Put 600 个 1 KiB 块，每 30 块在同一真实 QP 上 Send 通知，接收端流水 scatter，轮次 ACK 后再复用。交付最小 C++ 程序、CMake、可运行的 Python 跑测脚本、配置示例、README 和 PROGRESS。先验证完整内容与缓冲生命周期，再运行 baseline，保存真实日志和性能结果。不要实现双链接、SGL 或修改 IMM。没有目标 RDMA 硬件时完成可执行的本地工作并标明硬件验证 pending，禁止虚构结果。

### 阶段 2 提示词

> 请先读取两份设计/实施文档、PROGRESS 和阶段 1 结果，在已有 baseline 上实施阶段 2：两个 service 分别绑定两张 RDMA 网卡，每个 channel 一条 QP，每条传 300 块，总数仍是 600。保持单应用提交线程、单 scatter 线程和相同完成口径，新增详细诊断打点，按任务书定义记录提交、本地完成、chunk 就绪、scatter 和 ACK。交付同一二进制下 B1/B2 的真实对照结果、独立 trace、双 NIC 流量证据与报告。详细打点关闭时才进入正式性能主表，不跨机器相减时间戳。保持前一阶段可运行，本阶段不实现 SGL/IMM。

### 阶段 3 提示词

> 请基于已完成的前两阶段实施 SGL：使用当前 worker-poll PutV 合并路径，测试 8/16/30 个分散源地址写入连续 staging，正确处理尾块，保留按 chunk Send 通知和流水 scatter。核实实际 QP SGE 能力，并在短诊断中证明每 chunk 一个 WR；不支持项明确 SKIP。运行任务书的 9 个主 case，包括 baseline 回归和 S2-30 关闭流水对照，保存正确性、WR 核验、性能与打点证据。不要将不同通知粒度的综合收益全部归为 SGL。本阶段不实现 WRITE_WITH_IMM。

### 阶段 4 提示词

> 请基于阶段 3，在独立可审阅变更中最小改造 ubs-comm，增加只面向 RDMA worker-poll、连续远端、单 WR 的 PutVWithImm 和接收事件。按文档传递 token、设置 WRITE_WITH_IMM、处理接收 CQ opcode/flags、持续补投接收 WQE并保持旧 Send 协议。benchmark 仅替换 chunk 通知，保留 scatter 和轮次 ACK。先通过事件、长度、尾块、接收补投、普通 Send 共存和错误路径验证，再用同一改造库成对跑 Send/IMM，回归 baseline 和旧 PutV。报告原始性能结果及归因；两端全部重编并记录 ABI/库版本，不夹带其它性能优化。
