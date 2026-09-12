# 分阶段实施与验证任务书：ubs-comm RDMA 600 × 1 KiB

状态：阶段 1 direct B1 已实现；阶段 1.5、2、3、4 尚未实现。目标 Linux/RDMA 构建与硬件跑测尚未执行。更新日期：2026-09-12。当前实现参考 perf_test `0f5e382`。

配套设计：[DESIGN_CN.md](C:/code/RDMA_DEMO/perf_test/DESIGN_CN.md)。本文规定实施顺序、交付物和阶段验收，协议与内存布局以设计文档为准。执行模型若发现两份文档与源码存在冲突，应说明依据并记录最小修正，不应默默改变测试口径。

原始代码基线：ubs-comm `oneside-msge-merge`，`9e4c035a5d68ccca02d05fade3b6f5907db24ef4`。

本机项目位置为 `C:/code/RDMA_DEMO`；实际构建、跑测在两台目标 Linux RDMA 主机完成。文中的 `PERF_ROOT`、`UBS_ROOT` 是各自主机上的绝对目录，由实施者根据实际环境指定，不照搬 Windows 路径。

## 1. 交给实施模型的总约束

1. 先读设计文档、本文及工作目录适用的 `AGENTS.md`，查看现有代码与 git 状态，保留已有修改。先复用已有阶段产物，再继续实现。
2. 实现一个小型 C++ 可执行程序，不引入 memfabric，不搬入整个 hcom perf 框架。Python 只用标准库在各自主机启动一个角色并负责本机结果归档。
3. 顺序为：**阶段 1 单链接 direct baseline → 阶段 1.5 热路径优化 → 阶段 2 direct 双链接与打点 → 阶段 3 SGL＋scatter → 阶段 4 WRITE_WITH_IMM**。每阶段保持前一阶段用例可运行，逐阶段形成可独立审阅的 diff 和报告。
4. 两端总有效数据固定为每轮 600 × 1024 字节。阶段 1 的源/最终目标均为 stride=4096；第 `i` 个 Put 直接写 `dst + i*4096`，最终完成口径包含一个轮次完成通知和 ACK，不包含 staging 或 scatter。
5. 所有主 case 使用异步提交；一个 service 对应一个 RDMA 设备，每个 channel `linkCount=1`、worker poll、关闭内建 multirail；阶段 1 每端各一个应用线程。
6. 阶段 1/1.5/2 不使用 `chunk_items`、chunk 通知或 scatter。阶段 3 的 SGL＋scatter 与 direct 完成相同最终任务，主表比较整体策略 e2e；内部开销不同，不能把全部收益称为纯 SGL 合并收益。只有需要单独归因时才增加 plain-staged 诊断对照。
7. 阶段 1 单轮在途：同一 QP 上的 600 次 WRITE 后只发一个 `ROUND_READY` Send，再等待 ACK。实现局部函数、固定数组和必要的状态，不增加跨轮窗口、通用线程池、动态调度、自动重连或 selective signaling。
8. 第一至第三阶段优先只改 `perf_test`；第四阶段改 ubs-comm。若前期发现库缺陷确实阻塞正确性，单独提交最小修复并重新跑 baseline，不将修复收益归入双链接/SGL/IMM。
9. API 返回码、本地 callback 错误、接收消息合法性、超时、缓冲复用和退出 drain 都必须处理。不要为“代码最少”省略这些正确性条件。
10. 硬件结果只来自真实运行。编译通过、mock 通过、脚本可启动都不等于 RDMA 验证通过；不得生成示意数字冒充测量结果。

每次只交办一个阶段时，实施模型完成该阶段及可执行验证后交付，不擅自扩展到下一阶段。若收到整体实施指令，按 1 → 1.5 → 2 → 3 → 4 推进；阶段验收是技术条件，不是额外的人工审批流程。

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
  results/<run-id>-<role>/   当前两端分别保存，不是一个目录自动汇总两端
    manifest.json             参数、版本、库路径、构建、机器元信息
    <role>.stdout.log
    <role>.stderr.log
    result.jsonl              sender 端结果；receiver 不生成这份记录
    trace.jsonl               阶段 2 计划：单独诊断运行的打点
    REPORT.md                 当前 sender 报告；跨运行比较由实施者汇总
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

阶段 1 已实现下述每主机本地角色脚本接口，尚未在目标机跑测。阶段 1.5 复用 `--suite stage1`，通过二进制/提交/hash 区分优化版本，不增加一套启动器。后续阶段的参数扩展仍为计划，实际已支持命令以 README 为准。

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

上例的两条 rail 是阶段 2 的配置草案；当前 stage1 配置只使用一条，按仓库 hosts.example.json 填写（包括 stage1 轮数/超时设置）。阶段 2 才扩展并验证第二张 NIC、第二个端口和第二个 worker CPU，不能暗示当前解析器已经支持它们。应用不假定 OOB IP 就是 RDMA IP。

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
- 两端本地脚本分别检查自己启动的进程退出码；sender 校验结果记录。当前脚本不自动收集另一台机器的退出码，人工汇总或外部调度器必须确认双方成功后才接受整次跑测。缺少记录、重复记录或参数不匹配均失败，禁止拿旧文件代替。
- 失败/超时只清理当前主机、本次调用启动的本地进程组；不能 `pkill` 全部同名程序。
- 第一版可以要求二进制提前部署，不自动编译/拷贝/安装系统依赖。README 给出手工部署和两终端运行方法，便于排除包装脚本问题。
- 当前脚本记录本机 binary hash、命令、ldd 与本地退出状态；性能对比时另行保存静态 hcom 链接输入 hash、源码 commit/dirty diff，人工对齐两端构建。不要把尚未自动收集的证据描述为脚本已有能力。

构建依据：[ubs-comm build.sh](C:/code/RDMA_DEMO/ubs-comm/build.sh) 支持 service/RDMA 构建开关，默认产物位于 `dist/hcom`；[现有 perf CMakeLists.txt](C:/code/RDMA_DEMO/ubs-comm/test/hcom/tools/perf_test/CMakeLists.txt) 可参考 include、hcom、boundscheck 等依赖。实施者需核实实际产物，不凭记忆杜撰 include 路径或库名。

当前 CMakeLists 已定义 `HCOM_INCLUDE_DIR/HCOM_LIB_DIR`，构建命令如下；也可使用现有 build.sh，具体依赖路径以 README 为准：

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

`B1: links=1, mode=plain, direct dst stride=4096, tls=disabled`。

每轮：600 次异步 `Put(1024)`，第 `i` 次写入最终 `dst + i*4096`；随后同一 QP 追加一次 `Send(ROUND_READY)`。verify 轮先完整检查 dst 再发本轮 ACK；warmup/measure 轮收到 ROUND_READY 就发本轮 ACK，不逐轮扫描。全部 measure 轮结束后才做最终内容检查，成功后 FINISH_ACK；该扫描不进入每轮 e2e，失败仍使 run 无效。没有 stage 和 CPU scatter。

预期每轮 600 个数据 WR、1 个 `ROUND_READY` WR、1 个反向 ACK WR；有效数据 614400 字节。HCOM 在 `Start()` 前必须显式 `SetTlsOptions(enableTls=false)`，避免默认 TLS 初始化。第一版即可记录 `submit_us`、`e2e_us` 和整体有效带宽，为下一阶段提供稳定参考。

### 4.2 实现步骤

1. 实现参数解析、单 service 的 Create/Bind/Start/Connect、handler 和错误退出；在 `Start()` 前禁用 TLS。先用 HELLO/Reply 确认同一实际 RDMA 设备上的链接建立成功。
2. 分配源和最终 dst，触页、注册源/dst MR、交换 dst 地址和 key。接收端所有初始化完成后才返回 READY。
3. 根据设计预生成 600 个 Put 描述符；每个远端地址为 `peer_dst + i*4096`。实现 generation、单个 round-ready 状态、本地完成累计计数和 ACK 状态。
4. 实现异步 Put + 单个 ROUND_READY Send 提交循环、接收 CQ 发布 round ready、verify 校验、ACK、等待与 drain。不要用空 callback 指针模拟异步。
5. 实现 verify/warmup/measure 三个阶段；generation 连续递增，正式数据和描述符提前准备，日志输出在测量之后。
6. 交付 `run.py`、配置示例和 README，必须同时提供手工双端命令与脚本命令。

### 4.3 验证顺序与通过条件

| 检查 | 操作 | 通过条件 |
|---|---|---|
| 本地逻辑 | 验证 600 块 direct 分区、stride 地址边界、消息编码和错误参数 | 无越界/编码错误；用小型自检即可，不搭建 mock RDMA 框架 |
| 目标构建 | Linux/AArch64 构建与加载依赖检查 | 有完整命令、退出码、二进制与库标识 |
| 小运行 | 1 个验证轮次 | 首次建链、600 次直接写入、1 个 ROUND_READY、ACK、退出均成功 |
| 正确性 | 20 轮，每个 word 含 generation/block/word 模式 | 全部有效数据及间隙哨兵正确，round-ready 不丢失、不重复 |
| 复用 | 观察 generation 和 ACK 栅栏 | 下一轮数据提交不早于本轮 ACK；无覆盖 |
| 失败退出 | 接收进程中途退出或连接无法建立 | 有界退出、非零状态，脚本保存错误，不输出有效带宽 |
| 性能 | warmup=1000，measure=10000，repeat=5，trace 关闭 | 五次有效运行；报告中位数及波动，不能预设目标带宽 |

每次独立正式运行都先做正确性验证；若正式阶段轮数很少或运行时间过短，在报告中标出，并统一增加轮数重测。

阶段 1 交付：可构建代码、可用脚本、README、B1 原始结果与报告、PROGRESS 状态。没有硬件时交付上述可完成部分和准确的待执行命令，性能字段保留未测。

## 4.5. 阶段 1.5：direct B1 热路径优化（未实现）

### 1.5.1 目标与不变项

只处理两个有明确代码依据的问题：`WaitUntil` 的逐次 clock/OS yield，以及每个 1 KiB 请求的 callback 分配和过密原子记账。目标是减少应用管理成本，判断它是否限制当前 hcom 穿刺性能；不预先承诺提速比例。

保持：600 次异步 Put、source/dst stride=4096、1 ROUND_READY＋1 ACK、单轮在途、相同线程数量/绑核、队列、TLS、MR、verify 和完成口径。继续使用现有 hcom，每个 WRITE signaled，不改协议版本或引入跨轮窗口。

原始对照记为 B1-original（perf_test `0f5e382` 或明确记录的当前快照）；优化后仍输出 case=B1，以版本/hash 标识。两个子改动分别保留独立 diff：B1-A 仅等待优化，B1-AB 为等待＋记账优化。不必为旧行为永久保留一组运行时开关；可保留三个独立构建产物。

### 1.5.2 优化 A：数据面忙轮询，降低超时检查频率

当前 `WaitUntil` 每次谓词未满足都会检查错误、读取时钟并 `std::this_thread::yield()`。它同时用于 sender 的 round completion、receiver 的 ROUND_READY 和 ACK 本地完成，因此这项开销进入 e2e。

实施方案：

1. 仅数据面等待采用忙轮询；建链、FINISH、错误 drain 保留有界的非性能等待。避免全局替换所有等待函数。
2. 首选固定 `deadline_check_interval=256`（2 的幂），不新增无必要的 CLI 参数。每次 acquire 读取完成状态、快速检查 fatal，计数满 256 次再读时钟。入口和成功返回时检查错误，超时时抛出原有错误。
3. 数据面不逐次调用 OS yield/sleep。可用目标平台轻量 CPU relax 提示；不要将 ARM 的处理器 hint 与 `std::this_thread::yield()` 混为一谈，也不要引入没有明确唤醒机制的 WFE/休眠。
4. 运行前明确 app/worker 使用不同物理核心。配置不满足时拒绝该性能对比或保留旧模式并明确标记，不把共享单核的结果与绑核忙轮询结果混比。
5. 每轮等待仍有 deadline，网络断开和不再发生回调时也必须退出；不能因为移除 yield 就改成无界循环。

```cpp
WaitData(predicate) {
    CheckFatal();
    const auto deadline = steady_now() + timeout;
    uint32_t spins = 0;
    while (!predicate()) {             // 谓词保持 acquire
        if (fatal.load(acquire)) ThrowRecordedFailure();
        CpuRelax();                    // 轻量提示，可为平台上的安全空操作
        if ((++spins & 255) == 0 && steady_now() >= deadline)
            ThrowTimeout();
    }
    CheckFatal();
}
```

先运行 B1-original 与 B1-A，各 5 次，比较 e2e avg/p50/p95/p99、submit p50、有效 GB/s 和 CPU 使用量；独立短诊断可检查 sched_yield 调用与上下文切换，不在正式测量中启用 syscall tracing。忙轮询提高应用 CPU 占用属于预期代价，必须报告。

### 1.5.3 优化 B：按线程所有权精简计数，控制 callback 成本

当前每个数据请求有 1 次 callback new/delete，提交计数 1 次原子 RMW、完成计数 1 次、活动 callback 进入/退出 2 次；600 块合计 600 次分配/释放与 2400 次 demo 原子 RMW，不含 hcom timer/context、CQ 和控制消息。相邻的提交/完成计数还可能在同一 cache line 上被 app 与 CQ worker 反复写入。

先做无需改 hcom 所有权的最小优化：

1. 审核计数的全部读写者。`expected/attempted` 若仅由应用线程提交及同线程错误 drain 访问，改为普通 `uint64_t` 累计值，消除逐 Put 的 atomic fetch_add。不要对 callback 与主线程共享的 completed/fatal 变量照搬此改法。
2. 在调用异步 API **之前**更新本线程 attempted 数，保留当前错误路径的语义。callback 可能在 API 返回前执行，不能假设它只能稍后发生。只有 API 返回成功才推进正常路径的成功提交统计；失败立即进入错误退出，不强行声称 600 个请求已全部成功。
3. 按写入线程组织状态，隔离 app-owned 计数和 callback-owned 完成/活动计数，采用目标平台合适的 cache-line 对齐，不盲目宣称固定 64 字节适用于所有鲲鹏配置。仍保留必要的 release/acquire 发布。
4. 首轮保留每请求 completed 原子更新和 `ActiveCallbackGuard`。`done == expected` 只表示完成计数满足，并不自动证明所有 callback 栈帧已退出；不能为少两条原子指令直接删掉 teardown 的活动保护。
5. 不默认批量发布 completed 到第 600 个才更新，也不改成“只等待最后一个 callback”，避免部分提交失败时 drain 无法看到已完成前缀。如果后续要聚合计数，须单独解决错误/超时 callback 并发和尾部刷新，超出本阶段必做范围。

累计值的最小语义示例：

```cpp
// app 线程独占 attempted；callback 只访问 shared completed/fatal/active
for (block = 0; block < 600; ++block) {
    cb = NewDataCallback();
    if (!cb) FailBeforePost();
    ++attempted_data;                 // 普通整数，不做逐包 atomic RMW
    rc = ch->Put(req[block], cb);
    if (rc != OK) EnterBoundedFailurePath();
}
// 正常全成功时，等待对应累计完成阈值及远端 ACK
// 错误时不可假定失败 API 一定会回调；保留有界 drain，无法确认安全则进程退出
```

callback 动态分配的处理有明确边界：A/B 必做版本继续使用公共 `UBSHcomNewCallback`，保留其删除语义。用独立 profile 观察 allocator、callback、timer/CQ 在 CPU 样本中的占比；若分配没有成为主要成本，记录“保留，未证实值得复杂化”。这不是宣称已消除 600 次分配。

只有 profile 明确指向分配瓶颈才追加 B 的可选复用实验：按单轮最大在途数建立固定容量 callback 存储，显式处理成功 Run 后归还和 hcom 错误路径 delete 后归还，两者只能发生一次；源/状态对象在所有回调退出前存活。未经真实库错误路径所有权核验，不得把栈对象、同一个永久 callback 或普通数组元素直接传入可能 delete 的 API。若需要泛化内存池或改库才能安全完成，作为后续独立优化，不阻塞本阶段最小方案，也不混入阶段 2 双链接收益。

顺手将两个统计数组的 reserve 移到正式 wall 计时前，避免一次性分配混入有效带宽；这项边界修正在报告注明，不当成主要性能发现。

### 1.5.4 验收与交付

| 验证项 | 要求 |
|---|---|
| 协议回归 | original/A/AB 都是 600 WRITE＋1 ROUND_READY＋1 ACK，source/dst 无 scatter |
| 内容与复用 | 20 轮完整 verify、模式切换与最终检查；等待 ACK 和本地完成后才复用 |
| 完成竞态 | 覆盖 callback 早于 API 返回、快速完成和慢 worker；计数不得提前放行 |
| 有界失败 | 拒绝提交/中途断开/无回调时仍有界结束，不释放仍被引用的对象 |
| 原子所有权 | 去掉 atomic 的字段没有跨线程读写；活动保护和完成可见性仍成立 |
| 性能对照 | original/A/AB 各 5 次、统一绑核与库，独立 profile，保留原始结果 |
| 归因 | A 的等待收益与 B 的记账收益分别报告；callback 分配是否优化明确记录 |

交付独立改动、README 中当前生效行为、PROGRESS、三组结果和一份阶段 1.5 报告；代码及本地检查完成但无硬件时记为 IMPLEMENTED / LOCAL_PASS_HW_PENDING，不能预填提速数据。保留原版供比较；若某项优化无收益或回退，说明证据，阶段 2 选用经验证的共同实现，不强行叠加退化改动。

## 5. 阶段 2：在阶段 1 direct 基础上增加双链接并测量收益

### 5.1 双链接实现

本阶段的唯一数据路径变化是 links=1 → links=2。沿用阶段 1 的 direct 地址布局、每轮通知/ACK、单轮在途和校验逻辑；阶段 1.5 中采用的优化同时用于 B1 与 B2。不能把未优化的阶段 1 历史 B1 与已优化的 B2 直接比较并称为“双链接收益”。独立 trace 是诊断功能，正式性能关闭。

1. 将单 rail 状态放入长度不超过 2 的数组；每个 rail 独立 service、设备、MR、channel、就绪和 ACK 状态。
2. 每条分配 300 块，global_block_id 为 `rail*300 + local_id`。块总量、stride 和有效数据量保持不变。
3. 一个提交线程按块交替提交：Put(rail0,i)、Put(rail1,i)，i=0…299；完成提交后每条 rail 各发一个 ROUND_READY。receiver 不增加 scatter 线程，独立检查两条 rail 的 ready 并异步发各自 ACK，不能因等 rail0 ready 或 ACK 本地完成而阻塞 rail1 的通知处理。
4. 等待两条 rail 的 ACK 和本地完成后才能开始下一轮；不能将 rail0 ACK 当整轮完成。
5. 保留 `--links 1` 路径，使用同一个二进制重测 B1 与 B2。不能只用阶段 1 的历史 B1 数字与修改后的 B2 比较。

预期 B2 每条 300 数据 WR + 1 个 ROUND_READY WR + 1 个反向 ACK；两条总数据 WR 仍是 600，总通知为 2。

### 5.2 打点契约

第一阶段基本指标继续保留。新增 `trace` 诊断模式，记录最多固定数量轮次（建议 64 轮）的事件；正式 measure 默认关闭详细 trace。事件写入预分配数组，运行结束后批量输出，禁止在 callback 中逐条打印。

| 主机/线程 | 时间点 | 精确定义 |
|---|---|---|
| sender 应用 | S0 | 本轮首个数据 API 前 |
| sender 应用 | S_post[r] | 对应 rail 的 Nrail=600/L 个数据和 ROUND_READY API 均返回成功后（B1 为 600，B2 为 300） |
| sender 应用 | S1 | 本轮最后一个 ROUND_READY 提交成功后 |
| sender CQ | S_data[r] | 该 rail 本轮最后一个数据 API callback 成功的时刻 |
| sender 接收 callback | S_ack[r] | 收到并校验该 rail 本轮 ACK 后 |
| sender 应用 | S2 | 全部 ACK 与本地 data/Send 完成均已满足 |
| receiver CQ | R_ready[r] | 成功收到并校验该 rail ROUND_READY，发布 ready 之前 |
| receiver 应用 | R_ack[r] | 该 rail 的 ACK Send 调用前 |

每条记录带 `host_role, case, generation, rail, event, timestamp_ns`。用同机 `CLOCK_MONOTONIC_RAW` 或一致的单调时钟；不得拿不同机器的 timestamp 相减。

打点对象与并发要求：

- CQ 线程先写 `R_ready` 再 release 发布 ready，receiver 应用线程 acquire 后读取；不要反过来发布状态再填时间戳。
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
rail ACK imbalance   = abs(S_ack[0] - S_ack[1])
```

这些区间会重叠，**不能把 submit、local finish 和 ACK observed 直接相加作为 e2e 分解**。本地 callback 可以在全部请求提交完之前发生，不要假定其时间点必定晚于 S1。

direct B2 没有 receiver scatter 或 CPU 重叠指标。未同步跨机时钟时，不输出“单向网络时延”；ACK 区间包含网络、CQ 调度和接收者处理。

### 5.3 验证和比较

1. 重跑采用相同阶段 1.5 设置的 B1 正确性与性能；再做 B2 的完整校验和延迟单 rail 的验证，确认 ACK 栅栏和各 rail generation 正确。若阶段 1.5 尚未验收，B1/B2 都保留相同未优化实现并标明，不能只优化其中一组。
2. 记录实际设备、端口、QP 对应关系，以及两张网卡测试前后端口计数。只有建立两个 channel 的日志不足以证明双 NIC 正常承载数据。
3. 关闭 trace，在同一配置/同一二进制下交替运行 B1/B2，各 5 次；下一次 repeat 反转顺序，减少温度/频率与运行顺序偏差。
4. 单独开启 trace，再运行 B1/B2，产出上述指标的分位数或分布摘要；记录 trace 对 e2e 的影响，不将它混入正式对照表。
5. 提交对比表：B1/B2 的 e2e p50/p95/p99、submit p50、有效 GB/s、CPU 使用量、两条 ACK 不平衡和 receiver ready/ACK 的诊断结果。

```text
latency_speedup = median(B1 每次运行的 e2e_p50) / median(B2 每次运行的 e2e_p50)
bandwidth_ratio = median(B2 每次有效 GB/s) / median(B1 每次有效 GB/s)
```

聚合方式在报告中固定；不要一组平均值、另一组最优值。不要求 B2 必须更快。若更慢或无收益，结合打点、CPU 与 NIC 证据分析，标清“观察事实”和“待验证解释”；不为追求 2 倍收益直接改成两个应用提交线程。

阶段 2 通过条件：B1/B2 都正确、实际双 NIC 证据充分、两组性能原始数据齐全、打点可解释且无跨机器时间相减。单纯多建一条链不算完成本阶段。

## 6. 阶段 3：RDMA SGL 版本

### 6.1 实现步骤

阶段 3 为把多个 iov 合并为一个 WRITE，需要连续远端地址；这不代表所有 PutV 输入都必须连续。SGL＋staging/scatter 与 direct 最终都更新相同的 600 个分散目标块，主实验比较两种策略的整体 e2e，验证减少 WR 是否足以抵消额外拷贝/通知成本。不要把整体差值全部归因于纯 SGL。需要分离归因时再增加同 staging、同 K、同通知/ACK 的 plain-staged 对照；它是可选诊断，不是阶段 3 的实现前置条件。

1. 保留 direct `mode=plain`，增加 `mode=sgl`、`sgl_items=8|16|30`；每 chunk 指向预生成描述符中的连续 iov 段。按需实现 `mode=plain-staged`，不将该模式混入 direct B1/B2。
2. 每个 iov 的源地址仍按 4096 stride 分散，目标地址按 1024 连续，key 相同；一次异步 PutV 替代 chunk 内的 K 次 Put，通知 Send 和 scatter 协议不变。
3. 核实每个参与 QP 的实际 `max_send_sge`。不足则明确 SKIP；实际能力暂时无法核实应标 pending，不能只读公共常量 30。
4. 处理尾块，按实际地址数和实际字节发通知、scatter 与校验。
5. 增加 `scatter=after-all`：接收同样的 chunk 通知，等全部到齐后才 scatter，不改变通知数量。
6. 复用阶段 2 的时钟与 trace 存储，在 staged 分支新增每 chunk 的 ready/scatter_begin/scatter_end；direct B1/B2 不产生 scatter 指标。以同机区间观察就绪到消费的延迟与 memcpy 时间，不用跨机相减冒充网络时延。

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
- 用同一二进制运行 direct B1/B2、6 个 SGL case 和 `S2-30-off`，共 9 个主 case，比较整体方案并回归 direct。不支持的项附证据 SKIP；plain-staged 按需追加并单列。
- 对 `S2-30` 与 `S2-30-off` 做 trace 对照，证实消费时机改变且通知数量相同。若能力不支持 30，选择最高支持的 K 做 on/off 附加对照并明确命名，原 30 项仍 SKIP。
- SGL=8/16/30 同时改变 WR 和通知粒度，报告中说明这是综合配置对比。需要严格分离变量时加对应 `plain-staged(L,K)`，其每轮仍为 600 数据 WR、`L*ceil((600/L)/K)` 个通知，不默认展开无关矩阵。

重点分析：相对同链接数 direct 的 e2e/submit 收益能否覆盖新增 scatter/通知成本，SGL=30 是否受提交或 CPU scatter 限制；若增加 plain-staged，再分离合并路径收益。callback 次数减少也是当前 hcom 路径的真实收益，但不能全部归为网卡 gather。保持各组已验证的阶段 1.5 等待/记账实现一致。

阶段 3 通过条件：支持的 SGL 配置正确、尾块正确、单 chunk/单 WR 核验、direct B1/B2 无回归，整体对比及 on/off 有明确数据与解释。只有声称纯合并收益时才要求对应 plain-staged 证据。仅成功调用 PutV 不算验证了 RDMA SGL 合并。

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

> 请读取 perf_test/DESIGN_CN.md 和 perf_test/IMPLEMENTATION_PLAN_CN.md，实施阶段 1：单 RDMA 链接 direct baseline。只使用当前 ubs-comm，循环异步 Put 600 个 1 KiB 块，第 i 块直接写最终 `dst + i*4096`；600 个 Put 后在同一真实 QP 上仅 Send 一次 ROUND_READY，receiver 校验最终 dst 后发 ACK 再复用。不得分配 staging 或做 CPU scatter；在 `Start()` 前显式关闭 TLS。交付最小 C++ 程序、CMake、可运行的 Python 跑测脚本、配置示例、README 和 PROGRESS。先验证完整内容与缓冲生命周期，再运行 baseline，保存真实日志和性能结果。不要实现双链接、SGL 或修改 IMM。没有目标 RDMA 硬件时完成可执行的本地工作并标明硬件验证 pending，禁止虚构结果。

### 阶段 1.5 提示词

> 请读取两份设计/实施文档、PROGRESS及当前代码，实施阶段1.5，保持direct B1的600个Put、1个ROUND_READY/ACK、单轮在途和线程/队列配置不变。优化A只改数据面等待：绑核下忙轮询、默认每256次检查deadline，去掉逐次OS yield，保留错误和超时。优化B按线程所有权把应用私有提交计数改普通累计值、隔离跨线程写入缓存行，保留共享完成计数和ActiveCallbackGuard，不能提前放行或破坏部分失败的drain。callback分配先保留，以独立profile决定是否值得安全复用，不直接传可被delete的栈/共享callback，不引入通用池或改hcom。分别交付原版/A/AB的diff、正确性和性能证据；将统计数组reserve移出wall计时并注明。无硬件则标pending。本阶段不实现双链接/SGL/scatter/IMM。

### 阶段 2 提示词

> 请先读取两份设计/实施文档、PROGRESS 和阶段 1/1.5 结果，在阶段 1 direct baseline 上只扩展双链接：两个 service 分别绑定两张 NIC，每个 channel 一条 QP，每条传 300 块、直接写最终 dst，总数600。B1/B2 使用完全相同的阶段1.5优化与配置，保持一个应用提交线程、单轮在途和 ROUND_READY/ACK 完成口径，按块交替提交两条 rail。新增独立诊断打点，记录提交、本地完成、每rail round-ready和ACK。交付同一二进制下B1/B2真实对照、独立trace、双NIC流量证据与报告，不把阶段1.5收益算成双链接收益。正式性能关闭详细trace，不跨机器相减时间戳。本阶段不实现SGL/staging/scatter/IMM。

### 阶段 3 提示词

> 请基于已完成的阶段1/1.5/2实施SGL：使用当前worker-poll PutV合并路径，测试8/16/30个分散源地址写连续staging，正确处理尾块，新增按chunk Send通知和流水scatter。与同链接数direct B1/B2比较完成相同最终任务的整体e2e，验证WR减少能否抵消scatter/通知成本；需要纯合并收益归因时再加plain-staged，不作为前置条件。核实实际QP能力，短诊断证明每chunk一个WR，不支持项明确SKIP。保存9个主case的正确性、direct回归、WR核验、性能及on/off打点证据，不将综合收益全部归为网卡gather。本阶段不实现WRITE_WITH_IMM。

### 阶段 4 提示词

> 请基于阶段 3，在独立可审阅变更中最小改造 ubs-comm，增加只面向 RDMA worker-poll、连续远端、单 WR 的 PutVWithImm 和接收事件。按文档传递 token、设置 WRITE_WITH_IMM、处理接收 CQ opcode/flags、持续补投接收 WQE并保持旧 Send 协议。benchmark 仅替换 chunk 通知，保留 scatter 和轮次 ACK。先通过事件、长度、尾块、接收补投、普通 Send 共存和错误路径验证，再用同一改造库成对跑 Send/IMM，回归 baseline 和旧 PutV。报告原始性能结果及归因；两端全部重编并记录 ABI/库版本，不夹带其它性能优化。
