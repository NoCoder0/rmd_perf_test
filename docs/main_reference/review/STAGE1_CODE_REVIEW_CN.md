# 阶段 1 Code Review（独立审阅）

审阅日期：2026-09-11（Asia/Shanghai）

## 1. 结论

阶段 1 的正常路径总体符合 B1 设计：单 service/device/channel、`linkCount=1`、worker poll、每轮 600 次异步 `Put(1024)`、每 30 块一次异步 `Send(DATA_READY)`、接收端流水 scatter、整轮 ACK、本地 callback 与 ACK 双重完成后复用、generation 跨 verify/warmup/measure 连续、最后 FINISH/FINISH_ACK drain。没有发现把双链接、SGL 或 IMM 混入阶段 1 的情况。

但当前版本**不应直接进入正式性能测量或标记 HW_PASS**：

1. 真实 `ubs-comm` 的异步 `Put/Send` 在 `SER_NEW_OBJECT_FAILED` 重试路径上会重复使用已经自删除/被删除的 callback，存在确定的条件性 use-after-free；阶段 1 的高频异步调用会经过这条路径。该缺陷尚未在硬件上触发，但从真实源码调用链可以确认。
2. `run.py` 没有满足任务书规定的版本证据链：未记录静态 `hcom_static`/库输入 hash、源码 dirty diff，也不拒绝两端不同二进制或依赖；因此一个参数和退出码均正常的 run 仍可能由不同构建产生。
3. 当前机器没有目标 Linux/AArch64 `dist/hcom` 产物或 RDMA 主机参数，真实头文件编译、链接、`--self-test`、双机 verify/measure 均未执行。

优先级统计：P0 0 项，P1 2 项，P2 3 项，P3 0 项。

## 2. 审阅快照与范围

### 2.1 perf_test

- 仓库：`C:\code\RDMA_DEMO\perf_test`
- 分支/提交：`main@3b23c84e3ab493e5d9ee03a216737482b07f4b5f`
- 审阅开始时状态：clean，且与 `origin/main` 一致。
- `C:\code\RDMA_DEMO\perf_test\rdma_600.cpp`：SHA-256 `269BA619EEEC5F23F29F1BE4B6FFA4C692312E2DB5169EF747005E49FF0AA207`
- `C:\code\RDMA_DEMO\perf_test\run.py`：SHA-256 `677D92FCF77E3A89136C462CAEF09A01B57045A94ABD3ED673CA80AEA5A76DD1`
- `C:\code\RDMA_DEMO\perf_test\CMakeLists.txt`：SHA-256 `5D1E526A848B41CBF7024C493A5607A080ADA8050B5A319A26CA0B1C6100F6A9`

### 2.2 真实依赖

- 仓库：`C:\code\RDMA_DEMO\ubs-comm`
- 分支/提交：`oneside-msge-merge@9e4c035a5d68ccca02d05fade3b6f5907db24ef4`
- 审阅时状态：clean。
- 审阅依据是该仓库真实 `service_v2` API、callback/timer、channel 和 service 实现；`.syntax_stub` 仅视为既有本地语法检查资产，没有用它证明真实接口兼容。

### 2.3 已读文件

- `C:\code\RDMA_DEMO\perf_test\DESIGN_CN.md`
- `C:\code\RDMA_DEMO\perf_test\IMPLEMENTATION_PLAN_CN.md`
- `C:\code\RDMA_DEMO\perf_test\PROGRESS.md`
- `C:\code\RDMA_DEMO\perf_test\README.md`
- `C:\code\RDMA_DEMO\perf_test\rdma_600.cpp`
- `C:\code\RDMA_DEMO\perf_test\run.py`
- `C:\code\RDMA_DEMO\perf_test\CMakeLists.txt`
- `C:\code\RDMA_DEMO\perf_test\hosts.example.json`
- 按调用链读取了 `C:\code\RDMA_DEMO\ubs-comm\src\hcom\service_v2\api\*`、`service_channel_imp.cpp`、`service_imp.cpp`、`service_callback.h`、`service.cpp` 及现有 perf CMake/用例。

## 3. Findings

## P0

无。

## P1

### P1-01：真实 hcom 在异步资源失败重试时复用已释放 callback

**分类：确认缺陷（源码调用链可确认）；触发尚未在 RDMA 硬件复现。**

**位置：**

- `C:\code\RDMA_DEMO\perf_test\rdma_600.cpp:1215-1222`：每个 Put 创建自删除 callback，然后调用 `channel->Put`。
- `C:\code\RDMA_DEMO\perf_test\rdma_600.cpp:1324-1334`：每个 Send 创建自删除 callback，然后调用 `channel->Send`。
- `C:\code\RDMA_DEMO\ubs-comm\src\hcom\service_v2\service_channel_imp.cpp:409-423`：timer/context 分配失败返回 `SER_NEW_OBJECT_FAILED`。
- `C:\code\RDMA_DEMO\ubs-comm\src\hcom\service_v2\service_channel_imp.cpp:1925-1954`：单 rail 异步 one-side 路径在 timer 准备失败时用 `ProcessRemainCallback` 执行 callback。
- `C:\code\RDMA_DEMO\ubs-comm\src\hcom\service_v2\service_channel_imp.cpp:1903-1922`：`ProcessRemainCallback` 调用 `cb->Run`；`UBSHcomNewCallback` 生成的 callback 随后自删除。
- `C:\code\RDMA_DEMO\ubs-comm\src\hcom\service_v2\service_channel_imp.cpp:1998-2022`：外层 `Put` 收到 `SER_NEW_OBJECT_FAILED` 后仍用原 `done` 指针重试。
- `C:\code\RDMA_DEMO\ubs-comm\src\hcom\service_v2\service_channel_imp.cpp:836-880`：异步 Send 的若干失败路径直接删除 `done` 或通过 `DestroyTimerContext` 删除它。
- `C:\code\RDMA_DEMO\ubs-comm\src\hcom\service_v2\service_channel_imp.cpp:457-481`：外层 `Send` 对 `SER_NEW_OBJECT_FAILED` 同样重试原 `done` 指针。
- `C:\code\RDMA_DEMO\ubs-comm\src\hcom\service_v2\api\hcom_service_channel.h:53-70`、`:100-104`：确认该 callback 的 `Run` 默认执行 `delete this`。

**触发条件：**

- hcom context/timer pool 分配失败、ctx store 生成 seq 失败，或底层 post 返回被外层归为 `SER_NEW_OBJECT_FAILED` 的错误；
- 阶段 1 正在用 `CHANNEL_FUNC_CB` 提交异步 Put/Send。每轮会创建并提交 620 个此类 callback，因此资源压力/失败注入必须覆盖该路径。

**后果：**

- 外层重试把悬空 callback 指针再次交给 timer/transport，可能出现 use-after-free、二次删除、崩溃或静默内存破坏；
- 这发生在 `Put/Send` 返回给 benchmark 之前，benchmark 的“返回码失败后只记录并退出”策略无法兜底；
- 因而目前不能声称 API 错误、资源耗尽或部分提交失败均能安全有界退出。

**修复建议：**

- 在 `ubs-comm` 中统一异步 API callback 所有权。外层若要重试，内层失败不得执行/删除调用者 callback；或者异步调用遇到 `SER_NEW_OBJECT_FAILED` 直接返回且仅消费 callback 一次，不在公共层复用原指针。
- 对 `Put`、`Send`、`Reply/Call` 做同类路径审计，避免仅修本用例命中的两处。
- benchmark 侧继续遵守“API 失败后不自行 delete callback”，不要用调用方补丁制造双重释放。

**验证建议：**

- 增加无需 RDMA 数据正确性的定向 UT：让 `PrepareTimerContext` 第一次返回 `SER_NEW_OBJECT_FAILED`，以带析构计数/canary 的 callback 验证恰好销毁一次且不再被调用。
- 在真实 worker-poll RDMA 环境限制 ctx/SQ 资源或注入 post 失败，使用 ASan/UBSan（若平台支持）验证无 UAF/double-free，进程非零有界退出且不输出结果 JSON。

### P1-02：跑测 manifest 没有绑定实际源码与静态依赖，两端不一致仍会被判成功

**分类：确认缺陷。**

**位置：**

- `C:\code\RDMA_DEMO\perf_test\IMPLEMENTATION_PLAN_CN.md:128-147`：明确要求记录二进制和库 hash、加载路径、源码 commit、dirty diff；静态链接时记录链接输入 hash。
- `C:\code\RDMA_DEMO\perf_test\CMakeLists.txt:53-67`：只把 perf_test 的短 HEAD 写入宏，没有 dirty 标志、`ubs-comm` commit、`libhcom_static.a` hash 或构建选项摘要。
- `C:\code\RDMA_DEMO\perf_test\run.py:353-383`：`remote_identity` 只记录 benchmark 二进制 SHA-256、目录和 `ldd` 文本。
- `C:\code\RDMA_DEMO\perf_test\run.py:495-508`：两端 identity 被写入 manifest，但没有比较或策略校验。
- `C:\code\RDMA_DEMO\perf_test\run.py:400-433`：结果校验不检查 `commit`，也不把它与 identity/build manifest 关联。

**触发条件：**

- sender/receiver 部署了不同的 `rdma_600`；或任一侧用 dirty 源码构建；或静态链接了不同 `libhcom_static.a`；或在 identity 检查与实际 exec 之间替换了文件。

**后果：**

- 参数、退出码和结果 JSON 全部正常时，脚本仍会把混合构建标成 `status=ok`；
- 当前项目链接 `hcom_static`，`ldd` 不会给出被静态打入二进制的 hcom 输入，更不等于“库 hash”；
- 后续 B1/B2/SGL/IMM 对比可能把版本差异误归因于链路或 API。

**修复建议：**

- 构建时生成机器可读 build manifest，至少含：perf_test 完整 commit、dirty patch hash、`ubs-comm` 完整 commit/dirty、`libhcom_static.a` 和 boundscheck 输入 SHA-256、编译器版本、CMake flags/build type。
- 将 manifest 摘要嵌入二进制并由两端输出；HELLO 交换协议中也应比较兼容的 build/protocol 标识。
- `run.py` 在启动前或结果验收时拒绝两端 manifest 不一致；若允许主机相关二进制 hash 不同，应比较规范化源码/依赖/编译配置身份，而不是简单放过。
- identity 采集和 exec 尽量放在同一远端 wrapper 中，减少 TOCTOU 窗口。

**验证建议：**

- 用两个仅 commit/静态库输入不同但参数相同的测试二进制，确认脚本在启动数据面前失败。
- 用 dirty 源码构建，确认 manifest 非 clean 且 patch hash 可追溯。
- 检查每个 repeat 的记录能从 hash 还原到保存的源码、构建命令和链接输入。

## P2

### P2-01：远端清理只凭可回收 PID，未证明目标仍是本 run 的进程

**分类：确认的清理协议缺口；误杀需要 PID 恰好复用。**

**位置：**

- `C:\code\RDMA_DEMO\perf_test\run.py:213-225`：远端仅把 `$$` 写入 PID 文件，`exec` 后没有正常退出清理。
- `C:\code\RDMA_DEMO\perf_test\run.py:318-350`：失败清理读取 PID 后直接 `kill -TERM`，仅校验“全数字”，不校验 `/proc/<pid>/exe`、启动时间、命令 token 或进程组；PID 文件也未删除。

**触发条件：**

- 远端 benchmark 已退出，但本地 SSH 尚未观察到退出；其 PID 在清理命令运行前被系统复用；或 PID 文件残留而后续人工/自动清理再次使用。

**后果：**

- 清理可能向不属于当前 run 的新进程发送 SIGTERM，违反“只清理当前 run”的硬约束；
- 正常重复运行持续在 `/tmp` 留下不可回收的 PID 文件。

**修复建议：**

- 使用保留父 wrapper 的方式启动子进程，记录 PID、`/proc/$pid/stat` starttime、解析后的 `/proc/$pid/exe` 和随机 token；清理时四者均匹配才发信号。
- 更稳妥时使用独立进程组/cgroup/systemd scope，并让 wrapper `trap` 删除 PID/identity 文件。
- TERM 后做有界存活检查，仅对仍匹配的同一进程发送 KILL；最后删除本 run 文件。

**验证建议：**

- 用本地/隔离 SSH fake 制造“子进程退出、PID identity 改变”，确认清理拒绝 kill。
- 制造无响应进程，确认 TERM→有界等待→KILL 后不存在远端进程和 PID 文件。

### P2-02：repeat 报告没有波动指标，也丢失正式 p95/p99 的跨 repeat 汇总

**分类：确认缺陷。**

**位置：**

- `C:\code\RDMA_DEMO\perf_test\run.py:436-453`：正式报告只输出各 repeat 的 `e2e_p50`、`submit_p50`、`effective_GBps` 三项中位数。
- `C:\code\RDMA_DEMO\perf_test\run.py:423-433`：单次结果已包含 `e2e_p95_us/e2e_p99_us`，但报告未使用。
- `C:\code\RDMA_DEMO\perf_test\DESIGN_CN.md:373-373`：要求 independent repeats 报告中位数与波动。

**触发条件：**

- `--kind measure --repeat 5` 或更多重复运行。

**后果：**

- 两组中位数相同但抖动完全不同的结果在 REPORT 中不可区分；
- 热降频、NUMA 漂移、单次异常和长尾退化可能被中位数隐藏，后续阶段无法可信解释收益。

**修复建议：**

- 报告每次原值，并为带宽、submit p50、e2e p50/p95/p99 至少给出 median + min/max 或 IQR/MAD；样本只有 5 次时不要用不稳定的正态假设包装精度。
- 报告 requested/successful/failed repeats；存在失败时保持脚本非零，并明确该组不能作为正式对比结果。

**验证建议：**

- 喂入固定的 5 组含一个离群值的 mock result，核对表格、波动统计和失败状态。

### P2-03：脚本可接受缺少 NIC/QP/CPU 拓扑证据的“正式成功”结果

**分类：未验证风险；在当前无硬件环境不能确认实际映射是否错误。**

**位置：**

- `C:\code\RDMA_DEMO\perf_test\run.py:92-129`：只检查 app CPU ID 与 worker CPU ID 数值不同，不检查是否为同一物理核的 SMT sibling，也不检查 CPU/NIC NUMA。
- `C:\code\RDMA_DEMO\perf_test\run.py:353-383`：identity 不采集主机、CPU 拓扑、NIC、active MTU、RDMA device/port 或实际 affinity。
- `C:\code\RDMA_DEMO\perf_test\run.py:400-433`：正式结果验收不要求 QP→device/port、绑核或端口计数证据。
- `C:\code\RDMA_DEMO\perf_test\rdma_600.cpp:837-840`：只把 worker CPU 范围传给 hcom；程序没有在 worker 内回读/输出实际 affinity。

**触发条件：**

- 配置的两个逻辑 CPU 是同一物理核 siblings；worker 实际未按预期绑定；`rdma-ip` 映射到非预期 device/port；不同 repeat 的频率/NUMA/MTU 环境改变。

**后果：**

- run 会产生 `status=ok` 和正式带宽，但无法证明它测的是约定的单真实 RDMA QP/NIC 与隔离 CPU；
- 结果可用于功能 smoke，但不足以做性能归因或进入阶段对比主表。

**修复建议：**

- 将静态环境证据纳入每个 run manifest：`lscpu -e`/NUMA、RDMA device/port/link/MTU、IP→device 映射、进程/线程实际 affinity、驱动/provider 与固件版本。
- 从 hcom/诊断日志或最小只读接口记录 channel endpoint/QP 的真实 device/port；不要用创建前配置值代替创建后事实。
- 端口计数可作为独立采样证据，不必塞入热路径；正式报告只接受证据齐全的 run。

**验证建议：**

- 故意选择 SMT siblings、错误 RDMA IP、错误 NUMA 核，确认 preflight 或结果验收明确拒绝/降级为非正式 smoke。
- 在单轮短测前后采集目标端口计数，核对只出现一条数据 path 且工作量与预期相符。

## 4. 已确认的正常路径性质

以下结论来自当前源码，不等于硬件验证：

- READY 时序：receiver 在 `SetupMemory` 完成 stage/dst 初始化和 stage MR 注册后才设置 `mReceiverReady`；`OnHello` 再检查该标志并回复地址/key（`rdma_600.cpp:791-799`、`:900-911`、`:1036-1067`）。
- 单 channel/QP 意图：`linkCount=1`、内部 multirail 关闭，Put 与通知使用同一个 `mChannel`（`:845-848`、`:958-974`、`:1204-1228`）；真实 hcom `NextWorkerPollEp` 在单 driver/单 link 下选择同一个 endpoint（`service_channel_imp.cpp:352-379`）。实际 QP/device 仍需目标机证据。
- callback/缓冲生命周期：source、20 个 notice、ACK、READY、FINISH 都是对象成员；每轮等待 ACK、600 data callback 与 20 Send callback 后才复用（`:1208-1239`）。
- 接收发布：callback 内同步解码借用的 receive 数据，校验后用 release store 发布；scatter 线程 acquire load（`:1071-1094`、`:1242-1276`），未把 context/message 指针带出 callback。
- verify/warmup/measure：generation 从 1 连续递增；verify 每轮填入代际模式并逐 word/间隙检查；正式数据预生成在计时外，最后检查 generation 0 的最终 dst（`:1177-1199`、`:1338-1361`）。
- 完成口径：`submit_us` 到最后通知 API 返回，`e2e_us` 到 ACK 与本地 Put/Send callback 全部完成；正式 wall time 覆盖整段循环（`:1208-1239`、`:1498-1508`）。
- 收尾：最后一轮后发送 FINISH，receiver 完成最终检查并回复 FINISH_ACK，两端等待本地 Send 完成后再 drain/Disconnect/MR destroy/service Destroy（`:1253-1321`、`:1436-1481`）。

## 5. 构建与运行就绪度

### 5.1 当前可以确认

- `CMakeLists.txt` 的 include 路径、`hcom_static`、boundscheck、pthread/dl/rt 依赖与 `ubs-comm` 自带 perf CMake 的结构一致。
- 真实头文件签名与当前调用形式在源码层面一致：`Create/Bind/Start/Connect`、MR 注册/key、`Call/Reply`、异步 `Put/Send`、handler 和 channel 配置均存在。
- Python 脚本语法、CLI 和示例配置的 verify 参数生成可在当前 Windows 环境执行。

### 5.2 阻塞项

- P1-01 必须先修复或由依赖维护者给出等价的安全修订，并做失败注入验证。
- P1-02 必须在正式 measure 前修复；否则测量结果没有满足任务书的构建身份证据。
- 必须在目标 Linux/AArch64 使用真实 `dist/hcom/include` 与 `libhcom_static.a` 完成 Release 构建、链接和依赖加载检查。
- 必须依次通过 `rdma_600 --self-test`、双机短 verify、故障/超时验证，再运行 warmup=1000、measure=10000、5 个 fresh-process repeats。
- 正式性能归因还需补齐 P2-03 所列 QP/NIC/NUMA/affinity/MTU 证据。

因此当前判定为：**IMPLEMENTED 代码可进入目标构建修正/验证阶段，但尚不具备直接正式跑测或宣布阶段 1 通过的条件。** `PROGRESS.md` 的 `IMPLEMENTED / NOT_RUN` 描述没有夸大硬件状态。

## 6. 本次执行的检查

- `git status`/`rev-parse`/`log`：固定 perf_test 与 ubs-comm 快照，均为 clean。
- SHA-256：记录主要实现文件审阅摘要。
- 逐行静态审阅 C++/Python/CMake/配置和设计文档。
- 沿真实 hcom 路径核对：API 签名、callback 自删除、timer/context 所有权、`Put/Send/Reply` 异步提交、CQ callback 分派、endpoint 选择、MR 注册/销毁、Disconnect/Destroy 顺序。
- `git diff --check`：PASS。
- `run.py` AST 解析：PASS。
- `python run.py --help`：PASS。
- `hosts.example.json` 校验和 verify argv（warmup/rounds 固定为 0）：PASS。

## 7. 明确未执行的验证

- 未用真实 `dist/hcom` 头文件和库进行 Linux/AArch64 编译或链接；当前工作机没有相应产物。
- 未运行构建后的 `rdma_600 --self-test`。
- 未执行双机建链、MR、DMA、CQ/RQ、QP 顺序或 ARM DMA 可见性验证。
- 未执行故障注入、ASan、资源耗尽或部分提交复现。
- 未执行 SSH 远端启动/清理测试。
- 未执行 verify/warmup/measure、未产生任何性能数据、trace 或端口计数。

本报告没有把 stub 编译、静态审阅或 Python 检查当作真实 RDMA 验证证据。
