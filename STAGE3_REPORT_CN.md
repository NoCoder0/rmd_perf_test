# 阶段3 SGL＋流水 scatter 实现报告

日期：2026-09-14。状态：`IMPLEMENTED / LOCAL_SELF_TEST_PASS / TARGET_BUILD_AND_HW_PENDING`。

## 1. Git 与范围

- worktree：`C:/code/RDMA_DEMO/perf_test_duo_card_sgl`，分支 `duo_card_sgl`。
- 设计提交：`58b21f3ed6d18a66287d3d318627cfd88cef252a`；基于 `duo_card@b3f4e4e43ff87ffe2a1544a648bc0b01904b3806`。
- 只修改本 worktree 的 C++、CMake、配置示例及根文档；`docs/main_reference/` 未改变，没有创建 Python 文件。
- 只读依赖核对为 `ubs-comm@e709a37e71bc2493d2a195a14eaaefe86c2dfb28`，公共 C++/C 上限均为16；未修改共享依赖或其它 worktree。
- 阶段3主体实现提交：`8c71dd0950c5a6e4c8db5f60c051f71bd22118e3`；首次报告元数据提交：`3013b7535bebdb8cb0e0fc9b944d2d82884f31da`；最终 deadline/cap 输出收口提交：`ebdc4a4d42c8c532582b6403b7a5677d1f5f2dd3`。本报告最终回填另成文档提交，交付时以 `git rev-parse HEAD` 为准。

## 2. 已实现内容

1. 参数与能力：新增 `--mode sgl --pipeline on|off`；SGL 默认 on。`RDMA_600_SGL_ITEMS` 严格解析 1..30，默认16；direct 有效 K 固定0且拒绝 `--pipeline`。当前头上限16，K30 立即 `UNSUPPORTED`。`RDMA_600_QP_MAX_SEND_SGE` 严格按 links 解析，每个值必须大于0；小于K拒绝，SGL measure 缺失拒绝，verify/trace 缺失时输出 `QP_CAP_PENDING`。来源明确为 `external-declaration`，没有伪称自动读取真实 QP。
2. v5 wire：新协议/magic/version；Parameters 40B、HELLO 252B、READY 64B、COPY_REQ 9664B、CHUNK_DONE 40B。direct 与 SGL 同用 v5；600 source＋600 destination offset 始终在请求内。所有整数显式大端编码，精确长度、reserved、mode/K/format/stage/rail/generation/chunk 元数据均校验。
3. stage 与 SGL：仅 local/SGL 在各 rail 固定 app 线程分配、触页、注册 `R*1024` stage，并在 HELLO 导出完整 key。remote 按请求索引分 rail，以 `source_base+src_offset` 为本地 SGE、`peer_stage_base+(first+j)*1024` 为连续远端地址；每 chunk 一个 PutV。
4. 同 QP 通知：每个 PutV 成功返回后立即在同一 channel 发送该 chunk 独立、稳定存储的 CHUNK_DONE；不等待 PutV callback，不批量发送。实际唯一 QP 与 WRITE→SEND 顺序仍需目标机 verbs 证据。
5. scatter 与原子状态：local 在 COPY_REQ 前 release 发布 expected generation；handler 完整校验后以 CAS/release 发布 per-rail/per-chunk ready；主线程 acquire 后 scatter。ready 不清零，consumed 独立记录 generation。on 轮转消费任意已就绪 chunk；off 等齐后调用相同 ScatterChunk。完成还必须等待 COPY_REQ callback，慢 rail/chunk 或 callback 均不能提前返回。
6. 生命周期与失败：remote pending/active 分离；每 rail iov、PutV request、每 chunk 通知和 active entries 在 data/Send callbacks 全回收前不复用。attempted 在 API 调用前登记，允许 callback 早于返回；失败不 delete/retry callback。不能 drain/quiesce 时保留 `_Exit` 安全边界。成功 JSON 移到 FINISH、drain、teardown、固定线程退出之后。
7. deadline/result/trace：每次 local SparseCopy 从入口 t0 派生一个绝对 deadline，覆盖地址生成/校验/编码、请求 post、ready、scatter和请求 callback；direct 同步修正。schema=5 记录 mode/K/pipeline、地址与请求成本、stage/chunk、预期 PutV/WRITE/通知、cap 来源/未知边界和构建身份。measure 不采 chunk trace；独立 trace 记录 ready/scatter begin/end 与 remote PutV/通知 post。

## 3. 本地 C++ 自测

Windows 编译器：`C:/msys64/ucrt64/bin/g++.exe`，版本 `15.2.0`。ignored `build/stage3_compat/` 只提供 Windows 缺少的 Linux/securec 声明和两个 wrapper；HCOM 类型、`UBSHcomOneSideSglRequest`、`PutV` 及 `NET_SGE_MAX_IOV` 来自当前真实公共头。兼容文件和 exe 不提交。

自测专用编译并运行：

```text
C:/msys64/ucrt64/bin/g++.exe -std=c++17 -Wall -Wextra -Werror=return-type -Wno-pedantic -Wno-unused-parameter -Wno-sign-compare -DRDMA_600_SELF_TEST_ONLY -include build/stage3_compat/compat.h -Ibuild/stage3_compat -IC:/code/RDMA_DEMO/ubs-comm/src/hcom -IC:/code/RDMA_DEMO/ubs-comm/src/hcom/service_v2/api rdma_600.cpp -o build/stage3_self_test.exe
./build/stage3_self_test.exe
```

结果：exit 0；`SELF_TEST: PASS (sparse-copy-v5, B1/B2 + S1/S2, K=1/8/16/30, on/off, 9664-byte sparse-600 request, reordered multi-generation scatter)`。编译仅有 self-test-only 排除生产类后产生的未使用函数 warning。

自测实际覆盖：

- B1/B2 direct 映射与 gap；S1/S2 K1/8/16/30 的 chunk 表和尾 chunk；on/off 参数/wire。
- 任意 sparse-600 映射、重复 source 合法、重复 destination/越界/溢出拒绝；stage gather 后以跨 rail/chunk 逆序 ready scatter，与 direct 整个稀疏 destination（含 gap）逐字节比较。
- 两代复用；重复/错代/越界/坏 reserved/截断/尾随通知和消息拒绝；慢 chunk gate；全部 scatter 后 COPY_REQ callback 延迟仍不完成。
- 坏 K/QP cap、未知 mode、direct 带 pipeline、握手 mode/pipeline 不一致、HELLO stage key/range、READY/COPY_REQ/CHUNK_DONE reserved 与长度异常。

全文件受限语法检查：

```text
C:/msys64/ucrt64/bin/g++.exe -std=c++17 -Wall -Wextra -Werror=return-type -Wno-pedantic -Wno-unused-parameter -Wno-sign-compare -fsyntax-only -include build/stage3_compat/compat.h -Ibuild/stage3_compat -IC:/code/RDMA_DEMO/ubs-comm/src/hcom -IC:/code/RDMA_DEMO/ubs-comm/src/hcom/service_v2/api rdma_600.cpp
```

结果：exit 0，无输出。它验证全文件与当前真实公共头的类型/API 兼容，不是 Linux 链接或 NIC 测试。

其它检查：

- `C:/msys64/usr/bin/bash.exe -n ./build.sh`：PASS。
- PowerShell `Get-Content -Raw hosts.example.json | ConvertFrom-Json`：PASS。
- `git diff --check`：PASS。
- `git diff --quiet HEAD -- docs/main_reference`（提交前）：PASS，main 参考未改。
- `rg --files -g '*.py'`：无输出，未新增/恢复 Python。

## 4. 与设计的合理实现细化

- 为避免 `64×2×600` 个逐 chunk 原子 trace 点占用主线程栈，TraceRound 数组只在 trace 模式按轮数分配到堆；measure 不分配/采样。
- callback “全部完成” trace 不再依赖最后提交的 callback 恰好最后返回，而是在累计完成数达到本轮目标时发布，可正确处理 callback 乱序。
- direct 也改用 SparseCopy 入口统一绝对 deadline，并对 direct 地址加 checked arithmetic；这是设计允许且已做 direct 自测的口径修正。
- 输出成功状态延后到 teardown 后，防止后续 drain/销毁失败仍残留 `status=ok`。
- 没有改变重大语义：没有 IMM、成功 ACK、跨代窗口、callback 池、内部 multirail 或共享依赖修改。

## 5. 明确保留的风险与目标机任务

状态保持 `TARGET_BUILD_AND_HW_PENDING`：本会话没有 Linux/AArch64 完整链接、双机 RDMA、真实 MR/DMA/CQ、性能或故障注入结果。

目标机必须补齐：公共头与实际静态库/二进制 hash；创建后每个真实 QP 的编号、NIC、`max_send_sge` 原始 query；每 chunk groupCount=1、num_sge=count、stage/rkey 地址和 WRITE→CHUNK_DONE Send 同 QP；双 NIC 流量、CPU/NUMA/MTU；B1/B2 direct 回归；K8/K16 on/off 的 verify/trace/5次 measure；单 rail 延迟、断链、部分 post、callback/scatter 延迟。

`RDMA_600_QP_MAX_SEND_SGE` 只是外部声明，不能替代原始 query。当前库 cap16 下 K30 只能验证布局/wire，不能运行。B2/S2 继续为 `diagnostic-unsupported-by-hcom-contract`；固定线程没有把 HCOM 多 Service 限制变成受支持契约。PutV 的既有 PrepareTimerContext/失败回调所有权风险未在共享库修复，本 demo 采用保守失败退出，不能宣称错误注入已通过。
