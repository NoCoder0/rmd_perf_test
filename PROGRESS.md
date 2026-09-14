# 实施进度

更新日期：2026-09-14。

## 当前状态

状态：`IMPLEMENTED / LOCAL_SELF_TEST_PASS / TARGET_BUILD_AND_HW_PENDING`。

| 项目 | 实现 | 验证 |
|---|---|---|
| v5 direct B1/B2 回归 | IMPLEMENTED | LOCAL_SELF_TEST_PASS / TARGET_PENDING |
| SGL S1/S2、K1/8/16 布局 | IMPLEMENTED | LOCAL_SELF_TEST_PASS / TARGET_PENDING |
| K30 布局/wire | IMPLEMENTED | LOCAL_SELF_TEST_PASS / UNSUPPORTED_BY_CURRENT_CAP16 |
| stage MR、PutV＋同 channel CHUNK_DONE | IMPLEMENTED | STATIC/SYNTAX_PASS / HW_PENDING |
| on/off 共用 scheduler/scatter/gate 与 observer-before-ready | IMPLEMENTED | LOCAL_SELF_TEST_PASS / DMA_PENDING |
| 统一 deadline 与 callback 生命周期 | IMPLEMENTED | STATIC/SYNTAX_PASS / FAULT_INJECTION_PENDING |
| schema 5 与逐 chunk trace | IMPLEMENTED | STATIC_PASS / TARGET_TRACE_PENDING |
| 双机 RDMA verify/measure | NOT_RUN | TARGET_BUILD_AND_HW_PENDING |

## 已执行

- 当前 worktree 从设计提交 `58b21f3ed6d18a66287d3d318627cfd88cef252a` 开始，未修改其它 worktree 或 ubs-comm。
- Windows/MSYS2 `g++ 15.2.0`，使用 `ubs-comm@e709a37` 当前公共头及 ignored `build/stage3_compat` 最小 Linux 兼容声明：全文件 `-fsyntax-only` PASS。
- 同环境以 `RDMA_600_SELF_TEST_ONLY` 编译并运行：PASS。输出覆盖 v5 B1/B2/S1/S2、256B HELLO 边界、K1/8/16/30、on/off 共用调度/完成gate、固定扫描、ready发布顺序、慢尾chunk和乱序多代 scatter。
- `git diff --check`：PASS。

完整命令、实现偏差和风险见 [STAGE3_REPORT_CN.md](STAGE3_REPORT_CN.md)。本地兼容声明及 exe 均位于 ignored `build/`，不提交。

## 保持 pending 的证据

未执行目标 Linux/AArch64 真实静态库链接、双机 MR/DMA/CQ、创建后 QP cap、WRITE→Send 同 QP、实际 groupCount/num_sge、双 NIC 流量、NUMA/MTU、故障注入或性能矩阵。当前结论不得升级为硬件或性能通过。
