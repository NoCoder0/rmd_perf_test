# 阶段3实施与目标机验收计划

更新日期：2026-09-14。

## 已完成实现

1. direct 与 SGL 统一升级 v5，保留每轮 600/600 真实地址和 9664B 请求。
2. 增加 `--mode sgl --pipeline on|off`、严格 K 环境变量和每 rail 外部 QP cap 声明；当前依赖明确拒绝 K30。
3. local/SGL 每 rail 注册连续 stage；remote 构造连续远端 iov，按 chunk 执行 PutV 后立即同 channel Send CHUNK_DONE。
4. 增加 generation ready 的原子占位→trace observer→release发布、乱序 chunk 接收、生产/自测共用 scatter scheduler 与 completion gate、on/off gate、请求 callback gate和单一绝对 deadline。
5. 保留固定 rail 线程、pending/active、保守 callback drain；成功输出移到 teardown 之后。
6. schema/trace/self-test/启动文档更新；未修改 ubs-comm，未恢复 Python。

## 本地完成条件

- 使用当前 ubs-comm 公共头的全文件 `-fsyntax-only` 通过。
- `RDMA_600_SELF_TEST_ONLY` 编译运行通过，覆盖 direct/SGL、单/双 rail、K1/8/16/30、尾 chunk、on/off、固定起点完整扫描、乱序/多代/慢尾chunk/错误通知/callback 延迟、observer-before-ready、256B HELLO 边界和 checked arithmetic。
- `git diff --check`、文档 JSON 解析和 main_reference 完整性复核通过。

这些条件不等价于目标 Linux 链接或 RDMA 验收。

## 目标机验收顺序

1. 两端记录本提交、源码/二进制 hash、ubs-comm 提交、公共头和实际静态库 hash、编译器与构建选项。
2. Linux/AArch64 Release 构建并运行 `--self-test`；证明两端使用相同 v5 workload 参数。
3. 对每个真实创建 QP 做 query，记录 QP号/NIC/`max_send_sge`，再设置 `RDMA_600_QP_MAX_SEND_SGE`。校验声明与原始证据，不能只看设备 cap 或创建前日志。
4. B1/B2 direct 回归各 20 verify；S1/S2 的 K8/K16 on/off 各 20 verify。K30 在当前依赖跳过并确认 UNSUPPORTED；K1只做边界/背压正确性。
5. 独立 verbs/NIC trace 验证每 chunk `groupCount=1`、`num_sge=count`、stage 地址/rkey、WRITE 与紧随 CHUNK_DONE Send 使用同一真实 QP；双 rail 流量各约一半 payload。
6. 独立应用 trace 核对 ready/scatter begin/end，不跨主机相减时间戳。
7. 注入单 rail 延迟、断链、部分 post 失败、非法通知、请求 callback 和 scatter 延迟；任何失败不得留下 status=ok。
8. 正确性完成后，对支持的 K8/K16 on/off 各至少 5 次 `1000 warmup + 10000 measure`，记录 CPU/NUMA/MTU 和原始结果；B1/B2 同场回归。

## 未扩展范围

不实现 IMM、成功 ACK、跨代窗口、callback 池、内部 multirail 或多 service 契约修复。B2/S2 继续标 `diagnostic-unsupported-by-hcom-contract`。共享依赖 callback 失败重试风险如需修复，应另做 ubs-comm 补丁与全量重编。
