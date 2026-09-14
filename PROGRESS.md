# 实施进度

## 当前状态：阶段 1 已重构，等待目标机验证

2026-09-12，工作区基于 `main@38e6637` 完成 `sparse-copy-v3` 单卡 SC-B1 重构。当前改动尚未提交：本会话尝试提交文档时无法创建 `.git/index.lock`。没有切换或修改 `duo_card`，也没有修改 `ubs-comm`。

| 新目标 | 实现状态 | 验证状态 |
|---|---|---|
| 阶段 1：requester-driven SC-B1 | IMPLEMENTED_IN_WORKTREE | LOCAL_PROTOCOL_PASS / HW_PENDING |
| 阶段 1.5：A+B 迁移复核 | IMPLEMENTED_IN_V3 | STATIC_PASS / HW_PENDING |
| 同步公共基线至其它分支 | NOT_STARTED | NOT_RUN |
| 阶段 2：SC-B2 与 trace | NOT_STARTED_ON_MAIN | NOT_RUN_ON_V3 |
| 阶段 3：SGL 流水 | NOT_STARTED | NOT_RUN |
| 阶段 4：WRITE_WITH_IMM | NOT_STARTED | NOT_RUN |

## 阶段 1 已实现范围

- CLI 角色改为 local/remote：remote 监听，local 连接并调用同步 sparse_copy。
- setup 中 local 导出自己的 destination 基址、大小、区域 ID 和完整 MemoryKey；remote 只返回 source 区域 ID、大小和对齐能力，不导出 source key。
- 每次 local 从调用入口开始生成、校验并编码完整 600 个源/目标偏移对；COPY_REQ 固定为 64 字节头加 9600 字节正文，共 9664 字节。
- remote 接收 callback 将消息复制到预分配 pending，应用线程交接到独立 active 存储；每轮重新解析请求并填充 600 个 Put 描述符。
- remote 每轮异步提交 600 个 direct Put，随后在同一单 QP 上发送 DATA_DONE；local 等数据 ready 和自己的请求 Send 完成后返回。
- 删除逐轮成功 ACK。下一 COPY_REQ 作为 local 已消费上一代的复用信用；remote 在复用 WR/通知存储前等待自己的 callbacks。
- 增加仅失败路径的 COPY_ERROR；channel 不可用时由断链回调唤醒。失败 callback 仍发布生命周期完成计数，避免失败 drain 永久等待。
- 保留阶段 1.5 A+B：app/worker 分核、busy-poll relax、每 256 次检查 deadline、按线程所有权的 attempted 计数、共享完成发布、缓存行隔离和 ActiveCallbackGuard。callback 仍按请求分配。
- local 输出 schema 3 的 sparse_copy 主指标；remote 输出单独 status。`run.py` 和 `hosts.example.json` 已迁移到 local/remote、SC-B1、`--case` 和新结果校验。
- direct 保持单 service/device/channel/QP、`linkCount=1`、关闭内建 multirail、600×1024 字节、stride 4096；没有 staging/scatter/SGL/IMM。

## 已执行检查

| 检查 | 结果 | 边界 |
|---|---|---|
| `git diff --check` | PASS | 仅格式检查。 |
| C++ 公共头语法检查 | PASS（受限） | 使用 `ubs-comm oneside-msge-merge@9e4c035a5d68ccca02d05fade3b6f5907db24ef4` 的真实 service 公共头；只为 Windows 缺少的 Linux 声明临时提供兼容头，未保留到仓库。不能替代 Linux 编译/链接。 |
| 协议 self-test | PASS（本机测试专用构建） | 覆盖 HELLO/READY、COPY_REQ 9664 字节、DATA_DONE/COPY_ERROR、截断、重复目标、600 项非顺序映射和数据/gap 校验；不覆盖 RDMA。 |
| `run.py` 语法、`--help`、示例配置和 argv 契约 | PASS | 未启动 RDMA 进程。 |
| 目标 Linux/AArch64 构建和链接 | NOT_RUN | 当前机器没有目标 `dist/hcom` 静态库和 Linux 环境。 |
| 双机 RDMA verify/measure | NOT_RUN | 尚无新协议性能结果，不得引用旧协议数字。 |

## 目标机下一步

1. 两端用同一工作区版本和同一 ubs-comm 提交构建，运行 `./build/rdma_600 --self-test`。
2. remote 先启动 `--role remote`，local 再启动 `--role local`；各跑 20 轮 verify，确认完整非顺序地址映射、DATA_DONE 顺序、无成功 ACK 和有界错误退出。
3. 运行 `1000 warmup + 10000 measure`，每个物理 repeat 使用新进程和新目录；至少 5 次并保存两端 manifest、hash、日志和 NIC/NUMA/CPU 证据。
4. 只有硬件正确性与证据完整后，才把阶段 1 标为 HW_PASS。阶段 1.5 收益必须在同为 sparse-copy-v3 的 original/A/AB 变体之间比较。

## 后续阶段约束

- 阶段 2 只增加双 NIC direct；先同步公共 v3 基线并回归 SC-B1，不跨分支改写历史。
- 阶段 3 才实现 SGL/staging/scatter。SGL=30 只有在调用方真实提供可展开源组时才可使用 20 source + 600 destination（5024 字节请求）；任意 600 个分散源仍完整发送。
- 阶段 4 才评估 WRITE_WITH_IMM，需明确授权修改 ubs-comm。
- 历史阶段 1.5 旧 sender/receiver + ROUND_ACK 实现及 submit 长尾见 [STAGE1_5_REPORT_CN.md](STAGE1_5_REPORT_CN.md)，不作为 v3 性能结果。
