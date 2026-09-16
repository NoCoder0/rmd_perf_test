# 阶段3独立代码检视

日期：2026-09-14。检视基线 `97b20e1615f9cd4b9212d8e37462aef0345e4eb5`。本文件由设计/主会话检视形成，问题回交原 `gpt-5.6-sol / high` 实现会话 `01a09fd1-b3d6-7771-85c5-464828e7c7cf` 修复，修复后再独立复核。

## R1 — P1：HELLO实际256B，声明252B造成越界和截断

状态：CLOSED — 主会话独立复核通过，修复提交 `97131b883046d73fd9f6a40007f1db85d0781fa0`。

位置：`rdma_600.cpp:139`、`:451–499`（本次基线行号）。`kHelloWireBytes` 按stage descriptor `20+80` 计算，但EncodeHello/DecodeHello都额外编码/读取4B stage reserved：stage部分实际为 `4+4+8+8+80=104` 字节。总长度为256，声明/返回array/网络发送长度为252。

影响：每次HELLO编码写出array末尾4B；接收方仅要求252B却读取256B，丢失的stage key尾4B来自消息边界以外。direct和SGL握手都受影响，可能破坏邻接存储、误拒绝direct的全0stage或读取错误key。相同错误的encode/decode互相round-trip，不能证明边界正确。

独立可执行复现：ignored `build/review_hello_extent.cpp` 引入真实demo源文件和当前公共头，在256B backing buffer内构造真实字段，仅向DecodeHello声明252B，并在末尾4B放0xde。输出：

```text
declared=252 encoded_fields=256 accepted=1 consumed_outside_wire=1
```

退出0表示缺陷成功复现，不是功能通过。此复现避免依赖未定义内存碰巧可读，也证明了wire声明边界以外字节影响解码结果。

修复要求：统一为真实256B，或删除额外reserved并严格保持252B；同步确切wire格式、长度/断言、两端和全部当前文档。编解码必须校验最终cursor与buffer end，增加独立长度/末尾key字节/短消息测试，避免测试只复述同一错误常量。

## R2 — P2：ready先发布，trace后记录，流水诊断可能倒序

状态：CLOSED — 主会话独立复核通过，修复提交 `97131b883046d73fd9f6a40007f1db85d0781fa0`。

位置：`rdma_600.cpp:2301–2310`。OnChunkDone先CAS/release发布chunkReadyGeneration，然后才读取时钟并记录local_chunk_ready。local app acquire该ready后可以立即开始甚至结束scatter；callback随后记录的ready timestamp会晚于scatter begin/end，产生负的ready→scatter调度时延，误导on/off流水分析。

修复要求：在允许app观察ready前完成本次有效通知的trace timestamp发布；重复通知仍必须拒绝，不得覆盖既有trace。用可控的生产共用发布辅助函数测试发布顺序，不依赖线程调度运气；measure继续不打chunk时间戳。

## 验证覆盖复核

现有SelfTestSglMapping真实复制数据，但其scatter循环以及requestCallbackDone布尔判断独立于生产WaitAndScatterSgl/ScatterChunk，不能作为生产调度/返回gate已经执行通过的证据。修复时应将可测试的chunk scatter/调度或completion gate提成生产与self-test共用的最小函数，并补slow-rail、延迟request callback和乱序多代检查；不能再仅以手写布尔恒等式声称生产gate已验证。

主会话复跑全文件真实公共头受限语法和现有self-test均PASS，随后仍复现R1，说明这两项检查的原覆盖边界。main参考目录与设计提交完全一致；没有新增Python；main/duo_card/ubs-comm不在修改范围。

## 保留的部署边界

本次不把源码审阅和本地模拟等同Linux/AArch64链接或双机RDMA。实际QP cap、WRITE与通知同真实QP、DMA可见性、双NIC流量、部分post错误/库callback重试、TLS缓存与多service契约及性能仍需目标机证据。当前共享库16项上限下K30为UNSUPPORTED，外部cap声明不等于自动查询结果。

2026-09-16代码清理说明：本文件中的 self-test 叙述均是独立检视当时的真实历史证据。后续已删除 `RDMA_600_SELF_TEST_ONLY`、`--self-test`、`RunSelfTest/SelfTest*` 及专用源码；生产使用的 HELLO 校验、ready publication helper、scatter/scheduler、all-ready/completion gate 保留不变。

## 回修与最终复核

原实现会话已按本文件要求完成回修：HELLO 保留 reserved 并统一为256B，Encode/Decode 校验最终 cursor；新增独立字段计数、末尾非零 key、252/255/尾随边界测试。CHUNK_DONE 改为原子哨兵独占槽位，在可消费 generation 前运行 trace observer，重复通知不进入 observer；measure 短路不采时间。生产与 self-test 共用 ready 发布、固定起点完整扫描 scheduler、scatter payload、all-ready/completion gate，并覆盖慢 rail/末尾 chunk、乱序两代及 callback 延迟。流水有无进展均累计检查单元，每256个调用 fatal/deadline checkpoint。

主会话在回修会话结束后独立审阅全部修复diff，确认生产OnChunkDone共用observer→release helper，WaitAndScatterSgl共用scheduler/scatter/completion gate；两项问题及测试覆盖缺口均已关闭。本地执行结果：

- 使用当前真实公共头的全文件 `-fsyntax-only`：PASS。
- 按实现报告的self-test编译命令增加 `-O2 -Wno-unused-function`，重新编译为 `build/stage3_review_verified.exe` 并运行：PASS，覆盖B1/B2/S1/S2、K1/8/16/30、on/off、尾chunk、多代、发布顺序及完成gate。
- 独立重编 `build/review_hello_extent.cpp` 并运行：`declared=256 encoded_fields=256 legacy_252_accepted=0 accepted=1 tail_preserved=1`，exit0。
- Bash脚本语法、JSON解析、diff whitespace：PASS；main参考与设计提交完全一致，无Python脚本。

修复已由主会话提交为 `97131b883046d73fd9f6a40007f1db85d0781fa0`；本次关闭记录另成文档提交。子会话不能写Git元数据的临时限制已由主会话完成提交解决，没有请求用户干预或丢弃修改。

最终结论：**LOCAL_REVIEW_FIXES_VERIFIED / TARGET_BUILD_AND_HW_PENDING**。在已检视范围内未发现尚未关闭的新增P1/P2问题；前述共享库、多service和真实硬件验收边界仍然保留，不据此宣布部署性能验收完成。
