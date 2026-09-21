# Remote 尾部：手动 trace 后压缩分析

目标是查看“末次数据 POST_END → 最后数据 CQE”期间的逐步完成情况。
复用已有 HCOM 事件，不增加运行时打点、逐次空 poll 日志或自动运行脚本。
只支持 SGL remote 的单个 workload 完整 trace；本工具和同目录 compact_trace.py 一起使用。

## 手动操作

1. 按现有命令手动执行 `rdma_600 --kind trace ...`，把 remote 的完整 stdout/stderr 保存在设备本地，例如 `sgl-remote-trace.log`。其他测试参数保持该组对照所需配置。
2. 程序结束后手动压缩。原有阶段压缩命令继续使用；新增尾部报告是一个独立命令：

```bash
python3 compact_trace.py sgl-remote-trace.log > sgl-remote-compact.json
python3 analyze_remote_tail.py sgl-remote-trace.log > sgl-remote-tail.json
```

默认每轮保留最多64个完成时间桶（通常25 μs一桶）、8个最长CQE间隔、8个尾部慢WR，以及按QP汇总。
1600×656 B/K30只有54个数据WR，可以选择把全部WR压成数字表，列名只输出一次：

```bash
python3 analyze_remote_tail.py sgl-remote-trace.log --wr-details > sgl-remote-tail-details.json
```

默认报告与详细报告选一个上传即可，并附原有阶段compact。不需要上传原始大日志，也不需要measure.log。
不要把旧compact JSON当作输入：它已丢弃逐WR/逐poll窗口信息，无法逆推出新的明细。
若设备仍留有原始日志，可直接重压缩，无需重跑或重编译；当前实现只处理SGL，不能直接输入MF日志。

## 读法

- `pending_wr_at_start`：尾部起点仍未观察到CQE的WR数。不能把整个尾部理解为最后一个WR独自等待。
- `bins`：相对于尾部起点，每桶实际观察到的数据CQE数、完成字节数和桶结束时剩余WR数。用于区分持续慢推进与局部停顿。
- `largest_gaps`：按同一poll线程/CQ，两个不同数据CQE观察时刻之间的最长间隔。第一段从尾部起点截取；多CQ区间可能重叠，不应相加。
- `empty_polls_full_windows`：只累加完整落在该间隔内的原始poll统计窗口。`straddling_or_unknown_windows>0`表示边界窗口无法精确切分；不能把未计入的次数当成0。
- `max_poll_gap_reported_us`：原始窗口的最长“上次poll返回→下次poll进入”，含回调及trace开销，跨边界时也可能来自区间外。它不是线程被调度出去的直接证据。
- `dispatch_covered_us`：该线程已记录dispatch区间与目标间隔的交集并集；嵌套回调不重复相加。这是墙钟覆盖时间，不是CPU运行时间。
- `wrs`：按`wr_columns`解释。`post_begin_us/post_end_us/cqe_us`相对本轮首次数据提交，`residual_after_tail_start_us`相对尾部起点。关联使用QP和WR每次复用实例；回调缺失时为null，不虚构0。
- `tail_software_completion_GBps`：尾部已提交WR字节数/软件观察时间，不是网卡线速，也不代表这些字节全部在尾部期间传输。

CQE平稳返回、空poll持续发生、最长poll间隔较短时，优先检查数据完成路径；CQE长空档伴随长poll间隔时，再查worker在两次poll之间的工作或调度。
CQE已被读到而回调延迟较大，是另一段问题：末个CQE之后的回调不会计入本工具的尾部，但前面WR的回调可能延后下一次poll。

现有空poll/max统计是**每个poll线程自上次非空poll以来**的汇总，可能包括其他CQ，不能直接冒充目标QP的精确硬件等待时间。
缺记录、失败CQE、丢事件等检查复用compact_trace.py；不完整时返回非零退出码并抑制尾部明细。

## 需要进一步打点的条件

若上述报告发现明显poll空档，再针对长空档补少量事件：间隔起止、线程/CPU、当时处理的任务类型；先不逐次打印空poll。
若线程持续poll但没有完成，仅靠软件打点不能把耗时细分到源DMA、网络、对端写入或确认。
下一层需要实际网卡/驱动支持的完成时间戳或硬件计数器，而不是增加普通日志行数。

参考：[ibv_poll_cq](https://github.com/linux-rdma/rdma-core/blob/master/libibverbs/man/ibv_poll_cq.3)、
[扩展CQ与完成时间戳](https://github.com/linux-rdma/rdma-core/blob/master/libibverbs/man/ibv_create_cq_ex.3)。
