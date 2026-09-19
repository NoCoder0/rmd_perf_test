# SGL分组完成通知

日期：2026-09-19。分支：`duo_card_sgl`；修改前HEAD：`c7b428e`。本次只修改demo，未修改ubs-comm依赖。优化对象是CHUNK_DONE通知频率。

## 参数与运行

新增 `--notify-every-wrs G`，SGL默认1，严格十进制范围1～9600。remote和local都设置相同G；该参数在一次运行的所有case中固定。direct不接受显式设置，结果中记为0。

保持现有启动命令、CPU/NIC、轮数和矩阵，分别在两端追加：

```text
--notify-every-wrs 1
--notify-every-wrs 4
--notify-every-wrs 8
```

以上是三次独立运行，每次只选一行；K仍由 `RDMA_600_SGL_ITEMS=30` 等原环境变量控制。两端重新编译为v8后，用同一版二进制的G=1建立基线。

每条rail独立累计G个数据PutV后发送一次通知，轮末不足G的尾组也发送。G不改变每个PutV的SGE数、数据布局、PutV调用次数、数据callback频率或post链表批量。在当前预期路径中一个PutV对应一个数据WR；是否实际如此仍应由真实QP/verbs证据确认。

| N / links / K | G | 每轮数据PutV | 每轮CHUNK_DONE |
|---|---:|---:|---:|
| 9600 / 1或2 / 30 | 1 | 320 | 320 |
| 9600 / 1或2 / 30 | 4 | 320 | 80 |
| 9600 / 1或2 / 30 | 8 | 320 | 40 |
| 600 / 2 / 30 | 4 | 20 | 6 |
| 601 / 2 / 30 | 4 | 21 | 6 |

通知数为各rail的 `ceil(ceil(rail_blocks/K)/G)` 之和。不能先合计两条rail的WR数再统一取整。G大于该rail的chunk数时整轮一次通知。

## 协议与完成条件

- 协议/schema升至8：`sparse-copy-v8-group-notify`。HELLO/READY和矩阵参数包含G，拒绝G不匹配或旧版peer。HELLO从256B变为260B，READY从64B变为68B，矩阵项从56B变为60B。消息族magic沿用，显式版本字段区分兼容性。
- COPY_REQ仍为 `64+16N` 字节，最后一个原保留字段携带G；逐轮解码也校验G。当前N≤9600仍一次请求Send。
- CHUNK_DONE仍40B。`chunkId`表示本组首chunk，`firstItem/itemCount/payloadBytes`覆盖本组全部有效数据，`chunkCount`仍是该rail总chunk数。接收端严格校验组首对齐、整组长度及尾组；这不是累计水位，晚到/乱序组不会使此前未收到通知的组提前ready。
- 每个组只用其首chunk对应的一个原子槽发布ready，scatter查找chunk所属组。trace启用时，组内所有ready时间戳先写入，再release发布整组。重复/并发通知仍由CAS拒绝；错rail、错代及错误范围仍触发失败。
- remote在同channel提交组内全部PutV后立即Send通知，不为每组等待数据callback。每个在途通知使用独立payload存储，整轮等待全部数据callback及分组后的Send callback后才复用。
- local仍按原chunk调度scatter。pipeline=on可处理已到达组，pipeline=off等待所有组。完成条件仍是全部rail的全部chunk已scatter且COPY_REQ的Send callback完成；计时起止和请求准备成本不变。

本次继续依赖既有的同QP WRITE→SEND顺序与DMA可见性条件；软件通道测试不能验证NIC顺序。

## 输出与trace

JSON和汇总表新增 `notify_every_wrs`，JSON新增 `notifications_per_rail`。`data_wr_per_round_expected`仍按PutV数量计算，`completion_send_wr_per_round_expected`改按分组后的实际Send调用数计算。

trace schema为 `sparse-copy-v8-group-notify-v1`，每个事件带G。remote的 `remote_chunk_group_done_posted` 每组只输出一次，`chunk_id`是组首；`remote_chunk_posted`、local ready/scatter仍逐chunk输出。分析脚本需更新schema、表头和通知事件名。

## 验证

生产入口及10个cpp使用当前真实ubs-comm公共头，在Windows/MinGW GCC15.2与既有兼容声明下通过 `-Wall -Wextra -Werror -fsyntax-only`。第三方头作为system include处理，demo警告保留为错误。

软件回归通过504个case、1,008轮。外部测试链接生产的10个cpp，使用通道替身执行 `ProcessRemoteRail/ProcessRemoteSglRail` 的真实提交循环，以内存复制代替DMA。回调特意延后至本rail最后一个通知后逆序完成，检查所有在途通知payload保持不变、发送计数及drain条件；工厂替身禁止创建真实HCOM服务。

- CLI默认G、边界值、非法数字、重复选项和direct误用拒绝。
- 参数wire精确长度及canary，HELLO/READY/矩阵/COPY_REQ的G一致性与旧版本拒绝；CHUNK_DONE的错rail/错代/错范围/错误长度/不对齐组拒绝。
- N=100/101/600/601/9600，B=1024/656，links=1/2布局，K=1/16/30，G=1/4/8/9600，pipeline=on/off；覆盖尾chunk、尾组、整轮一次通知以及两端原有direct路径。
- 逆序交付通知组，验证只发布对应组、没有把漏收的前序组当成ready，scatter和完整数据/stride gap校验通过；重复组拒绝，跨轮和跨case不会误用旧generation。
- 完整生产scatter调度器等待延后的COPY_REQ Send callback；trace中组内ready先于scatter，remote仅输出实际发出的组通知事件。
- 504条结果JSON解析及公式检查通过；N9600/K30时G=1/4/8对应320/80/40次通知，data PutV保持320次。

外部验证程序及日志保存在ignored的 `build/notify_*`，没有向生产程序添加self-test入口。主要记录为 `build/notify_group_validation.log` 和 `build/notify_json_validation.log`。软件通道按顺序调用各rail，未验证真实HCOM线程并发、QP或NIC行为。

尚未执行目标Linux静态库链接、双机RDMA verify及性能测试。推荐先验证N=100/101/600/601/9600、B=1024/656、links=1/2、pipeline=on/off和G=1/4/8，特别检查双rail不等长尾组。性能比较保持K=30与其余配置相同，记录端到端avg/p95/p99、首组ready到首scatter的延迟以及CPU占用；操作数下降比例不能直接当作加速比。
