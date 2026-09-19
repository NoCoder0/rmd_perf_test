# 每条链接一个scatter线程

日期：2026-09-19。分支：`duo_card_sgl`；修改前基线：`e831745`。本次修改local的scatter执行方式，保持K、通知间隔G、远端PutV提交方式和v8消息格式。

## 执行方式

| local配置 | scatter执行线程 | CPU配置 |
|---|---|---|
| SGL / links=1 | 主应用线程处理rail 0 | `--app-cpu A` 或 `--app-cpus A` |
| SGL / links=2 | 主应用线程处理rail 0，已有的第二应用线程处理rail 1 | `--app-cpus A,B`，顺序对应rail 0/1 |
| direct | 不执行scatter | 沿用原应用线程配置 |

没有新增每轮线程创建或通用线程池。第二应用线程在整个run期间保持同一线程身份，新增 `ScatterLocalRound` 命令；各线程只读取本rail的ready、修改本rail的consumed generation并复制到本rail的destination。共享请求描述符在该轮scatter期间只读。HCOM worker继续处理完成通知与callback，应用线程继续负责内存复制。

`ScatterSglRail`复用原chunk扫描调度器，每次只扫描所属rail的真实chunk数。stage、destination和源地址映射沿用原实现，奇数N导致的rail长度不等分别计算。未同时引入就绪队列、改变K或调整通知间隔。

## 流水与完成条件

- **pipeline=on**：COPY_REQ提交后，主线程向第二线程发布该generation的scatter命令，随后处理rail 0；两条rail各自处理已收到通知的chunk。一条rail暂未ready不会阻止另一条rail复制。
- **pipeline=off**：主线程先等待全部rail的所有组ready，再发布rail 1任务并执行rail 0。保留原来的全局等待条件，避免把off改成“各rail收到自己的全部数据即可提前复制”。
- 每个rail只有在自身所有chunk scatter完成后才返回。rail 1通过原有命令序列的release/acquire完成交接，使目标数据、consumed generation和trace对主线程可见。
- 主线程先完成rail 0，再确认rail 1任务完成，最后确认COPY_REQ Send callback完成。sample结束、数据校验及下一轮请求描述符复用均在这些条件满足之后。
- 两个线程使用同一轮的绝对deadline。worker异常通过既有fatal路径传给主线程；Run失败处理先发布fatal并等待第二线程静止，再进行drain/teardown。case切换新增检查，拒绝第二线程还有未完成任务时重设case存储。

协议和schema仍为v8，无新增wire字段。local结果增加 `scatter_threads`、`scatter_policy`、`scatter_off_barrier`，并在汇总说明中注明每rail一个线程。trace事件名不变，各rail的scatter事件现在可并发出现，不能把两个线程的逐chunk耗时之和视为整轮墙钟时延。

## 验证

入口和10个生产cpp使用真实ubs-comm公共头、MinGW GCC15.2及既有Windows兼容声明，通过 `-Wall -Wextra -Werror -fsyntax-only`。外部测试直接链接生产代码，使用内存复制通道替身；本次并发测试运行真实C++主协调线程和持久rail 1线程。

已通过16组异步就绪测试：G=1/4/16/32、pipeline=on/off，分别延迟rail 0或rail 1。on验证已ready的rail可以独立完成且不会误结束整轮；off验证任何rail缺少通知时两个线程均不提前scatter；两条rail完成后仍等待故意延后的请求callback。覆盖逆序通知、N601不等长尾chunk/尾组，以及任务未完成时拒绝case切换。

已通过缺失通知触发共享deadline、worker侧scatter越界、主线程scatter越界、活动期间错误generation四类故障测试；检查错误传递、任务完成序列和worker安全静止。

矩阵回归通过744个case、1,488轮，覆盖N100/101/600/601/9600、B1024/656、links1/2、K1/16/30、G1/4/8/16/32/9600、pipeline on/off，以及direct回归。所有scatter完成后检查完整数据、stride gap、consumed generation、任务完成序列和持久线程身份；包含跨轮、跨case复用及trace检查。

744条结果JSON解析通过：SGL的 `scatter_threads` 等于links、策略为 `per-rail-app-thread`，direct的线程数为0；v8协议、COPY_REQ次数及各rail/整轮通知数公式保持正确。`git diff --check`通过。

测试及日志位于ignored的 `build/parallel_scatter_*`，主要记录为 `build/parallel_scatter_validation.log` 和 `build/parallel_scatter_json_validation.log`；生产程序没有新增self-test入口。

本地检查不包含Linux静态库链接、真实RDMA/DMA可见性或硬件性能；Windows兼容层也不验证Linux绑核/NUMA策略。

## 目标机比较

重新构建后沿用原SGL命令。先固定 `--pipeline on --notify-every-wrs 1`、K和原CPU/NIC配置，与 `e831745` 的单scatter线程基线比较；再单独改变G。建议同时覆盖N100/101/600/601/9600、B1024/656及links1/2，先verify再measure。

CPU与内存NUMA位置应按rail对应。既有SetupRail由本rail应用线程分配并首次写入stage/destination，本次scatter沿用该线程；实际内存归属仍受目标机NUMA策略影响。重点观察端到端avg/p95/p99、各rail的ready积压以及最后通知到全部scatter结束的尾部时延，不预设双线程一定获得两倍提升。
