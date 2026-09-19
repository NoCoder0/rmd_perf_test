# measure路径开销审计与后续A/B方案

> 2026-09-19：通知合并现已按v8实现为 `--notify-every-wrs G`，说明与验证见 [NOTIFY_GROUP_REPORT_CN.md](NOTIFY_GROUP_REPORT_CN.md)。下文“未实施”与v7候选命名是此前审计记录。

> 2026-09-18后续：本报告保留v6基线开销；当前rail0容量、单Send请求和依赖cap变化见 [LARGE_REQUEST_REPORT_CN.md](LARGE_REQUEST_REPORT_CN.md)。下文提议的“v7范围通知”未实施；版本7已用于大请求协议，未来合并通知需使用新版本。

日期：2026-09-17。审计对象：批量功能基线 `7d6e72a`、纯模块拆分 `bbf59d7`，只读依赖 `ubs-comm@e709a37`。本报告区分源码可确认的工作量、本地检查结果和未取得的硬件证据，不给出模拟加速比。

## 1. 已落地的变化

唯一运行时优化是 `src/data_path.cpp::ValidateCopyEntries` 的destination去重集合：从每rail 9,600个bool改成150个uint64_t位图。两个rail总存储由19,200B变成2,400B，每次调用少初始化16,800B；正常一轮local与remote各校验一次，合计少初始化33,600B。这是对象及清零工作量，不是实测内存总线流量。位操作也有成本，不能据此推导端到端提升。

计时开始后仍重新生成全部源/目标offset、验证对齐/范围/重复、编码、发送和解析完整请求。错误文本、首个失败的检查顺序、rail边界、允许重复source的语义完全保留。未改变协议、通知粒度、调度、超时检查、callback或stage生命周期。该优化与纯拆分分开提交。

验证证据（均在ignored `build/`，没有生产self-test入口或宏）：

- `bitmap_validation.cpp` 将修改前bool实现与当前函数逐次比较：**513,252个合法/非法输入**的bool结果及完整错误文本相同。覆盖N=100..9600每个整数、两种B、1/2 rail、随机generation、错数量、错对齐、越界、溢出值、容量不足、重复目标；额外逐一覆盖最大case每个目标位，包括63/64边界和最后一位。
- GCC15.2 `-O2 -S`：旧实现生成长度19,200的memset；新实现生成300次 `rep stosq`，即2,400B。这只证明该本地编译器确实减少清零量，目标AArch64代码仍需检查。
- 修改后重新编译全部实际cpp并运行原软件回归，结果记录于 `build/optimized_validation.log`；测试范围为38,004个布局、360个case、1,080轮和原错误路径检查。HCOM服务工厂为调用即抛异常的外部替身，DMA为软件复制；没有真实RDMA调用。

## 2. 应用计时与热路径

| 项目 | 现状与计时位置 | 判断 |
|---|---|---|
| 样本边界 | `local.cpp::SparseCopy` 在MakeCopyEntries之前开始，全部rail通知、scatter和请求Send callback满足后结束 | 完整操作边界保留；不跨主机相减时钟 |
| 输出与统计 | measure没有逐轮/逐chunk打印；case结果格式化、排序统计在case结束后，全部结果在最终drain/teardown成功后输出 | 不计入单次样本；错误日志仍可能发生，失败run不发布成功表 |
| 应用trace | `TraceIndex` 在measure返回false，不调用逐chunk trace时钟/Publish | 分支仍在，编译器是否消除需看机器码；库trace另行分析 |
| 时钟 | 起止各一次；请求准备后一次deadline检查；等待结束也查时钟，等待/扫描每256次检查，remote提交每256个WRITE/chunk检查 | 不能把measure成本描述成只有两次取时钟；保留超时保障 |
| 请求处理 | MakeCopyEntries逐项算rail和offset；local校验；编码64+16N字节；分片复制；remote重组、pending→active复制、解码和再次校验 | 全部是操作必要成本，保持在计时内；最大逻辑153,664B、10片，应用wire合计153,984B |
| 地址和WR | remote每轮重新检查地址并写N项Put/iov，复制每项的local/remote完整key，再构造每chunk请求 | 不能用预先发送模板或移出计时替代真实请求；库内还有iov转换/复制 |
| 数据内容 | remote每轮更新每块头尾8B generation标记，包含在延迟内 | 主体在warmup/measure固定，不能宣称逐轮全内容重填 |
| 结果校验 | 每轮local头尾校验、样本push在单次样本外，但在measure wall内；verify与case末完整校验 | effective吞吐和wall吞吐含义不同，保持原口径 |
| 内存分配 | 连接/MR、最大请求存储、entries、WR/iov、stage、样本容量在setup/case准备时分配或预留 | 不等于每轮零分配：每个异步API的callback仍new/delete；失败字符串亦有成本 |
| scatter | on按ready执行；off等全部ready；每块地址检查和memcpy在延迟内 | 未移动scatter、削弱完成门槛或改变尾chunk处理 |

`ScanReadySglChunks` 每轮扫描固定slot集合，包含linear取模/除法、`RailChunks`/rail块数计算、consumed检查和ready acquire load；等待无进展时CpuRelax。即使一部分chunk已完成，后续扫描仍访问它们。缓存本轮不可变chunk数量可作为低风险候选，但本轮未实施，也未声称除法必然留在目标机器码中。ready队列或缩小扫描范围会改变调度与公平性，必须单独比较。

每个incoming持有 `mCaseMutex`，会串行化两个rail的接收处理；`ChannelCopy` 加锁并复制带引用计数的channel。每个incoming/Send/data callback还通过 `ActiveCallbackGuard` 修改共享原子两次，完成计数再做原子RMW；CHUNK_DONE发布使用CAS与release/acquire，consumed由app线程拥有。这些操作维持case切换、trace-before-ready和对象释放顺序。不能直接去锁、降内存序或跳过计数。若考虑immutable case快照/per-rail callback计数，必须补齐epoch与最终quiescence证明及延迟callback故障测试。

## 3. HCOM日志：源码行为与实际配置边界

检查文件为依赖 `src/hcom/hcom_log.h`、`hcom_log.cpp`、`service_v2/service_channel_imp.cpp`，以及RDMA endpoint/worker/QP实现。

- `hcom_log.cpp` 默认级别为1（INFO）；`HCOM_SET_LOG_LEVEL` 接受0 DEBUG、1 INFO、2 WARN、3 ERROR。`NN_LOG` **先检查级别，再构造ostringstream/求值日志参数**。因此默认INFO时API Send/Put/PutV处的DEBUG不会格式化字符串，但单例获取/级别分支仍存在；首次logger初始化可能分配对象并读环境。
- DEBUG开启时API日志会执行流格式化和 `oss.str()`；默认输出路径还调用gettimeofday、localtime_r、strftime与cout。普通Print用换行字符，不应笼统声称每条都endl强制flush。安装external logger后输出成本由该回调决定。
- `NDEBUG`/Release不会删除 `NN_LOG_DEBUG`。`NN_LOG_TRACE_INFO` 只有定义 `NN_LOG_TRACE_INFO_ENABLED` 才编译成INFO调用，否则为空。当前源码中声明/使用该条件不能证明部署静态库是否带此宏。
- 已检查的正常异步API入口是DEBUG；INFO初始化日志一般在setup，WARN/ERROR可能在队列压力、分配失败或异常路径触发。内部split/RNDV相关日志不应算成当前关闭这些机制后的必经成本。
- 外部 `logging_probe.cpp` 链接真实logger，使用有副作用的参数与计数sink验证：默认INFO确实不求值DEBUG参数，DEBUG级别会求值，WARN/ERROR保留。两个 `-DNDEBUG -O2` 可执行文件分别不定义/定义TRACE_INFO宏，均通过。此测试验证当前源码的宏行为，不验证部署archive。

当前本地进程的 `HCOM_SET_LOG_LEVEL`、`HCOM_ENABLE_TRACE`、`HCOM_TRACE_LEVEL`、`HCOM_BUILD_TYPE` 均未设置；本地未找到部署用HCOM静态库/实际编译命令，也没有目标进程环境。因此不能断言目标机日志级别或库编译开关。目标机应记录启动环境，例如：

```bash
env | sort | grep -E '^(HCOM_|RDMA_600_)'
sha256sum ./build/rdma_600 /absolute/path/to/libhcom_static.a
grep -E 'CMAKE_BUILD_TYPE|CMAKE_CXX_FLAGS|BUILD_WITH_HTRACER' /hcom/build/CMakeCache.txt
grep -E 'NN_LOG_TRACE_INFO_ENABLED|HTRACER_ENABLED|NDEBUG|[[:space:]]-O[0-3s]' /hcom/build/compile_commands.json
```

路径需对应实际部署构建。compile_commands缺失时保存verbose构建日志，不能以grep无输出证明宏关闭。demo和HCOM是两个独立构建：依赖CMake用区分大小写的 `MATCHES "release"`，其build.sh会将类型转小写；手工传 `Release` 可能走另一分支。应核对真实flags，不能用demo的Release标签代替库证据。

可比measure基线建议两端明确设置 `HCOM_SET_LOG_LEVEL=1`、`HCOM_ENABLE_TRACE=0` 并保存环境；INFO级别已屏蔽DEBUG格式化。级别2可以用于单独诊断比较，但需保留WARN/ERROR且两端/各版本一致；本轮没有替用户修改环境，也没有移除依赖日志。

## 4. 库内trace、时钟、复制、池与CQ

- 应用trace与htracer独立。`common/trace/htracer.h` 的TRACE_DELAY_BEGIN/END检查 `g_htraceIntf.IsEnable()`；打开时调用时间采集和记录，关闭时仍可能有函数指针/条件开销。`htracer.cpp` 的可用性还依赖初始化与Enable状态，`NetTrace::HtraceInit` 读取 `HCOM_ENABLE_TRACE`，未设置时默认false并调用EnableHtrace。`HCOM_TRACE_LEVEL`是另一套NetTrace等级。
- `src/hcom/CMakeLists.txt` 声明 `BUILD_WITH_HTRACER` 默认ON；当前源码搜索未找到将该选项映射为去掉这些宏或定义HTRACER_ENABLED的依据，不能声称传OFF就没有trace成本。应记录实际预处理/编译结果与运行开关。
- RDMA endpoint的PostSend、PostWrite/Sgl含GetFinishTime及重试期限逻辑，与应用起止时钟和htracer关闭与否不同。库内计时并不因measure关闭应用trace而消失。
- 非TLS普通Send从注册发送MR池取buffer，清header、复制payload并计算header校验；应用分片后还有这次库内复制，因此COPY_REQ不是零复制。CHUNK_DONE也经过同一路径，虽只有40B仍承担固定成本。
- `service_channel_imp.cpp::PrepareTimerContext` 从线程局部池取timer、placement new、分配seq、引用计数并加入timer机制。PutV将完整key/iov转为transport iov；`rdma_worker_io.cpp::PostOneSideSgl` 再分配池上下文、复制iov、按rkey/连续远端地址分组，然后构建/提交WR。对象池减少部分堆分配，不等于没有管理开销。
- 公共 `UBSHcomNewCallback` 使用new创建closure callback，Run执行函数后delete this。源码可确认每个demo异步API至少一个该对象；不能无证据断言std::bind还会额外分配。本轮未改成复用callback：依赖还管理callback所有权，且PutV的PrepareTimerContext失败销毁done、外层SER_NEW_OBJECT_FAILED重试的组合存在原有所有权风险，需单独修库验证。
- API层Send/Put/PutV对部分资源不足错误 `usleep(100us)` 后重试；endpoint还有自己的重试，可能影响尾延迟，但没有目标机证据证明本次触发。不能把所有长尾归因于此。
- QP发送与WRITE使用IBV_SEND_SIGNALED，worker/CQ、context回收、锁和队列计数依然存在。通知合并不会自动消除每个data WR的CQ处理。demo设置send queue 1024；大量WR可能形成压力，是否触发WARN/重试需实测。

## 5. CHUNK_DONE工作量与合并候选（未实施）

定义每rail块数Rr，chunk数Cr=ceil(Rr/K)，C=sum(Cr)，请求分片数F=ceil((64+16N)/16000)。目前每轮SGL发C次PutV、C次40B CHUNK_DONE、F次COPY_REQ Send；demo callback数为 `2C+F`，不含控制协议及库内wrapper。direct则为N次Put、L次DATA_DONE和F次请求，demo callback数 `N+L+F`。

| case | C | CHUNK_DONE应用字节 | remote数据+通知API数 |
|---|---:|---:|---:|
| N600、K16、1或2rail | 38 | 1,520 | 76 |
| N9600、K16、1或2rail | 600 | 24,000 | 1,200 |
| N9600、K1、1或2rail | 9,600 | 384,000 | 19,200 |

字节不含HCOM/网络头，API数也不能无条件当成实测物理WR数：仍须验证实际groupCount、真实QP和提交行为。当前stage连续布局旨在每chunk一个data WR。

建议后续独立实验增加v7协议，HELLO/矩阵协商通知组大小G与策略，拒绝不一致peer；保留本报告v6二进制作基线，v7 G=1复现原粒度。先测G=2/4/8和每rail末尾一次，均为新候选，不能当作本轮已实现。

具体消息采用固定48B的RANGE_DONE：magic(u32)、version(u16)、rail(u16)、generation(u64)、firstChunk(u32)、chunkCount(u32)、firstItem(u32)、itemCount(u32)、payloadBytes(u32)、totalChunks(u32)、reserved(u64)。按已有显式端序编码，magic/opcode独立于CHUNK_DONE。接收端根据协商参数推导范围和尾组长度，检查所有字段、reserved=0、无溢出、generation匹配且firstChunk严格等于该rail的nextExpectedChunk；重复、重叠、跳组、越界和跨rail消息都失败。

remote在同一真实QP按顺序提交本组所有WRITE，随后提交一条RANGE_DONE，不等待每个WRITE callback再发通知；每rail独立提交并冲刷最后不足G的组，不能由rail0通知覆盖rail1。只有目标验证WRITE→SEND顺序与DMA可见性后，local才能一次发布本组所有chunk-ready，原scheduler/scatter及总完成门槛保留。每个data callback仍保留；通知callback目标数改为sum(ceil(Cr/G))。每条通知的buffer保留到自身Send callback；request/active metadata/stage不可在上一轮消费、remote回调和case drain尚未满足时重用。组内中途失败走错误/超时，不发布半组成功。

例：N9600/K16/2rail，G=1/2/4/8/每rail末尾分别有600/300/150/76/2条通知；data API仍600。G8单rail为75条，两rail因各自尾组变成76条。减少了通知Send、接收、callback和对应CQ固定成本，但首chunk须等同组后续WRITE提交后才能scatter，可能损失pipeline重叠、产生突发scatter及更高p99。off模式本就等待全部ready，仍不能保证一定收益。跨chunk真实DMA可见性、错误定位和超时行为必须重新验证。

## 6. 硬件A/B执行计划

1. 保留A=`7d6e72a`单文件、B=`bbf59d7`纯拆分、C=位图优化三个二进制及source/archive/hash。固定编译器、flags、架构、LTO、HCOM环境与库版本。先A/B识别拆分影响，再B/C隔离位图，不将两项收益混算。
2. 先完成目标Linux链接及verify；查询实际创建后QP的cap、num_sge/groupCount、WRITE与通知的真实QP、两NIC计数/路由、NUMA与CPU亲和、MTU和服务线程约束。当前依赖上限16，K30必须拒绝；B2/S2仍为 `diagnostic-unsupported-by-hcom-contract`，不得升级为库契约支持。
3. 默认完整矩阵：N100..9600步100，B1024/656；每个固定mode/links/K/pipeline组合单独跑，每case20 verify、100 warmup、1000 measure。重点补101/601/997/9599等不均匀和尾组case。K1/8/16；links1/2；SGL on/off；direct作原对照。
4. 每配置建议5个独立run，随机交错版本顺序，记录全部run而不挑最快。输出avg/p50/p95/p99、effective GB/s、wall GB/s与错误率，报告run间离散程度；CPU使用、CQ/WR/通知数、NIC有效与实际字节、重试/池耗尽记录另行采集。profiler/库trace只在单独诊断run打开，不能将其样本混入正式measure。
5. 通知合并独立比较v6、v7 G1、G2/4/8/rail-final；保持全部offset请求、源标记、数据校验、计时口径、cap和完成条件一致。trace检查ready→scatter的延迟及WRITE/scatter重叠，不能只报通知数量下降。
6. 注入延迟最后一rail/尾chunk、延迟请求和数据/通知callback、重复/漏通知、错generation/错rail/非法范围、断链和资源不足。确保不会提前结束样本/切case、stage提前覆写、callback后访问已释放对象；case失败非零退出且无成功表。软件测试可查状态机，不能替代RDMA顺序与DMA实验。

其他候选包括在本轮内缓存不可变chunk数量、减少中间分片复制、pending/active buffer交换、缩减key重复复制、callback池和ready队列。除单纯本轮数量缓存外，都涉及所有权、库契约或请求成本口径，需各自独立设计/验证；本轮没有实施这些改变。硬件资源目前不可用，上述A/B均待执行。
