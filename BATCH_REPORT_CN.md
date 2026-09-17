# v6批量case实现与验证

日期：2026-09-17。工作目录 `C:/code/RDMA_DEMO/perf_test_duo_card_sgl`；分支 `duo_card_sgl`；开始核对HEAD为 `26f52304793c8548d5c80113fe77d4f38f05bb45`，工作树/暂存区干净。本次直接在该分支修改，没有新分支/worktree，没有改共享ubs-comm、main/duo源码或main参考文档，没有新增Python或恢复生产self-test入口。没有适用于该路径的AGENTS.md。

状态：**IMPLEMENTED / LOCAL_CPP_VALIDATION_PASS / TARGET_BUILD_AND_HW_PENDING**。

交付源码rdma_600.cpp SHA256：`B1B4B4EBE70AC39313FE3329940D603D300708AB919B154A4296A219AA5F5CBA`。此功能阶段已完成验证；后续模块拆分以独立提交保留此阶段基线。

## 实现结果

- 默认192个case：1024B的100～9600/步长100完整列表，再运行656B的同一完整列表。每case默认20 verify、100 warmup、1000 measure；支持自定义块数列表或起止/步长、单长度/单case。mode、links、K、pipeline保持固定，不扩展组合扫描。
- 参数矩阵在连接后逐项精确协商：每项56B，包括magic/version/reserved、矩阵长度、索引及40B完整CaseParameters。每项Call/Reply逐字节核对，无哈希碰撞或“只比第一个case”的盲区；远端按进展逐项等待，长列表不共用单个10秒总超时。
- MR和最大请求/WR/chunk缓冲仅规划一次。稀疏MR最大总39321600B，SGL stage最大总9830400B；两rail各自独立MR。奇数N按连续ceil(N/L)分区，最后rail取实际余数；chunk/通知/scatter使用每rail实际数量。测试覆盖K1和两个rail chunk数不同的情况。
- 原乘11目标置换在N可配置时会产生冲突，已改为对任意N成立的反向循环置换；源重复仍合法，目标重复拒绝。
- logical COPY_REQ严格为64+16N字节，最大153664B。每片32B头+最多16000B正文，最大10片，总应用消息payload153984B。保留服务16384B容量和禁用内部split/RNDV的约束；完整地址生成/编码、分片复制/发送、pending重组、active复制/解码、WR构造全部留在sparse_copy时延内。
- pending严格校验generation、总长、offset、片长、保留位和发布状态。最后一片才发布；app加锁复制到独立active后才能复用pending。active及逐chunk通知缓冲不在所有rail数据/发送callback结束前复用。
- 每caseSTART/READY仅在计时外；末尾FINISH/ACK复用两rail固定app线程逐rail执行，在local全部scatter及完整校验、remote全部callback结束后达成屏障。两端再drain活跃handler/callback并切换参数。generation全矩阵递增，ready/consumed槽保留generation，不清零伪装新轮；样本、wall时间、trace存储每case独立重置。
- 接收handler与case参数切换共享互斥锁，防止迟到/非法消息与参数写入构成C++数据竞争；这一host侧开销在实际往返内。固定rail app线程没有跨service调用。最后case已收到对应退出token的rail允许对端正常断链，未完成callback的错误/超时仍判失败。
- 逐轮更新每个source块首末8B generation标记，remote更新在时延内；local每轮在样本外、wall内验证所有目标标记。verify逐代写全内容，case结束验证完整内容/gap；主体在warmup/measure固定，头尾变化防止整块旧数据、漏WRITE或漏scatter仍通过。它不能证明任意局部DMA故障或非一致缓存可见性。
- local在全部case完成并最终安全退出后输出逐case JSON和汇总表。字段包含N/B/N×B、mode/links/K/pipeline、verify/warmup/measure、avg/p50/p95/p99微秒、同延迟口径的有效GB/s、另列wall GB/s等。失败run非零退出，标出case参数，不输出部分成功性能表。

`effective_GBps = N×B / average_latency_ns`（单位换算后数值恰等于十进制GB/s）；wall分母额外包含每轮目标标记验证和样本记录。分位下标为`floor(p*(n-1))`。源标记更新、新分片头和接收参数锁改变了测量成本，v6不能直接与旧v5稳定内容/9664B请求数据混表声称收益。

## 本地验证证据

环境：Windows/MSYS2 MinGW g++，真实 `ubs-comm@e709a37e71bc2493d2a195a14eaaefe86c2dfb28` 公共头。已有 ignored `build/stage3_compat` 仅提供Linux声明/头路径兼容；不是Linux/AArch64链接，也没有HCOM服务、QP、DMA或NIC性能运行。WSL未安装，当前没有可用目标机SSH配置；hosts示例仅含占位符。

完整生产源码语法检查（exit 0）：

```powershell
# 工作目录 C:/code/RDMA_DEMO/perf_test_duo_card_sgl
& C:/msys64/ucrt64/bin/g++.exe -std=c++17 -Wall -Wextra -Werror=return-type -Wno-pedantic -Wno-unused-parameter -Wno-sign-compare -fsyntax-only -include build/stage3_compat/compat.h -Ibuild/stage3_compat -IC:/code/RDMA_DEMO/ubs-comm/src/hcom -IC:/code/RDMA_DEMO/ubs-comm/src/hcom/service_v2/api rdma_600.cpp
```

独立C++验证放在ignored build目录，不增加生产宏/CLI入口/测试逻辑。`build/batch_validation.cpp`从当前完整源码去除main、将访问限定符private改成public，并拼接 `build/batch_checks.inc` 生成；算法/处理函数本身未替换。未用到的真实网络函数在链接时丢弃。用实际公共 `UBSHcomServiceContext` 的派生对象提供消息内存，直接调用生产handler；用memcpy模拟实际生产构造出的Put/PutV地址，不模拟硬件成功或计时性能。

编译/执行命令（exit 0，日志 `build/batch_validation.log`）：

```powershell
& C:/msys64/ucrt64/bin/g++.exe -std=c++17 -O2 -ffunction-sections -fdata-sections '-Wl,--gc-sections' -Wall -Wextra -Wno-unused-function -Wno-unused-parameter -Wno-sign-compare -include build/stage3_compat/compat.h -Ibuild/stage3_compat -IC:/code/RDMA_DEMO/ubs-comm/src/hcom -IC:/code/RDMA_DEMO/ubs-comm/src/hcom/service_v2/api build/batch_validation.cpp -o build/batch_validation.exe
& ./build/batch_validation.exe
```

| 检查 | 结果与边界 |
|---|---|
| 默认矩阵、单case、列表/范围、非法/重复参数、K30/低cap/缺cap拒绝 | PASS；调用生产ParseOptions/ValidateSglCapability |
| 全部整数N=100..9600 × B=1024/656 × links=1/2 | 38004种布局PASS；目标唯一、源范围、rail总数、K1/8/16/30尾chunk |
| request/wire/fragment | PASS；包含995/996/997的15984/16000/16016B边界、9600最大请求；canary、精确回组、短/长消息、乱序、重复、错代、各头字段破坏均检查 |
| HELLO | PASS；独立256B长度、末尾key字节、255B拒绝、参数不一致拒绝 |
| 矩阵不一致 | PASS；直接调用生产OnMatrix拒绝损坏项 |
| direct/SGL生产路径 | 18种固定配置×20个case=360软件case、1080轮PASS；links1/2，direct及SGL K1/8/16/30 on/off，N=100/101/110/995/996/997/600/601/9599/9600，B两种 |
| 通知/scatter/切换 | PASS；真实生产OnCopyReq/ReceivePending/DecodeActive、BuildRemotePut/SglRequests、OnDataDone/OnChunkDone、WaitAndScatterSgl；逆序通知、延迟最后rail尾chunk、延迟请求callback、重复CHUNK_DONE拒绝、全内容/gap、头尾旧代检测、连续generation、FINISH/ACK跨case、pending跨case拒绝 |
| JSON/统计口径 | PASS；单独的明确合成样本夹具验证JSON解析、656B有效分子、微秒/GBps换算和小wall秒数的9位精度；未发布合成性能结果 |
| 其它 | build.sh语法、hosts JSON、git diff --check、main_reference未改、共享依赖和其它worktree无修改PASS |

K30在软件验证中只走布局/内存模拟；生产参数检查仍基于公共头cap16拒绝，不能把它写成K30硬件支持。验证没有执行真实发送调度、SQ背压或硬件故障注入，也不能证明线程/TLS库契约受到支持。

## 依赖实查与目标机待验证

- 依赖HEAD仍e709a37，`src/hcom/hcom_def.h` 与C API的SGE上限16。没有改头文件冒充K30。实际链接静态库及公共头一致性需要目标机hash证据。
- `service_channel_imp.cpp::OneSideSglAsyncWithWorkerPoll`选择`NextWorkerPollEp(ep,0)`；同key/连续远端地址的SGE组进入一个WRITE。demo保留每rail单channel、linkCount=1、WORKER_POLL、内部multirail关闭、WRITE之后同channel CHUNK_DONE。
- `rdma_verbs_wrapper_qp.cpp`记录的是CreateQp前请求cap，公共service API不提供创建后真实QP cap查询。环境cap仍仅外部声明。目标机须保存每个QP编号、创建后query、设备/NIC、两端库与程序hash。
- `service_ctx_store.h::GetOrReturn`仍明确禁止同时创建同协议两个service。固定线程只是诊断性workaround，不能消除库契约限制；TLS cache/pool退出次序及失败回调所有权风险未在共享库修复。
- 需要Linux/AArch64真实链接，双机verify/trace/measure；验证WRITE与CHUNK_DONE同真实QP、DMA可见性、每chunk num_sge/地址、双NIC路由与各自流量、CPU/NUMA/MTU、最大N及K1的队列背压、单rail延迟、断链、部分post、callback/scatter延迟。

本次没有真实双机性能数据。运行命令和表格口径见 [README.md](README.md)。
