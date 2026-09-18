# COPY_REQ容量扩大：限制依据与验证

日期：2026-09-18。修改前基线为 `d6fb3bf`，本次只读共享依赖。依赖HEAD为 `ubs-comm@e709a37`，但工作区已有外部未提交修改：`hcom_def.h`、`api/capi_v2/hcom_c.h`、SHM/SOCK driver_oob及UB driver。其中公共头SGE上限已从16改为30；本次没有覆盖这些修改。所读 `hcom_def.h` SHA256为 `26410CBC14D3DCF6655BD1A3D574883808B33B0D648475614DB95E1D49357CA2`。容量上限仍为524288000B。

## 16000B的来源与库限制

16000B不是ubs-comm固定上限，是demo先前为自己设置的16384B服务段预留头部余量后的应用分片阈值。旧最大N9600因此发送10片。

实查依赖源码：

| 位置（相对ubs-comm） | 确认内容 |
|---|---|
| `src/hcom/service_v2/api/hcom_service_def.h:132` | `maxSendRecvDataSize` 是服务选项，可配置 |
| `src/hcom/service_v2/service_imp.cpp:1783` | 该值传给driver的 `mrSendReceiveSegSize` |
| `src/hcom/hcom.cpp:1347` | `ValidateSegOptions` 接受1..524288000B，即最大500MiB；这是配置校验上限，不保证硬件可用 |
| `src/hcom/transport/rdma/verbs/rdma_verbs_wrapper_qp.cpp:254` | `PostSendMaxSize` 取对端交换的receiveSegSize |
| `src/hcom/transport/rdma/verbs/net_rdma_async_endpoint.cpp:34` | 实际非TLS Send payload上限为 `min(本地段, 对端段)−sizeof(UBSHcomNetTransHeader)` |
| `src/hcom/transport/rdma/verbs/rdma_validation.h:54` | `SizeValidate` 拒绝超过allowedSize的消息 |
| `src/hcom/transport/rdma/verbs/net_rdma_async_endpoint.cpp:237` | 当前异步Send带opInfo路径复制payload到注册MR，并前置HCOM传输头 |

所以不能只把应用的16000改大而保留16KiB收发段；两端都要更新。当前程序关闭TLS与内部split/RNDV，上述普通Send路径适用。MTU不等同于Send消息上限，一次Send仍可能形成多个网络包。

## 实现

- `src/common.h` 集中定义rail0段256KiB、其他rail段16KiB；应用分片正文上限为 `262144−sizeof(UBSHcomNetTransHeader)−32`。当前公共头传输头为28B，故正文上限为262084B，实际按请求长度发送，不发送填充字节。编译断言检查头部开销和当前最大请求能一次发送。
- `src/transport.cpp::SetupRail` 两端同编号rail使用同样容量。只有rail0承载COPY_REQ，避免放大另一个rail的小消息池。队列深度、buffer数量、callback、数据PutV和CHUNK_DONE策略不变。
- 协议版本升至7，名称 `sparse-copy-v7-large-request`，schema也升至7，握手/矩阵参数拒绝旧v6。保留既有magic作为消息族标记，实际兼容性由显式版本字段检查。不能混用旧版peer。
- JSON新增分片正文容量、请求服务段大小和控制服务段大小；请求Send数和传输payload字节沿用公式自动更新。地址列表、分片头、编码/复制/接收/解析仍在原计时路径，未预生成或省略请求。

| 每轮 | N600 | N9600 |
|---|---:|---:|
| 逻辑请求64+16N | 9664B | 153664B |
| 应用Send payload，含32B头 | 9696B | 153696B |
| COPY_REQ Send数，旧→新 | 1→1 | 10→1 |

SGL=16时N600仍为38次PutV与38次CHUNK_DONE，和此次容量调整无关。

## 注册内存成本

依赖 `service_v2/service_imp.h:56` 默认 `maxSendRecvDataCount=8192`，`net_rdma_driver.cpp::CreateSendMr` 创建段大小×数量的发送MR池。没有修改该数量，以免同时引入池耗尽/背压行为变化。

rail0每端发送池的buffer数据区因此从 `16KiB×8192=128MiB` 增至 `256KiB×8192=2GiB`，增加1.875GiB；这是单rail、单进程发送池，不是程序总内存。QP接收池也随段增大，另有应用source/stage/destination及元数据。双rail时rail1仍是128MiB发送池。目标机须具备相应内存和注册资源；分配失败应报错，不静默回退到16KiB或省略请求。减少池数量会改变资源压力，应另行比较，不混入本次调整。

## 本地验证及硬件边界

使用真实ubs-comm公共头、MinGW GCC15.2、现有Windows兼容shim，重新编译10个cpp及外部测试；工厂替身被调用即抛异常，未运行实际RDMA。验证记录 `build/large_request_validation.log`，检查脚本/可执行文件都在ignored build目录，没有生产self-test入口或Python。

- 38,004个N/B/links布局全部验证每轮COPY_REQ只需一次Send。
- 360个软件case、1,080轮及原错误路径回归：数据构造、scatter、通知、case重置和完成条件。
- N600与N9600精确发送长度、HCOM头后的容量约束、wire canary、解码一致性、截断/字段破坏/错代、完成后重复请求、超大逻辑长度拒绝。
- v6 HELLO元数据和v6分片拒绝；JSON schema、容量及每轮请求Send数检查。

首次测试停在历史“K30必须被cap16拒绝”断言：当前真实头已变为cap30。外部测试改为拒绝 `compiled_cap+1`，生产能力检查不变，随后重跑。K30软件布局结果不代表实际库/硬件支持。

目标Linux真实静态库链接、256KiB MR/QP创建、超过16KiB的真实Send、双机verify和性能尚待验证。优先两端运行N600/997/9600、B1024/656的verify，再保持相同100 warmup/1000 measure与旧版比较；保存峰值RSS/锁页/注册内存及实际库版本。没有声称吞吐或延迟已改善。

此前MEASURE_AUDIT报告中“v7范围通知”只是未实施候选名称；v7现用于本次大请求协议，未来通知合并需分配新的协议版本。
