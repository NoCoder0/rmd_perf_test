# 阶段3：SGL gather + 流水 scatter 设计

日期：2026-09-14。状态：DESIGN_COMPLETE；实现已落地，见 `STAGE3_REPORT_CN.md`。硬件能力与性能仍未验收。

## 1. 精确基线、文档及范围

- 用户已确认阶段1.5重构完成并授权设计→独立实现会话→检视及回修。
- 新分支 `duo_card_sgl`，worktree `C:/code/RDMA_DEMO/perf_test_duo_card_sgl`，起点为 `duo_card@b3f4e4e43ff87ffe2a1544a648bc0b01904b3806`，创建时源工作区干净。不能退回旧 8970f08。
- 依赖只读基线 `C:/code/RDMA_DEMO/ubs-comm@e709a37e71bc2493d2a195a14eaaefe86c2dfb28`。用户称 ubs-hcomm，本地实际目录为 ubs-comm。
- main@4719c50d22a7fcb8cc7f1cb93b0511c3b4087105 的全部五份已跟踪 Markdown 原样保存于 `docs/main_reference/`，另保存现存历史 review。以 Git blob 校验五份正文完整性。历史 Python 命令仅属于原始文档内容，不恢复 run.py，不新增 Python 内容或运行脚本。
- 当前本分支的规范优先级：本设计及后续阶段3报告→本分支 direct 设计→main 历史参考。main 的单 app、多 rail 内存布局、v3、旧 SGE30 pin 均不能直接覆盖 duo 的固定线程、每 rail MR、v4 基线。
- 本次交付 B1/B2 direct 回归、S1/S2 SGL、流水 on/off、参数/协议/内存映射自测、可执行启动说明、检视回修。无 IMM、成功 ACK、跨代窗口、callback 池或多 service 架构改造。
- 首版固定 `sparse-600`，每轮真实传输全部600个源偏移和600个目标偏移，9664B COPY_REQ。main 的 grouped-source 是保留的后续独立实验：未提供真实业务源组契约，不将任意600个源地址伪压缩成20个。此决策不阻碍 SGL 的8/16/30布局。

## 2. 配置与能力边界

CLI 保留 `--links 1|2 --mode direct`；新增 `--mode sgl --pipeline on|off`。SGL 默认 on，direct 不做 stage/scatter，拒绝不适用的 off。

`RDMA_600_SGL_ITEMS` 控制 chunk 的最多SGE项数 K（不是字节数），默认16；严格十进制范围1..30，拒绝空串、负号、空白、尾随字符、0和溢出。direct 的有效 K 固定0，不因环境变量存在而改变 direct 工作量。两端 HELLO/READY 必须核对有效 mode/K/pipeline/source format，不自动取较小值。

布局支持1..30，部署先运行8/16。实际可运行条件是 K 不超过编译公共头 `NET_SGE_MAX_IOV`、与其匹配的实际链接库上限及该 rail **真实创建的QP** max_send_sge。当前库头/内部/C API 均为16，因此 K30 在该依赖下明确 `UNSUPPORTED`，不夹到16、不拆为多个WR冒充K30、不仅改demo宏或ABI数组。不改共享 ubs-comm；将来扩容需独立依赖补丁、两端全量重编和QP证据。

库已有 gather 路径的源码证据（行号随实现更新）：

- `src/hcom/service_v2/service_channel_imp.cpp`：PutV→非空callback→OneSideSglAsyncWithWorkerPoll→NextWorkerPollEp(ep,0)→PostWrite；内部 multirail 的 SGL 始终用 driver0。
- `src/hcom/transport/rdma/verbs/rdma_worker_io.cpp`：按相同 rkey 且远端地址首尾连续分组；一组分配一个WR上下文。
- `rdma_verbs_wrapper_qp.h::PostOneSideSglGrouped`：组内多个本地SGE→一个连续远端WRITE，IBV_SEND_SIGNALED。
- `rdma_verbs_wrapper_ctx.h/.cpp`：mMaxSge从库上限与设备max_sge取小；`rdma_verbs_wrapper_qp.cpp` 在CreateQp之前的日志只是请求值，不能当创建后实际QP cap。

当前公共 service/channel API 未提供实际QP cap读取；不得将设备cap、环境变量或上述创建前日志标成自动查得的QP cap。首版用外部诊断证据输入 `RDMA_600_QP_MAX_SEND_SGE`（每rail一个正整数，逗号分隔，数量严格等于links）登记**该次部署真实QP查询**所得值。证据需在独立目标机诊断中绑定程序/库hash、QP号、NIC、创建后query结果；程序将来源记录为 `external-declaration`，不伪称自动验证。缺少声明可跑 verify/trace并标 `QP_CAP_PENDING`，不得输出合格measure性能；measure启动即拒绝缺少cap的SGL配置。声明小于K明确UNSUPPORTED；声明本身不是硬件验收证据，报告仍须核对原始query记录。direct无需该声明。

所有配置校验在计时外，双方分别校验本地能力；协议比较工作量配置，不要求两端硬件cap数值完全相同。不能把库编译上限当作实际链接产物的hash证明。

## 3. 固定工作量与内存布局

N=600，B=1024，stride=4096，payload=614400B；L=1或2，R=N/L。每rail独立source/destination MR继承duo：R槽、R*4096B，源允许重复，dst槽唯一，offset按本rail MR校验。请求索引决定rail，不能按offset数值选rail。

仅 SGL 的 local 分配/触页/注册额外每rail `R*1024` 字节连续stage，总614400B；由该rail固定app线程注册并导出完整stage MemoryKey。dst保留基线MR以保持布局；remote的source仍为稀疏stride池，无额外CPU gather。stage在发布key前初始化，READY后不能清零可能已开放的写目标。

每rail chunk数 C=ceil(R/K)。chunk c：`first=c*K`，`count=min(K,R-first)`，`global_first=rail*R+first`。对j=0..count-1：

```text
i = global_first+j
iov[j].lAddress = rail.source_base + active_entries[i].src_offset
iov[j].lKey     = rail.source_key
iov[j].rAddress = rail.peer_stage_base + (first+j)*1024
iov[j].rKey     = rail.peer_stage_key
iov[j].size     = 1024
```

因此一个PutV内rkey相同、远端连续，预期groupCount=1，num_sge=count。local散写：`memcpy(rail.dst_base + entries[i].dst_offset, rail.stage_base + (first+j)*1024, 1024)`。不能把stage槽当最终dst槽，不能使用source offset作为stage offset。全部乘加、base+length以及region bounds以减法或checked arithmetic验证，拒绝溢出和跨rail key。

| L / K | 每rail chunk数 | 总WRITE / CHUNK_DONE WR | 每rail尾chunk项数 |
|---|---:|---:|---:|
| 1 / 8 | 75 | 75 / 75 | 8 |
| 1 / 16 | 38 | 38 / 38 | 8 |
| 1 / 30 | 20 | 20 / 20 | 30 |
| 2 / 8 | 38 | 76 / 76 | 4 |
| 2 / 16 | 19 | 38 / 38 | 12 |
| 2 / 30 | 10 | 20 / 20 | 30 |

K1最坏每rail600个chunk，状态/通知数组按实际C预分配或容量600；不得沿用128上限而允许K1导致越界。资源背压仍由异步库处理，不能逐chunk同步等待来替代流水。

## 4. 协议

统一升级为 `sparse-copy-v5-dual-rail-sgl`，version5及新magic，direct也使用v5以避免同版本异构消息。不能与旧v4或main v3混装。整数显式网络字节序编码，拒绝长度不符、尾随字节、保留位、未知mode/format。

- Parameters由32B扩为40B，追加mode/u16、K/u16、pipeline/u16、source_format/u16；direct=(direct,0,off,direct-pairs)，sgl=(sgl,K,on|off,sparse-600)。所有消息携带/校验一致参数。
- HELLO继承dst descriptor，追加stage区域ID/u32、stage基址/u64、stage长度/u64、完整80B key，总252B（旧144+参数8+stage100）。direct的stage字段必须全0，SGL stageID=3、长度R*1024。READY随params扩为64B，source key仍不导出。若实现增加能力字段，必须更新确切长度/自测/报告，不复用v4长度。
- COPY_REQ保持64B头+600对(u64 src,u64 dst)，总9664B；复用头中已有mode/K/source_format/stageID字段，SGL填有效值，direct保留0。pipeline已在握手锁定；不占用地址正文、不减少请求成本。
- 新增CHUNK_DONE opcode（现有700..706以外），40B：magic/u32、version/u16、rail/u16、generation/u64、chunk_id/u32、first_item/u32（rail内）、item_count/u32、payload_bytes/u32、chunk_count/u32、reserved/u32=0。接收rail来自handler注册绑定并与wire比较。
- SGL每个PutV成功post后立即在**同channel、唯一真实QP** Send对应CHUNK_DONE；不等待该PutV本地callback后才发，也不等一rail全部数据再批发通知。SGL无需额外DATA_DONE；direct仍每rail一条DATA_DONE。
- 每条真实QP上的WRITE→SEND顺序以及DMA可见性必须目标机验证。保留WORKER_POLL、linkCount=1、内部multirail=false、禁用split/RNDV、16384B消息容量；仅channel id/links数字不足以证明QP/NIC。
- CHUNK_DONE必须校验当前generation、rail/c/count/first/bytes、当前模式、重复通知；当前代由local发送请求前release发布。过期/未来/重复/越界通知使run失败，合法跨rail或跨chunk交错可接受。ready槽以原子CAS防重复，并以release发布；app acquire后才读stage。

## 5. 线程、流水与生命周期

remote：rail0接收完整请求复制pending；app复制至active、严格解析本代请求后发布给rail1。两条固定app线程各自填本rail全部iov/通知buffer，循环PutV(c)→Send CHUNK_DONE(c)，提交后有界等待该rail数据及Send callbacks。active、iov、source、通知buffer直到所有rail本代回收后才能复用。通知buffer每chunk独立，不能在循环中覆盖单一Send缓冲。callback可先于API返回，attempted在调用前登记，按当前库callback所有权处理失败，不能盲目delete/retry同指针。

local：主app线程负责COPY_REQ及所有rail的CPU scatter；rail1固定线程仍负责自己的HCOM setup/握手/finish/teardown。主线程只读rail1 stage并写dst，不调用rail1 service，不破坏TLS pool归属。此版不额外增加scatter线程，on/off CPU配置一致。

```text
sparse_copy(g):
  t0 = NowNs(); absolute_deadline = t0 + timeout
  生成、校验、编码本次600对地址
  初始化本代消费状态并publish expected_generation=g
  rail0异步发送9664B COPY_REQ
  while 未scatter全部chunk 或 COPY_REQ本地callback尚未完成:
    检查fatal；周期性检查同一个absolute_deadline
    on: 轮转rail和chunk，acquire读ready==g，有就绪即执行该chunk全部memcpy
    off: 等所有chunk ready==g，之后用同一scatter函数遍历
    消费仅一次；无进展CpuRelax
  最后检查fatal/deadline；t1 = NowNs(); 记录t1-t0
```

ready与consumed状态独立，不清零上代原子来造成迟到通知被误接收；每代expected generation配合CAS识别重复。不要求每轮逐块计时，独立trace才记录ready/scatter begin/end。本代输入entries在全部scatter完成前保持不变。direct回归不分配stage，不调用scatter。

下一COPY_REQ就是上代stage/dst已消费的复用信用，成功路径没有chunk ACK/round ACK。remote上代callback尚未完成时允许下一请求进入pending，绝不能覆盖active；等待上代回收的时间自然计入下一次local调用。generation跨verify/warmup/measure连续且不回绕。

保留CPU relax、每256次检查deadline、app私有attempted、callback原子完成、缓存行隔离、ActiveCallbackGuard。SGL各处理环节使用从t0派生的同一deadline，不在每个等待/scatter步骤重置超时；不能给on/off不同预算。direct若统一修正deadline，应记录变更并回归。

## 6. 失败、退出与库限制

错误、断链、超时、非法通知、内容/gap不符、部分post失败均使整个run无效；有可用channel时COPY_ERROR，无法发送则断链/有界退出。保留当前可证明安全的callback drain和无法quiesce时退出保护，不释放仍在DMA或callback访问的buffer。

FINISH/FINISH_ACK只是退出握手；全部scatter、最后内容校验和两端回收成功后才输出合格结果。输出成功后若仍可能drain失败，需将成功输出移到最终安全点，不能留下status=ok的失败run。

HCOM多service thread_local pool约束仍在，B2/S2继续标 `diagnostic-unsupported-by-hcom-contract`。固定线程不等于库契约已支持多service。不得借本任务把一个app在两个service间交替调用，或静默改成内部multirail。TLS cache生命周期、service销毁次序必须检视并将尚缺证据列入报告。

库PutV失败重试路径存在callback所有权风险（PrepareTimerContext失败删除callback后SER_NEW_OBJECT_FAILED外层可能重试）；本任务不修改共享库、不宣称错误注入已通过。实现应沿用保守失败退出，检视明确区分demo新增错误与依赖既有风险；必要依赖修复以独立补丁建议交付，不能掩盖为正常重试。

## 7. 结果与验收

schema升5，case保留B1/B2，新增S1-K-on/off、S2-K-on/off。记录mode/K/pipeline、source_format、600/600地址数、descriptor9600/request9664、payload614400、stage总大小、chunks各rail/总数、request Send=1、data/notify WR expected、ACK=0、cap编译值/声明值/来源/验证状态、protocol、git/build身份。expected计数与实际verbs trace证据分开，不把PutV API计数标为测得WR。

verify性能字段null。合格measure要求统计有限正值、全部正确性和退出成功，记录sparse_copy avg/p50/p95/p99、有效GBps、block Mops、request GBps、wall。不同主机时间不能相减；不把网络与scatter重叠时间相加。源数据模式沿用基线verify逐代填、warmup/measure稳定种子，内容验证包含src重复、dst非顺序、gap和跨代复用。

本地无需硬件的C++自测至少覆盖：

1. B1/B2、S1/S2，K1/8/16/30布局及尾chunk，on/off参数和wire round-trip；坏env/模式/握手不一致、截断/尾随/坏key范围和地址溢出。
2. 任意合法600对映射；手工模拟stage gather→按chunk ready乱序→scatter，比较direct参考结果；on/off完全一致，全部614400B和gap、重复source及重复dst拒绝。
3. 就绪状态先收到慢rail之外的数据不提前完成；最后chunk、COPY_REQ callback延迟、错代/重复/越界通知不得提前返回；多代复用。
4. 用当前真实公共头全文件受限语法检查和self-test-only运行，保存命令/结果，明确不是Linux真实库链接或硬件验证；不新增Python。

目标Linux/AArch64另行验收：真实头/静态库一致构建，两端20 verify；实际QP cap和WRITE/Send同QP、每chunk groupCount=1/num_sge/地址验证；双NIC计数各约半payload，NUMA/CPU/MTU记录；单rail延迟、断链、部分post失败、callback/scatter延迟；on/off独立trace；支持的8/16（30当前UNSUPPORTED）各5次1000 warmup+10000 measure。K1用于布局/边界和背压正确性，不作为必做性能主矩阵。不以本地mock声称DMA、NIC或性能通过。

## 8. 实现交接及检视顺序

任务1完成点：本文件落盘、main参考完整、worktree/分支/起点核对及设计提交。随后才启动独立sol high会话，修改仅限此worktree，读取本文件和旧交接/适用AGENTS，保留main参考，不提交到main/duo或修改依赖。

实现完成后主会话检视协议、内存/原子发布、线程所有权、异步失败、cap诚实性与计时/结果；发现问题以位置、触发条件、影响、修复要求通知原实现会话继续修改，完成后复查和运行相关检查。交付 `STAGE3_REPORT_CN.md` 和 `STAGE3_REVIEW_CN.md`，硬件未执行保持TARGET_BUILD_AND_HW_PENDING。
