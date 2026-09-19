# RDMA sparse_copy：可配置块数/块长与连接复用批量测试

在 `duo_card_sgl` 分支阶段3上扩展。local拥有最终destination/stage并统计完整请求往返和scatter；remote拥有稀疏source并发起WRITE。当前协议为 `sparse-copy-v8-group-notify`，两端必须使用同版二进制，不能混用v7及更早的旧程序。

当前状态：`IMPLEMENTED / LOCAL_CPP_VALIDATION_PASS / TARGET_BUILD_AND_HW_PENDING`。本地检查不代表Linux链接、QP/DMA顺序、双NIC路由或真实性能通过。设计见 [BATCH_DESIGN_CN.md](BATCH_DESIGN_CN.md)，验证见 [BATCH_REPORT_CN.md](BATCH_REPORT_CN.md)。

源码已按local、remote、传输、协议、配置、数据路径和结果输出拆至 `src/`；入口保留在 `rdma_600.cpp`。职责映射与独立编译单元回归记录见 [MODULARIZATION_REPORT_CN.md](MODULARIZATION_REPORT_CN.md)。

measure开销、HCOM日志/trace实际控制条件及去重位图优化见 [MEASURE_AUDIT_CN.md](MEASURE_AUDIT_CN.md)。2026-09-19已实现可配置CHUNK_DONE分组通知，使用方法和验证见 [NOTIFY_GROUP_REPORT_CN.md](NOTIFY_GROUP_REPORT_CN.md)。当前没有本次分组优化的硬件加速比结论。

SGL scatter现在由每条链接的固定应用线程独立执行：rail 0使用主线程，rail 1复用已有的第二应用线程。设计、完成条件和并发验证见 [PARALLEL_SCATTER_REPORT_CN.md](PARALLEL_SCATTER_REPORT_CN.md)。

2026-09-18：COPY_REQ所在rail 0服务段从16KiB扩大到256KiB，当前N≤9600均一次Send。ubs-comm限制依据、发送池内存成本与验证见 [LARGE_REQUEST_REPORT_CN.md](LARGE_REQUEST_REPORT_CN.md)；此前批量/审计报告中的16000B和10片数据属于v6历史基线。

## 默认矩阵与配置

默认先运行全部1024B case，再运行全部656B case；每种长度按顺序遍历块数 **100、200、400、600、800、1200、1600、2400、3200、4800、6400、9600**，共 **24个case**。每case默认 **20 verify、100 warmup、1000 measure**。连接、source/destination MR、SGL stage及最大请求存储只建立一次，所有case复用。每case独立清空样本、校验并在所有rail安全完成后切换。

| 参数 | 含义 |
|---|---|
| `--blocks 100,601,9600` | 按给定顺序运行块数列表；每项100～9600，支持奇数，不允许重复 |
| `--block-start 100 --block-end 9600 --block-step 100` | 显式范围扫描；提供任一范围参数即启用，未提供的start/end/step分别取100/9600/100；end为包含上界，不强行加入未被步长命中的end；不能与`--blocks`并用 |
| `--block-bytes 1024,656` | 默认两种真实块长，也可只选1024或656；按长度列表顺序遍历完整块数列表 |
| `--warmup 100 --rounds 1000 --verify-rounds 20` | 每case轮数，可覆盖默认；verify必须大于0，measure的rounds必须大于0 |
| `--mode direct --links 1或2` | B1/B2，N个普通Put，无scatter，K=0，不接受`--pipeline` |
| `--mode sgl --links 1或2 --pipeline on或off` | S1/S2，源SGE gather→连续stage→CPU scatter；on为默认 |
| `--notify-every-wrs 1或4或8` | SGL专用，十进制1～9600；默认1。每条rail每G个数据PutV发一次CHUNK_DONE，尾组不足G也发；不改变K |
| `--kind verify或measure或trace` | 默认measure；verify无性能输出；独立trace需`--trace-rounds 1..64` |

mode、links、K、notify-every-wrs、pipeline在一次运行中固定，程序不扫描它们的组合。单case：`--blocks 600 --block-bytes 1024`。两端必须提供相同矩阵顺序、mode/links/K/notify-every-wrs/pipeline和轮数；启动后逐项核对完整矩阵，差异会失败退出。

## 构建与能力

在目标Linux/RDMA机器、此worktree源码目录构建：

```bash
bash ./build.sh --ubs-root /absolute/path/to/ubs-comm
```

`RDMA_600_SGL_ITEMS`控制K，默认16，严格十进制1..30。原始`ubs-comm@e709a37`公共头上限16；2026-09-18发现共享依赖已有未提交修改，公共头上限为30。本程序仍按实际编译头和声明QP cap检查，不自动降级；头文件允许30不能证明部署静态库和真实QP支持30。本次只读取并保留这些依赖修改。

SGL measure要求`RDMA_600_QP_MAX_SEND_SGE=<cap0[,cap1]>`，每rail一项，且至少为K。值必须来自本次部署真实QP创建后的query；程序只记录为 `external-declaration / declared-not-programmatically-verified`，环境变量不能证明硬件通过。verify/trace可不提供，标记`QP_CAP_PENDING`。

本程序没有内嵌self-test入口或Python运行脚本。`hosts.example.json`只作为配置记录，程序不读取它。

## 启动

先启动remote，随后local。以下是一条固定S2/K16/on配置的默认24-case measure。示例IP/CPU须替换为实际环境，QP cap必须来自实际证据。两个app CPU互不相同、两个worker CPU互不相同、任何app与worker不能重合。

remote：

```bash
export RDMA_600_SGL_ITEMS=16
export RDMA_600_QP_MAX_SEND_SGE=16,16
./build/rdma_600 --role remote \
  --rdma-ips <remote_nic0_ip>,<remote_nic1_ip> \
  --listen <remote_oob_ip>:19000,<remote_oob_ip>:19001 \
  --links 2 --mode sgl --pipeline on --kind measure \
  --app-cpus <remote_app0>,<remote_app1> \
  --worker-cpus <remote_worker0>,<remote_worker1> \
  --warmup 100 --rounds 1000 --timeout-sec 30
```

local：

```bash
export RDMA_600_SGL_ITEMS=16
export RDMA_600_QP_MAX_SEND_SGE=16,16
./build/rdma_600 --role local \
  --rdma-ips <local_nic0_ip>,<local_nic1_ip> \
  --peer <remote_oob_ip>:19000,<remote_oob_ip>:19001 \
  --links 2 --mode sgl --pipeline on --kind measure \
  --app-cpus <local_app0>,<local_app1> \
  --worker-cpus <local_worker0>,<local_worker1> \
  --warmup 100 --rounds 1000 --timeout-sec 30
```

先做小规模正确性检查时，两端追加/替换为 `--kind verify --blocks 100,101,997,9600 --block-bytes 1024,656`。101检查不均匀rail及尾chunk，997覆盖原16000B阈值以上的请求，9600检查最大单次Send。单case性能在两端追加 `--blocks 600 --block-bytes 656`。

单rail使用`--links 1`，各IP/endpoint/CPU列表只提供一个值，cap也只一项；可以使用`--rdma-ip/--app-cpu/--worker-cpu`别名。direct改为`--mode direct`并去掉`--pipeline`，SGL环境变量不改变direct工作量。独立trace建议明确指定单case，避免默认矩阵产生大量逐chunk日志。

测试通知合并时，两端原有SGL命令都加上 `--notify-every-wrs 4`（或8）；保持 `RDMA_600_SGL_ITEMS=30` 即保持K=30。两端需先重新编译为v8。同一版二进制用G=1作为基线，随后分别运行G=4/8。direct不接受该参数。

G按每条rail的数据PutV调用计数，每个PutV在当前预期SGL路径对应一个数据WR；它不控制post链表批量或CQ信号频率。N9600/K30时总计320次PutV，G=1/4/8分别产生320/80/40次CHUNK_DONE（单/双rail均如此）。尾组各rail独立计算，例如N600/双rail/K30/G4为每rail10次PutV、3次通知，共6次通知。G超过本rail的chunk数时，该rail整轮只有一次通知；增大G会推迟该组scatter的开始，实际收益需实测。

并行scatter自动随 `--links` 生效，无需新增参数。local的 `--app-cpus A,B` 分别绑定rail 0/1的scatter线程，`--worker-cpus`继续用于HCOM通知处理。每个线程只扫描和复制自己的rail，使用已有的独立stage/destination；CPU应按对应NIC/内存的NUMA位置选择。`pipeline=on`下各rail独立处理已ready的chunk；`pipeline=off`仍先等待**全部rail**就绪，再并行scatter。建议先保持 `--pipeline on --notify-every-wrs 1` 和原有K，与修改前的 `e831745` 对比，再单独测试其他G。

## 计时、结果和失败

每次logical COPY_REQ为`64+16N`字节，真实携带N个源偏移及N个目标偏移；最大153664B，当前矩阵每轮只需一次Send。保留32B应用分片头，最大Send为153696B。rail 0服务段为262144B，分片正文上限按`段大小−sizeof(HCOM传输头)−32`计算；另一rail仅收发控制消息，仍为16384B。构造、复制、Send、接收重组、解析的逐轮成本都在完整sparse_copy时延内。

每轮从请求准备前取local `CLOCK_MONOTONIC_RAW`，直到全部rail通知、全部CPU scatter（SGL）和全部请求Send callback完成后结束。不跨主机相减时钟。源端每块头尾generation标记每轮更新，**更新成本在延迟内**。每轮local头尾标记校验在样本外、measure loop wall内；verify轮和case结束做完整有效内容及stride gap校验。主体在warmup/measure期间固定，头尾标记可以发现整块旧代/漏写，不能证明任意局部DMA故障或缓存一致性。

全部case及最终drain/teardown成功后，local先输出每case一条schema8 JSON，再打印汇总表，每case一行，列为：

```text
case blocks bytes payload_B mode links K notify_every_wrs pipeline verify warmup measure avg_us p50_us p95_us p99_us GB/s wall_GB/s status
```

这里只展示表头，不提供模拟性能数值。JSON包含相同指标及协议/提交身份、首末generation、每rail块数/chunk数/通知数、`notify_every_wrs`、请求逻辑/传输字节、MR/stage容量、预期WR数、QP cap声明与验证状态。`completion_send_wr_per_round_expected`按分组后的通知数计算；direct的`notify_every_wrs`为0，仍每rail一次DATA_DONE。

local JSON新增 `scatter_threads`（SGL为links，direct为0）、`scatter_policy`（SGL为`per-rail-app-thread`）及 `scatter_off_barrier`。协议和schema保持v8，消息格式与通知数量保持原定义；用提交身份及scatter字段区分串行基线与并行结果。

- `avg/p50/p95/p99`：单次sparse_copy的微秒；分位取升序样本的`floor(p*(n-1))`下标。
- `GB/s` / `effective_GBps`：`measure次数 × N × B / 所有延迟样本之和`，十进制GB/s，与平均单次时延同一分母。656B按656B计算。
- `wall_GB/s`：同样有效字节除以measure循环wall，包含逐轮标记验证及样本记录，不包含verify/warmup、case握手、MR规划或最后完整校验。
- verify/trace无measure样本，JSON性能字段为null，表格为`-`。

请求错误、重复/错代通知、超时、断链、callback失败、校验失败均使整个run非零退出，stderr标注case、块数、块长。失败run不发布部分成功性能表；不会把未完成case填成零或成功。case屏障使用ACK，数据轮不增加成功ACK。

每rail固定app线程独占该service的HCOM调用；内部multirail关闭，channel linkCount=1。B2/S2仍标为`diagnostic-unsupported-by-hcom-contract`。目标机需要确认WRITE与CHUNK_DONE同真实QP、DMA可见性、双NIC流量/路由、实际cap、库/二进制hash、NUMA/CPU/MTU及故障注入。固定线程和cap声明不能替代这些证据。main历史原文保留在`docs/main_reference/`。
