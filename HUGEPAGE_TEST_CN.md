# SGL 显式 hugetlb 对照（2026-09-21）

已实现，Linux hugetlb/RDMA 实机验证待设备执行。默认仍是原来的 `posix_memalign(4096)`；
新增 `--memory-backend hugetlb` 使用匿名私有 `mmap(MAP_HUGETLB)`，没有自动回退。
进程独立选择，不加入线上协议协商，所以可以只改变 remote。
当前基底为 `38f9be8`，保留 trace、compact、source_order、max-inflight 等已有功能。

## 实验基线与改动范围

保持 1600×656 B、stride=4096、K30、G32、pipeline=on、sequential-stride、单口和 remote max-inflight=0。
已有同层结果 MF/SGL 首次数据 post→最后 CQE 为228.12/404.31 μs，local e2e为351.04/539.83 μs。
两者都是54个数据WR（53×30SGE/19680B + 1×10SGE/6560B）。源顺序0/1已试过无收益。
窗口8实测在途35/36→8，但数据窗口404.31→404.81 μs、e2e539.83→544.77 μs；
提交跨度139.71→348.90 μs，完成尾部264.60→55.91 μs，完成25/50/75/100%几乎不变。
本次不重复窗口扫描，不把尾部缩短当成优化成功。

每条 rail 的 remote source、local destination（scatter目标）和 local staging 均走所选后端。
direct 模式的有效载荷缓冲区也支持该选项。控制消息、HCOM队列、回调池仍按原实现分配。
保留原来的绑核及 rail 所属线程上 `memset` first-touch，不引入 NUMA 绑定、迁移、MAP_POPULATE、
MADV_HUGEPAGE 或 MF 运行时依赖，不修改系统池、THP或内核设置。
源偏移排列、stride、首尾标记更新、IOV/WR数、K/G、通知、scatter、pipeline和计时边界不变。

## 页大小、失败和生命周期

- `--memory-backend aligned`：默认，原4096字节地址对齐。不能据此声称实际使用4 KiB物理页。
- `--memory-backend hugetlb`：读取 `/proc/meminfo` 的 `Hugepagesize`，按该页大小显式编码请求。
- `--hugepage-kb N`：仅hugetlb可用，严格正十进制、2的幂、单位KiB；由内核核验目标系统是否支持。
  必须大于实际 `sysconf(_SC_PAGESIZE)`。不假定AArch64基础页为4 KiB或大页为2 MiB。
- mmap失败抛出包含 logical/mapped/page/errno 的错误，非零退出，不产生成功性能结果。
  检查相应大小的池、各NUMA节点可用页、进程允许节点、权限和hugetlb cgroup限制；程序不自动更改它们。
- `Size()`、MR注册长度、HELLO/READY容量及边界检查仍使用逻辑长度。仅映射/释放长度向大页取整；
  检查零长度、非2幂页和取整溢出。分配失败不取得所有权，重复分配须先显式Reset，防止误释放仍注册的MR。
- 保持原有 callback drain→disconnect→MR注销→service销毁；缓冲区在 benchmark 析构时释放。
  普通内存free，大页munmap完整映射；异步未排空仍按原路径 `_Exit`，不冒险析构。
  部分rail/部分缓冲区初始化失败由既有teardown及RAII回滚。hugetlb首次访问失败等OS致命故障也不得当成功结果。

## 构建和最少设备命令

两端在当前SGL仓库的 `duo_card_sgl` 分支拉取修改，沿用已有构建配置：

```bash
git pull --ff-only origin duo_card_sgl
cmake -S . -B build
cmake --build build --parallel
```

已有build缓存继续沿用相同HCOM/boundscheck配置；没有缓存时用
`bash build.sh --ubs-root ../ubs-comm`，指定本次已验证的同一个HCOM产物，不需要改HCOM传输代码。
另附 `sgl_hugetlb_20260921.patch` 是以38f9be8为基底的离线交付快照。不能拉取远端时，可改用
`git apply --check /path/to/sgl_hugetlb_20260921.patch`，通过后再执行相同命令去掉 `--check`，然后构建。
拉取和应用补丁二选一；已有补丁时先保留本地改动，不重复应用，也不用reset清空现场。

两端保留原 `APP_CPU`、`WORKER_CPU` 和已验证的 `RDMA_600_QP_MAX_SEND_SGE` 环境变量。
脚本使用之前示例remote=192.168.75.87、local=192.168.75.86、端口19000；实际基线不同则在各机设置
`RDMA_IP` 和两端相同的 `REMOTE_ENDPOINT`。`HUGEPAGE_KB` 可留空使用机器默认值；指定时仅传给hugetlb进程。
脚本只运行单口，以免同时引入双口因素；固定20 verify、100 warmup、1000 measure，随后另启进程采集2轮trace。
等待对端超时120秒；先运行remote，再运行local，同一组合结束后再开始下一组。

| 组合 | remote命令 | local命令 |
|---|---|---|
| 原分配 | `bash run_memory_compare.sh remote baseline` | `bash run_memory_compare.sh local baseline` |
| 仅remote大页 | `bash run_memory_compare.sh remote remote-huge` | `bash run_memory_compare.sh local remote-huge` |
| 两端大页 | `bash run_memory_compare.sh remote both-huge` | `bash run_memory_compare.sh local both-huge` |

每条命令自动保存measure原日志、trace原日志，再输出一份 `sgl-角色-组合-compact.json`。
任一进程/解析失败就停止，不继续下一阶段；已存在日志会拒绝覆盖，重跑前另存上一轮文件。
先回传 baseline 与 remote-huge 的两端JSON，共四份；确认remote实际生效后，再执行both-huge并回传新增两份。
设备保留全部原始日志，失败时回传对应错误行。脚本不创建假的进程退出码证明，成功echo表示本机所有命令返回0。

已有日志也可单独压缩：

```bash
python3 compact_trace.py sgl-remote-remote-huge-trace.log \
  --measure-log sgl-remote-remote-huge-measure.log > sgl-remote-remote-huge-compact.json
```

双rail继续用原先两端完整启动命令（两项IP/endpoint/app CPU/worker CPU和QP cap），各进程只追加
`--memory-backend aligned` 或 `--memory-backend hugetlb`；每rail均独立覆盖全部对应缓冲区。
不要把单口脚本直接当成双口脚本。现有S2的HCOM多service契约诊断限制仍保留。

只读查看系统页大小和池，不自动调整：

```bash
getconf PAGESIZE
grep -E 'Huge|MemAvailable' /proc/meminfo
grep -H . /sys/kernel/mm/hugepages/hugepages-*kB/{nr_hugepages,free_hugepages,resv_hugepages}
grep -H . /sys/devices/system/node/node*/hugepages/hugepages-*kB/{nr_hugepages,free_hugepages}
```

若实际选择2 MiB大页，单口source/destination逻辑6,553,600 B各映射8 MiB，
staging逻辑1,049,600 B映射2 MiB；remote有效载荷需4页，local需5页。
这只是该页大小下本应用有效载荷的容量说明，不是系统总需求；其他页大小按各缓冲区分别取整。

## 如何确认生效和控制日志长度

每个缓冲区在初始化first-touch后、退出drain后各一条 `memory_identity` JSON。
字段包括角色/rail/用途/phase、requested/actual/fallback、逻辑长度、映射长度、大页大小和证据。
hugetlb只有显式指定页大小的MAP_HUGETLB成功后才标记，`evidence=MAP_HUGETLB-success`；
普通分配的 `mapping_bytes`、`hugetlb_page_bytes` 为null，表示分配器底层映射长度/大页大小未知。
原始日志保留地址供核对，compact省略地址，保留初始化和退出两份证据。

`mapping`只读取覆盖目标范围的smaps/numa_maps摘要：KernelPageSize、MMUPageSize、AnonHugePages、
Private/Shared_Hugetlb、VmFlags.ht以及每NUMA节点页数和其页单位。读不到的字段为null/unknown。
`scope=whole-intersecting-vmas`非常重要：malloc的VMA可能包含其他分配，统计不是精确的buffer独占页数，
多个缓冲区可能共享VMA，不能直接求和。THP字段为0也不是长期不会变化的承诺。
页类型和字段语义依据[Linux proc文档](https://docs.kernel.org/filesystems/proc.html)，
显式页大小及munmap约束见[mmap手册](https://www.man7.org/linux/man-pages/man2/mmap.2.html)。

compact保持原 `sgl-compact-v1`及既有阶段指标，新添 `memory`，旧日志继续解析并明确缺少页证据。
新日志检查单/双rail用途和init/exit覆盖、后端一致性、映射取整和hugetlb成功证据；缺失或矛盾标incomplete。
`--measure-log`附加同角色、同workload/后端/提交/HCOM身份的trace-off结果及那次进程自己的页证据，
不以trace进程的页证据替代measure进程。页类型/NUMA可能随运行变化，归因前逐份核对。
保留原首次post→最后CQE、完成25/50/75/100%、在途WR、最后组可消费、scatter及e2e指标。
所有/proc读取只在初始化和退出，无逐WR输出或测量热路径读取。

## MF真实映射只读核对

已核查正确参考仓库 `C:/code/RDMA_DEMO/memfabric_hybrid`：
`hybm_conn_based_segment.cpp::AllocMemory`优先MAP_HUGETLB，部分56位GVA分支还可能走HAL，
最后普通mmap回退由 `MF_HYBM_ENABLE_4K_PAGE` 控制；`=1`是允许回退，绝非强制普通页。
VMM host路径也可能请求MEM_HUGE_PAGE_TYPE。源码请求和实际分配是两种证据，不能直接宣布MF已用大页。
本次不复制MF固定GVA/MAP_FIXED，也不改MF源码或已有分析器改动。

在MF诊断运行中，取实际成功分配日志/实际数据MR对应的本进程payload地址及长度，在进程仍存活时：

```bash
# PID、PAYLOAD_ADDR(0x...)、PAYLOAD_BYTES取自这次MF运行；不是远端GVA或未提交的预留地址。
python3 /path/to/perf_test_duo_card_sgl/mapping_check.py "$PID" "$PAYLOAD_ADDR" "$PAYLOAD_BYTES" \
  > mf-memory.json
```

优先在分配并first-touch完毕、尚未开始正式测量时读取；若只能在执行中采样，另做诊断运行，
该轮不用于性能结论。避免进程退出后PID复用；没有对应地址、进程已退出或权限受限就明确未知，不据全进程总量归因。
`hugetlb_flag=true`及相关页大小是映射证据；THP看AnonHugePages；NUMA看numa_pages与numa_page_kb。
纯预留区域或还没触页时页分布可以为空，须在有效载荷实际分配/触页后取证。

## 本地验证及下一步判断

```bash
python3 -m unittest discover -s tests -p 'test_*.py'
bash -n run_memory_compare.sh
```

本Windows环境已通过30项CPU/解析检查及全C++翻译单元兼容头语法检查。
涵盖普通分配、默认页解析、非4KiB基础页、取整/溢出、失败后所有权、严格不回退、RAII/重复Reset/
munmap完整长度、参数、窗口回归、单/双rail身份、旧日志及measure/trace配对、只读映射解析。
Linux mmap分支由替身系统调用编译执行；不是Linux hugetlb成功、目标机完整链接或RDMA验证。
实机第一轮仍需构建成功、完整verify、两端退出0及各自actual/evidence匹配。

正式收益看trace-off的1000轮local avg/p50/p95/p99；少量trace用于解释完成曲线与末组就绪时间。
若仅remote大页有变化，再与两端大页比较接收侧增量；同时核对NUMA分布，不能把位置变化误当纯页大小效果。
大页是否缩小MF/SGL差距留待设备结果，不预先承诺加速。
