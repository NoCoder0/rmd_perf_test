# MF / SGL 每轮源内容更新对照

两边新增同义选项：SGL `--source-update static|markers`，MF `--source-update=static|markers`。
默认保持 SGL=markers、MF=static。两端必须选同一模式；SGL 在已有参数握手中检查模式一致性。

| 模式 | warmup / trace / measure 的源数据操作 | 内容校验 |
|---|---|---|
| static | 不改写源内容 | SGL 保留最后一个完整 verify 轮的正文和标记；MF 保留初始化内容 |
| markers | 每块仅首尾各写 8 B | SGL 逐轮检查 generation 标记，结束检查正文；MF 按实际末轮 seq 检查标记和完整正文 |

SGL 的完整 verify 轮仍按原流程填充和校验，之后 static 冻结的是**载荷内容**，请求、通知、控制 generation
和目标地址轮换继续推进。static 是诊断模式，会弱化跨轮载荷新鲜度检测：相同块的旧内容可能通过校验。
没有关闭内容校验。MF markers 的最终 staging 和 scattered 两份内容都校验。

1600×656 B 时，markers 每轮改写 1600 块，块内偏移为 0、648，总写入 25,600 B；源 stride=4096。
两边使用相同的标记计算式，不在测量轮对源执行全量 FillBlock/memset。

## 构建一次，保留当前同源 HCOM

在两台 Linux 机器更新这两个应用仓库后，沿用已成功部署的 MF/HCOM 产物、ABI 和库路径。
本次未修改 HCOM，不需要重建它。若使用此前同源构建生成的 `output/stage-build.env`：

```bash
# 从 RDMA_DEMO 目录执行；仅重建两个 benchmark
cd memfabric_hybrid
export HCOM_SOURCE_DIR="$(cd ../ubs-comm && pwd)"
bash test/indirect_transport_test/build_hostrdma_bench.sh ./output
source output/stage-build.env
cd ../perf_test_duo_card_sgl
bash build.sh --hcom-include "$STAGE_HCOM_INCLUDE" --hcom-lib "$STAGE_HCOM_LIB" \
  --boundscheck-root "$STAGE_BOUNDSCHECK_ROOT" --build-type Release
```

若此前未用该 env 文件，直接复用原来的应用构建命令及实际 HCOM 路径。继续使用原运行环境的
`LD_LIBRARY_PATH`、MF 绑核/提交配置及大页设置。

## 手动 trace

先在两个终端设置 `MODE=static`，完成 MF、SGL 各自双端运行；随后两端改为 `MODE=markers` 再运行一次。
得到 MF-static、SGL-static、MF-markers、SGL-markers 四组、共八份端日志。不同程序不同时运行。
本次统一 warmup=100、trace=3 轮；SGL 保留 verify=20。不要直接用历史 warmup=10/100 的异步结果替代新对照。
不需要 measure，也不使用运行包装脚本或自动上传。

SGL 示例沿用最近成功的大页实验：local=192.168.75.87，remote=192.168.75.86。
MF 示例沿用原 MF 运行文档：local=192.168.75.86，remote=192.168.75.87。现场以各自原命令为准。
`APP_CPU`、`WORKER_CPU` 在各终端填原来的应用核与轮询核；MF 继续沿用原来的环境绑核参数。
MF 的 store 地址继续使用原来的可达地址（下面为此前文档中的 90.91.183.86:18580）。

两端公共设置：

```bash
MODE=static                         # 下一组仅改成 markers
export RDMA_600_SGL_ITEMS=30
export RDMA_600_SOURCE_SEQUENTIAL=1
export RDMAV_HUGEPAGES_SAFE=1         # 沿用上次大页成功运行的设置
unset MF_BENCH_RAIL_TRACE
```

MF：在各自 MF 仓库根目录运行。**先启动提供 store 的 local**，再启动 remote。
保留已经生效的 MF 大页、线程、QP、pipeline 和提交策略设置。

```bash
# local 终端
./test/indirect_transport_test/hostrdma_batch_bench \
  --role=local --rank=0 --store-url=tcp://90.91.183.86:18580 \
  --hcom-url=tcp://192.168.75.86:19000 --with-store=1 \
  --mode=cont --count=1600 --size=656 --stride=4096 --chunk=960 \
  --warmup=100 --rounds=3 --trace=1 --source-update="$MODE" \
  > "mf-local-$MODE.log" 2>&1

# remote 终端
./test/indirect_transport_test/hostrdma_batch_bench \
  --role=remote --rank=1 --store-url=tcp://90.91.183.86:18580 \
  --hcom-url=tcp://192.168.75.87:19000 --with-store=0 \
  --mode=cont --count=1600 --size=656 --stride=4096 --chunk=960 \
  --warmup=100 --rounds=3 --trace=1 --source-update="$MODE" \
  > "mf-remote-$MODE.log" 2>&1
```

SGL：在各自 SGL 仓库根目录运行。**先启动 remote**，再启动 local。
沿用最近的大页成功配置：双端 hugetlb=2 MiB、单 rail、K30/G32、pipeline on、窗口不限制。

```bash
# remote 终端；APP_CPU、WORKER_CPU 为该端原有绑核值
./build/rdma_600 --role remote --links 1 --mode sgl --pipeline on \
  --rdma-ip 192.168.75.86 --listen 192.168.75.86:19000 \
  --app-cpu "${APP_CPU:?填原remote应用核}" --worker-cpu "${WORKER_CPU:?填原remote轮询核}" \
  --blocks 1600 --block-bytes 656 --notify-every-wrs 32 --max-inflight 0 \
  --memory-backend hugetlb --hugepage-kb 2048 --source-update "$MODE" \
  --kind trace --verify-rounds 20 --warmup 100 --trace-rounds 3 --timeout-sec 30 \
  > "sgl-remote-$MODE.log" 2>&1

# local 终端；APP_CPU、WORKER_CPU 为该端原有绑核值
./build/rdma_600 --role local --links 1 --mode sgl --pipeline on \
  --rdma-ip 192.168.75.87 --peer 192.168.75.86:19000 \
  --app-cpu "${APP_CPU:?填原local应用核}" --worker-cpu "${WORKER_CPU:?填原local轮询核}" \
  --blocks 1600 --block-bytes 656 --notify-every-wrs 32 \
  --memory-backend hugetlb --hugepage-kb 2048 --source-update "$MODE" \
  --kind trace --verify-rounds 20 --warmup 100 --trace-rounds 3 --timeout-sec 30 \
  > "sgl-local-$MODE.log" 2>&1
```

## 运行结束后，手动 compact

两端程序正常退出后，在对应仓库、对应端执行各自命令。`MODE` 保持为刚运行的模式。
两边日志始终分开，不用文件名推断实际角色；compact 中的 role/host_role 才是依据。

```bash
# MF 仓库，local 端
python3 test/indirect_transport_test/analyze_hostrdma_trace.py --compact "mf-local-$MODE.log" > "mf-local-$MODE-compact.json"
# MF 仓库，remote 端
python3 test/indirect_transport_test/analyze_hostrdma_trace.py --compact "mf-remote-$MODE.log" > "mf-remote-$MODE-compact.json"
# SGL 仓库，local 端
python3 compact_trace.py "sgl-local-$MODE.log" > "sgl-local-$MODE-compact.json"
# SGL 仓库，remote 端
python3 compact_trace.py "sgl-remote-$MODE.log" > "sgl-remote-$MODE-compact.json"
```

优先比较每个实现内部 static→markers 的变化，再看 MF/SGL 差距是否随模式收敛。
新增 `config.source_update` 和 remote 的 `source_prepare_us`。源准备时间只包围源更新步骤，static
也包含空分支及打点开销；它仍在 local 端到端时间内，**不并入** `first_data_post_to_last_cqe_us`。
SGL 旧日志继续压缩，其原 `source_prepare_us` 为 decoded→prepared（标记 `legacy-decoded-to-prepared`）；
新日志标记 `prepare-only`。MF 旧日志没有源准备字段时不会伪造为 0。

继续检查 status、54 个数据 WR/CQE、1,049,600 B 完成覆盖，以及 25/50/75/100% 完成时间。
SGL 为 `completed_pct_us`，MF 为 `data_completed_pct_since_first_post_us`。这些均为同主机软件观察时间，
trace 自身有开销；源更新与缓存/DMA 的因果解释仍需看新实验结果，不能预先认定。

## 本机验证

Windows 上本任务相关的 SGL 32 项、MF 28 项测试全部通过，包括生产函数的 CPU 内容测试与解析/压缩回归。
并用现有 Windows 兼容头检查修改的 C++
翻译单元（含 MF trace 有/无 HCOM 两条编译分支）。这不是 Linux 完整链接或 RDMA 实机验证。

```bash
python3 -m unittest discover -s tests -p 'test_*.py'  # SGL 仓库
python3 -m unittest discover -s test/indirect_transport_test -p 'test_*.py'  # MF 仓库
```
