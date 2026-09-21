# SGL 在途窗口 0 / 4 / 8 对照

2026-09-21更新：窗口8已实测生效，但首次数据post→最后CQE为404.31→404.81μs，
e2e为539.83→544.77μs，未观察到加速；完成曲线各点几乎不变，尾部等待转移到提交过程中。
当前默认沿用max-inflight=0，不再要求重复以下历史0/4/8扫描。
完整结果在父目录 `stage_analysis_20260921/WINDOW8_ANALYSIS_CN.md`；下一项大页实验见
[HUGEPAGE_TEST_CN.md](HUGEPAGE_TEST_CN.md)。以下保留实现语义与历史复现步骤。

目的：验证减少在途数据请求是否缩短完整传输时间和 local e2e。仅末次提交到最后CQE变短，不代表性能改善。

## 参数与行为

- `--max-inflight N` 只加在 **remote** 命令上，范围0–9600，默认0表示不限制。
- 按每条rail的“累计已提交数据PutV − 累计完成数据callback”控制窗口，完成一个即可补一个。
- 上限针对数据PutV请求，不包含通知SEND。当前K=30的实测路径每个数据PutV对应一个WR；其他HCOM配置可能拆分WR。
- 窗口在verify、warmup、measure和trace中一致生效，跨轮累计计数不清零；双rail分别限流。
- G=32、W=4不会等待32个数据请求一起完成才继续，也不依赖local通知来释放窗口。
- 原有错误检查和绝对超时处理继续生效。未启用窗口时不增加完成计数读取和窗口计时。
- 参数不修改SQ、源数据更新、请求布局、K/G、通知顺序、callback或scatter逻辑。

remote最终JSON和紧凑JSON的config新增`max_inflight`、`inflight_limit_unit`。
这个实验开关不改变线上协议，不在两端协商；local不接受该参数，也不会虚构remote的窗口值。
请用相同的文件名后缀`w0/w4/w8`配对两端结果。

## 更新和编译

两台机器在SGL仓库执行。已有build目录时复用原来的HCOM/boundscheck配置，不必重建HCOM：

```bash
git pull --ff-only origin duo_card_sgl
cmake -S . -B build
cmake --build build --parallel
```

没有已有CMake缓存时，沿用原来的`bash build.sh --ubs-root ...`或显式库路径构建命令。

## 运行：每次先remote，再local

保留此前的物理网口、NUMA、绑核、库路径、QP能力声明和源地址顺序设置。以下IP来自仓库原示例，
实际使用原基线地址；`APP_CPU`、`WORKER_CPU`在各机设置为此前对应角色的核。
`RDMA_600_QP_MAX_SEND_SGE`保留此前已验证的声明，不要重新猜测数值。

两端各设置公共参数，每次将`W`分别设为0、4、8；W在local仅用于标记文件名：

```bash
export RDMA_600_SGL_ITEMS=30
# local保留本次已采集基线的顺序：
export RDMA_600_SOURCE_SEQUENTIAL=1
COMMON=(--links 1 --mode sgl --pipeline on --blocks 1600 --block-bytes 656 \
        --notify-every-wrs 32 --warmup 100)
W=0
```

先跑正式measure，remote：

```bash
./build/rdma_600 --role remote --rdma-ip 192.168.75.87 \
  --listen 192.168.75.87:19000 --app-cpu "$APP_CPU" --worker-cpu "$WORKER_CPU" \
  "${COMMON[@]}" --kind measure --rounds 1000 --max-inflight "$W" \
  > "sgl-remote-w${W}-measure.log" 2>&1
```

local：

```bash
./build/rdma_600 --role local --rdma-ip 192.168.75.86 \
  --peer 192.168.75.87:19000 --app-cpu "$APP_CPU" --worker-cpu "$WORKER_CPU" \
  "${COMMON[@]}" --kind measure --rounds 1000 \
  > "sgl-local-w${W}-measure.log" 2>&1
```

两端正常结束后，采集两轮trace。remote：

```bash
./build/rdma_600 --role remote --rdma-ip 192.168.75.87 \
  --listen 192.168.75.87:19000 --app-cpu "$APP_CPU" --worker-cpu "$WORKER_CPU" \
  "${COMMON[@]}" --kind trace --trace-rounds 2 --max-inflight "$W" \
  > "sgl-remote-w${W}-trace.log" 2>&1
python3 compact_trace.py "sgl-remote-w${W}-trace.log" > "sgl-remote-w${W}-compact.json"
```

local：

```bash
./build/rdma_600 --role local --rdma-ip 192.168.75.86 \
  --peer 192.168.75.87:19000 --app-cpu "$APP_CPU" --worker-cpu "$WORKER_CPU" \
  "${COMMON[@]}" --kind trace --trace-rounds 2 \
  > "sgl-local-w${W}-trace.log" 2>&1
python3 compact_trace.py "sgl-local-w${W}-trace.log" > "sgl-local-w${W}-compact.json"
```

每组正常退出后再换W，不并行运行不同组。测量和trace的warmup都统一为100；trace耗时单独分析。

## 回传与判断

回传三组local measure结果行，以及每组两端compact JSON（共六份）。设备保留完整原始日志。

重点看：

1. 正式measure的local avg/p50/p95/p99是否降低。
2. trace中remote的`first_data_post_to_last_cqe_us`是否降低。
3. remote的`max_inflight_wr_observed`是否随窗口下降，同时WR数、CQE数、字节覆盖仍正确。
4. 结合`first_data_post_to_last_post_end_us`和`last_data_post_end_to_last_cqe_us`判断，
   避免把等待转移到提交阶段误判为加速。软件callback窗口与trace观察的硬件WR窗口不是完全同一口径。

若只是尾部缩短、总数据窗口和e2e不变或变差，就没有证明限流有收益。

## 离线测试

```bash
python3 -m unittest discover -s tests -p 'test_*window.py'
python3 -m unittest discover -s tests -p 'test_compact_trace.py'
```

窗口测试需要C++17编译器（默认g++，可设置CXX），编译实际参数解析和窗口等待函数，
在CPU夹具中检查默认关闭、窗口边界、跨轮计数、双rail独立、G32/W4推进、错误及超时。
离线测试不能代替Linux完整构建或双机RDMA性能验证。
