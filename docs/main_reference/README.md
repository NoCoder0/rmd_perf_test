# RDMA 600 × 1 KiB — sparse_copy 阶段 1

当前工作区已将单卡 direct baseline 重构为 `sparse-copy-v3`。实现仍需在目标 Linux/AArch64 RDMA 主机完成真实构建、self-test、双端 verify 和 measure；本地检查不能替代硬件结论。

完整协议见 [DESIGN_CN.md](DESIGN_CN.md)，实施与验收见 [IMPLEMENTATION_PLAN_CN.md](IMPLEMENTATION_PLAN_CN.md)，当前状态见 [PROGRESS.md](PROGRESS.md)。[STAGE1_5_REPORT_CN.md](STAGE1_5_REPORT_CN.md) 只保留旧 sender/receiver + ROUND_ACK 协议的历史证据。

## 阶段 1 行为

固定角色：

- `local`：`sparse_copy` 调用者、最终 destination 拥有者和主计时端；主动连接 remote。
- `remote`：source 拥有者和 RDMA WRITE 发起端；监听连接。

一次 SC-B1 调用：

1. local 从调用入口开始计时，校验并编码 600 个源/目标偏移对。
2. local 发送一条 9664 字节 `COPY_REQ`：64 字节头加 `600×16` 字节正文。
3. remote 将接收内容复制到独立 pending 槽，应用线程解析并按本次请求重建 600 个 `Put(1024)`。
4. remote 在同一真实 QP 的 600 次 WRITE 后发送一个 `DATA_DONE`。
5. local 等待 `DATA_DONE` 和自己的 COPY_REQ Send callback 后返回并停止计时。

成功路径没有逐轮 ACK。下一次 COPY_REQ 表示 local 已消费上一代并授予下一代写入权限；remote 仍须等待自己的 Put/Send callbacks 后复用描述符和通知缓冲。失败路径使用 `COPY_ERROR`，发送路径失效时依靠断链唤醒。

local 在 setup 中把 destination 基址、大小、区域 ID 和完整 `UBSHcomMemoryKey` 发送给 remote。remote source key 始终留在 remote 本地，local 只发送相对 source 区域的偏移。

阶段 1 保持：单 service、单真实 QP、`linkCount=1`、关闭内建 multirail、每端一个应用线程和一个 busy-poll worker；没有 staging、scatter、SGL 或 IMM。Send/Recv 应用消息容量为 16384 字节。

## 构建

在目标 Linux 主机上使用匹配的 ubs-comm 产物：

```bash
bash ./build.sh --ubs-root /absolute/path/to/ubs-comm
```

也可以分别指定公共头、静态库和 boundscheck 根目录，详见：

```bash
bash ./build.sh --help
```

构建系统要求真实 `hcom_service.h`、`hcom_service_context.h`、`libhcom_static.a` 和 boundscheck。不要用本机兼容头生成部署二进制。

## 本地协议自测

```bash
./build/rdma_600 --self-test
```

自测覆盖 version=3 的 HELLO/READY、9664 字节 COPY_REQ、DATA_DONE/COPY_ERROR、截断请求、重复目标、非顺序 600 项映射和完整数据/gap 校验。它不验证建链、MR、DMA、CQ/RQ 或 QP 顺序。

## 直接运行双端

先在 remote 主机启动监听：

```bash
./build/rdma_600 --role remote \
  --rdma-ip <remote_nic0_rdma_ip> \
  --listen <remote_oob_ip>:19000 \
  --kind verify --verify-rounds 20 --warmup 0 --rounds 0 \
  --timeout-sec 10 --app-cpu <remote_app_cpu> --worker-cpu <remote_worker_cpu> \
  --links 1 --mode direct
```

看到 `LISTENING role=remote` 后，在 local 主机启动：

```bash
./build/rdma_600 --role local \
  --rdma-ip <local_nic0_rdma_ip> \
  --peer <remote_oob_ip>:19000 \
  --kind verify --verify-rounds 20 --warmup 0 --rounds 0 \
  --timeout-sec 10 --app-cpu <local_app_cpu> --worker-cpu <local_worker_cpu> \
  --links 1 --mode direct
```

正式 measure 将两端同时改为：

```text
--kind measure --verify-rounds 20 --warmup 1000 --rounds 10000
```

每端的 app/worker CPU 必须不同。两台机器使用完全一致的轮数、超时和协议版本。

## 使用 run.py 归档

从 [hosts.example.json](hosts.example.json) 复制 `hosts.json`，分别填写 `local` 和 `remote` 的二进制、动态库、RDMA IP、CPU，以及 remote 的 OOB 地址和端口。两端保存相同配置。

remote 主机：

```bash
python3 run.py --config hosts.json --role remote \
  --suite stage1 --case SC-B1 --kind verify \
  --output results/<run-id>-remote
```

local 主机：

```bash
python3 run.py --config hosts.json --role local \
  --suite stage1 --case SC-B1 --kind verify \
  --output results/<run-id>-local
```

measure 使用新的输出目录并把两端 `--kind` 改为 `measure`。脚本只启动当前主机的一个角色，不 SSH、不部署、不构建，也不会清理其它同名进程。

local 目录包含 `result.jsonl` 和 `REPORT.md`；remote 目录包含 `status.json`。manifest 记录实际命令、本机二进制 SHA-256、动态依赖和退出状态。

## 结果口径

local 输出 schema 3：

- 主时延：`sparse_copy_avg_us`、`p50`、`p95`、`p99`。
- 有效载荷：614400 字节/调用；请求：9664 字节/调用。
- 应用预期 WR：1 COPY_REQ、600 data WRITE、1 DATA_DONE、0 success ACK。
- 吞吐：`effective_GBps`、`block_Mops`、`request_GBps` 和 `measured_wall_seconds`。

verify 的性能字段全部为 `null`。measure 在最后一次调用后完整校验 destination；verify 每轮改变源/目标排列并完整校验。不同主机时钟不相减，remote 的提交时长不是主结果。

## 当前限制

- 尚未完成目标 Linux/AArch64 构建和真实 RDMA 验证，所有性能结论为 pending。
- 阶段 2 双 NIC、阶段 3 SGL/staging/scatter、阶段 4 IMM 尚未实现。
- SGL=30 的未来 grouped-source 请求只有在调用方确实提供可展开的 20 个源组描述符时才可使用 20 source + 600 destination；任意 600 个分散源地址仍必须完整发送。
- 当前 callback 仍按请求分配；没有引入池或修改 ubs-comm。
