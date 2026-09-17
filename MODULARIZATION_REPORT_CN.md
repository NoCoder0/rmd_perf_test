# 模块拆分与检查记录

日期：2026-09-17。批量测试功能基线为 `7d6e72acde846d906809c2c32f2a620f7b0d2061`；本阶段只拆分源文件、更新构建和文档，不改变协议、CLI、计时边界或调度规则。

## 源码职责

| 文件 | 职责 |
|---|---|
| `rdma_600.cpp` | 18行程序入口、异常退出 |
| `src/config.*` | CLI、环境变量、能力检查、配置名称 |
| `src/common.h` | 配置、rail状态、buffer、计数器和trace数据结构 |
| `src/benchmark.h`、`src/benchmark.cpp` | 基准对象、矩阵协商、case生命周期、失败记录 |
| `src/local.cpp` | 请求发起、通知处理、等待/scatter调度、结果校验 |
| `src/remote.cpp` | 请求重组、remote轮次、WRITE/PutV提交、源内容更新 |
| `src/control.cpp` | 消息分派、握手/边界协议、Send及callback |
| `src/transport.cpp` | 固定线程、连接、MR、drain和资源释放 |
| `src/protocol.*` | wire编解码、分片、消息元数据校验 |
| `src/data_path.*` | 映射、地址校验、scatter原语、调度模板 |
| `src/results.cpp` | trace、JSON、汇总表 |
| `src/support.*` | 时钟、CPU绑定、统计辅助函数 |

CMake显式构建入口和10个独立cpp。跨文件使用命名空间 `rdma_bench`；模板和原有短小热路径辅助函数保留在头文件。对象成员顺序保持一致。没有加入生产self-test宏、入口或Python脚本，没有修改共享ubs-comm。

## 本地验证

- 对照基线逐个核对148个函数/方法体，忽略空白后全部相同；单独检查benchmark成员定义及顺序相同。记录：`build/check_split_bodies.cjs`、`build/split_body_manifest.json`（均为忽略的本地检查产物）。这是纯拆分阶段的结果，后续优化另行记录。
- MinGW GCC 15.2、C++17、真实ubs-comm公共头文件、现有Windows兼容shim：所有独立编译单元语法检查通过。
- 实际独立cpp链接运行软件回归：38,004个布局，360个case、1,080轮及原有错误路径检查全部通过。外部测试编译使用 `-fno-access-control` 访问私有测试点，生产头文件和构建没有该选项。日志：`build/split_validation.log`。
- 实际main与独立cpp链接后的 `--help`、非法 `--blocks 99` 拒绝检查通过。测试中HCOM服务工厂替身被调用就抛异常，因此不会把模拟程序误当RDMA程序运行；只链接真实库的logger实现以满足静态符号。未链接部署用HCOM静态库。
- `build/batch_baseline_before_split.cpp` 的SHA256为 `B1B4B4EBE70AC39313FE3329940D603D300708AB919B154A4296A219AA5F5CBA`，是批量功能基线的历史单文件快照；BATCH_REPORT中的源文件hash也指该阶段。

## 验证边界

当前没有目标Linux静态库、RDMA设备或可用目标机连接，尚未完成目标Linux构建及硬件测试。软件回归通过不证明真实QP顺序、DMA可见性、双NIC流量或性能。

拆分会改变跨编译单元内联机会，函数体相同不保证机器码或性能相同。硬件比较应分别保留单文件基线、纯拆分版本和后续优化版本，固定编译器、优化参数及LTO设置。未自行开启LTO。GCC关于 `hardware_destructive_interference_size` 的提示源于该类型进入公共头；同一目标的各编译单元必须使用一致架构参数，本地测试仅抑制该提示，不改变生产编译选项。
