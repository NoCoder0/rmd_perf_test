# 批量 sparse_copy（协议 v6）

本设计取代阶段3文档中固定600×1024、v5及1000/10000轮的限制。工作目录与分支保持 `perf_test_duo_card_sgl` / `duo_card_sgl`，不修改共享依赖或 main 参考。

- 默认矩阵：块数100到9600，步长100；先遍历1024B，再遍历656B。支持显式块数列表、起止/步长和单case。mode、links、K、pipeline保持一次运行的配置，不做额外组合扫描。每case默认20 verify、100 warmup、1000 measure。
- 建链、最大容量MR、stage和请求存储一次分配。两端在数据轮开始前逐项精确核对整个矩阵；每case开始握手与所有rail结束屏障位于计时外。generation贯穿全矩阵递增。case切换先等待全部rail通知、scatter、发送/数据callback及handler退出，再更换参数和重置样本；pending/active分离。
- COPY_REQ仍编码每个真实源/目标偏移。逻辑长度64+16N，最大153664B；以不超过16032B的应用层分片在rail0传输，保留16384B服务消息容量。每片含32B头（版本、generation、总长度、offset、片长及保留字段），严格顺序重组，拒绝丢片、重复、越界、错代与跨case。编码、分片构造、Send、重组、解析均在完整 sparse_copy 延迟中。
- 分rail采用连续分区，rail0最多ceil(N/L)块，最后rail持余数，支持奇数块数。目标映射使用反向循环置换，避免原乘11映射对新块数出现重复目标；源可重复。656B是真实WRITE/scatter长度与有效吞吐分子。
- sparse_copy从请求准备前开始，到所有rail数据就绪、所有scatter及全部请求片的本地Send callback完成为止。逐代源头尾标记更新包含在请求往返内；每轮目标头尾标记校验在延迟样本外、wall内，verify轮及case结尾做完整内容/gap校验。避免固定内容掩盖整块旧代/漏写；这不替代硬件DMA顺序证明。
- 每case独立统计，最终安全退出后打印逐case JSON及表格。平均/分位为单次纳秒样本转微秒；有效GB/s采用有效字节/平均单次纳秒（十进制），另列包含逐轮标记校验的wall吞吐。失败中止矩阵、非零退出，未完成case不输出成功性能。

依赖实查仍为 `ubs-comm@e709a37`，公共/内部上限16；PutV使用driver0，单channel linkCount=1。K可配置1..30，但超过真实公共头或声明QP cap则拒绝，当前K30 UNSUPPORTED。环境cap仍只是外部声明。多service固定线程约束、WRITE与CHUNK_DONE同真实QP、DMA可见性、双NIC路由与流量、TLS pool退出仍需目标机证据。此次未获得目标机连接配置，不以本地验证冒充硬件性能。

接收handler与case参数切换共享互斥锁；其实际逐消息成本计入往返，JSON记录receive_handler_case_lock=true。v6的逐轮标记更新和分片头也改变了测量口径，不与旧v5数据直接混表。
