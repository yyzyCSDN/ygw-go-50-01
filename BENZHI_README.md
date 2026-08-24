# CDCPipeline

CDCPipeline 是一个变更数据捕获与同步管道（Change Data Capture pipeline）：
从源库变更日志按位点读取数据，解析行变更并拆分大事务，按表映射与过滤规则
处理事件，经投递层去重、重试并保持事务顺序后写入目标端。同步阶段状态机
（全量 -> 增量）与投递状态机（in-flight -> acked -> retrying）保证重启后从
已确认位点继续，不丢变更、不重复消费。

## 功能

- 位点管理：只前进不回退的已确认位点、读取水位与周期 checkpoint。
- 变更解析：按事件所属 schema 版本解码，避免 DDL 后旧事件字段错位。
- 表映射与过滤：schema 版本快照与过滤规则版本隔离，在途事件不被半更新规则影响。
- 投递层：按表外键依赖顺序投递、事务内顺序保持、幂等去重与指数退避重试。
- 目标写入：幂等应用、外键约束校验与写超时背压传导。
- 全量/增量切换：快照起始水位绑定，切换窗口不丢不重。
- 同步监控页面：web/monitor.html 实时展示位点、积压、表行数与运行统计。

## 构建

项目依赖 github.com/cespare/xxhash/v2，依赖已 vendor，可完全离线构建：

```bash
go build -mod=vendor ./...
go test -mod=vendor ./...
go vet -mod=vendor ./...
```

## 启动

```bash
go run -mod=vendor ./cmd/cdcpipeline
```

默认监听 8123 端口。环境变量：CDC_ADDR、CDC_BATCH_SIZE、CDC_MAX_TXN_SIZE、
CDC_CHECKPOINT_MS、CDC_POLL_MS、CDC_SINK_DELAY_MS、CDC_SINK_TIMEOUT_MS、
CDC_TABLES。

HTTP 探测：

```bash
curl http://localhost:8123/healthz
curl http://localhost:8123/api/status
curl http://localhost:8123/monitor
```

## Docker

构建 linux/amd64 或 linux/arm64 镜像（离线 vendor 构建，构建阶段仅编译不跑测试）：

```bash
./build_benzhi_docker.sh cdcpipeline linux/amd64
./build_benzhi_docker.sh cdcpipeline linux/arm64
```

容器内执行完整校验：

```bash
docker run --rm cdcpipeline bash -lc 'go build -mod=vendor ./... && go vet -mod=vendor ./... && go test -mod=vendor ./...'
```
