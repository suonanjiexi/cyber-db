# CyberDB

CyberDB 是一个兼容 MySQL 的云原生 HTAP（混合事务分析处理）数据库系统，基于 Go 语言实现，具有计算存储分离、一写多读、分布式存储等特性。

## 功能特点

- **MySQL 兼容**：兼容 MySQL 协议，可直接使用 MySQL 客户端连接
- **云原生架构**：支持容器化部署和编排
- **计算存储分离**：计算节点和存储节点独立可扩展
- **多副本复制**：基于 Raft 协议的强一致性数据复制
- **高可用集群**：支持多节点集群模式，自动故障转移
- **HTAP 支持**：同时支持事务处理和分析查询
- **混合存储引擎**：结合内存和磁盘存储，优化OLTP和OLAP性能
- **列式存储**：提供高效的列式存储引擎，优化OLAP性能
- **写入缓冲**：异步批量刷盘，提高写入性能
- **灵活配置**：支持不同场景的存储引擎配置
- **备份恢复**：支持数据备份和恢复功能
- **性能监控**：提供实时性能统计和诊断功能

## 系统架构

CyberDB 采用模块化设计，主要由以下组件组成：

1. **协议层**：实现 MySQL 协议，处理客户端连接和请求
2. **计算引擎**：负责 SQL 解析和执行计划生成
3. **存储引擎**：提供数据存储和事务支持
   - **内存引擎**：适用于OLTP场景，提供高性能的读写操作
   - **磁盘引擎**：基于Pebble，提供持久化存储，适用于大规模数据
   - **混合引擎**：结合内存和磁盘引擎优点，适用于HTAP场景
   - **列式存储引擎**：优化分析查询，提供高效的OLAP支持
4. **复制系统**：基于 Raft 协议的数据复制
5. **集群管理**：负责节点间通信和状态同步
6. **性能监控**：实时监控存储引擎性能指标，提供诊断能力

系统支持三种部署模式：
- **单节点模式**：适用于开发和测试环境
- **集群模式**：提供高可用性和水平扩展能力
- **HTAP模式**：同时支持OLTP和OLAP工作负载，混合存储引擎

## 开始使用

### 前置条件

- Go 1.21 或更高版本
- Git
- Make

### 安装

1. 克隆仓库：

```bash
git clone https://github.com/cyberdb/cyberdb.git
cd cyberdb
```

2. 构建项目：

```bash
make all
```

这将在 `bin` 目录下生成 `cyberdb`（服务器）和 `cyberdb-client`（客户端）可执行文件。

### 运行

1. 单节点模式：

```bash
make run-single
```

2. 集群模式：

在不同终端中运行：

```bash
# 终端 1 - 主节点
make run-master

# 终端 2 - 从节点
make run-slave
```

3. HTAP模式：

```bash
# 初始化HTAP环境
make init-htap

# 启动HTAP服务器
make run-htap
```

4. 客户端：

```bash
make run-client
```

或者直接通过任意 MySQL 客户端连接：

```bash
mysql -h 127.0.0.1 -P 3306 -u root
```

## 配置

配置文件位于 `configs` 目录下：

- `single-node.yaml`: 单节点配置
- `cluster-master.yaml`: 集群主节点配置
- `cluster-slave.yaml`: 集群从节点配置
- `htap.yaml`: HTAP模式配置

可以根据需要修改这些配置文件。

### HTAP模式配置

HTAP模式提供了一系列配置选项，可以根据工作负载特点进行调优：

```yaml
storage:
  # 存储引擎类型: memory, disk, hybrid
  type: "hybrid"
  # 数据目录
  data_dir: "data/htap"
  # 同步间隔（毫秒）
  sync_interval_ms: 1000
  # 写入缓冲区大小（MB）
  max_buffer_size_mb: 128
  # 是否自动刷盘
  auto_flush: true

htap:
  # 是否分离OLTP和OLAP处理
  separate_processing: true
  # 分析查询路由策略
  analytical_query_routing: "auto"
```

## 备份和恢复

CyberDB 提供了数据备份和恢复功能，支持通过命令行工具或API进行操作：

### 命令行工具

```bash
# 备份数据到指定目录
./bin/cyberdb-tools backup --output=/backup/cyberdb_backup --engine=hybrid --data-dir=data

# 从备份恢复数据
./bin/cyberdb-tools restore --input=/backup/cyberdb_backup_latest --engine=hybrid --data-dir=data --force
```

### API方式

```go
// 备份数据
err := storage.BackupEngine(engine, "/backup/path")

// 恢复数据
err := storage.RestoreEngine(engine, "/backup/path")
```

## 性能监控

CyberDB 提供了全面的性能监控能力，支持实时监控和性能诊断：

### 命令行监控工具

使用内置的监控命令查看实时性能指标：

```bash
# 实时监控数据库性能
./bin/cyberdb-tools monitor --engine=hybrid --interval=5

# 监控10分钟后自动退出
./bin/cyberdb-tools monitor --engine=hybrid --interval=5 --time=600
```

### 性能采集和分析API

通过代码使用性能监控API：

```go
// 创建性能监控器
monitor := storage.NewPerformanceMonitor(engine, 5*time.Second, 100)
monitor.Start()

// 记录操作性能
startTime := time.Now()
// ... 执行某些操作 ...
monitor.RecordRead(time.Since(startTime))

// 获取性能报告
report := monitor.GenerateReport()
fmt.Println(report)

// 获取最新的性能样本
sample := monitor.GetLatestSample()
fmt.Printf("读吞吐量: %.2f ops/s, 平均延迟: %d ns\n", 
    sample.Throughput.ReadsPerSec, sample.Latencies.AvgReadLatency)
```

### 性能顾问

系统提供性能顾问功能，可自动分析性能问题并提供优化建议：

```go
advisor := storage.NewPerformanceAdvisor()
suggestions := advisor.Analyze(monitor.GetLatestSample())
for _, suggestion := range suggestions {
    fmt.Println(suggestion)
}
```

## 列式存储

CyberDB 实现了高效的列式存储引擎，专为OLAP查询优化：

```go
// 创建列式存储表
schema := &storage.TableSchema{
    Columns: []storage.ColumnDef{
        {Name: "id", Type: storage.ColumnTypeInt},
        {Name: "name", Type: storage.ColumnTypeString},
        {Name: "value", Type: storage.ColumnTypeFloat},
    },
}
err := columnStore.CreateColumnTable(ctx, "analytics", "sales", schema)

// 插入数据
rowID, err := columnStore.InsertRow(ctx, "analytics", "sales", map[string]interface{}{
    "id": 1,
    "name": "product1",
    "value": 99.9,
})

// 高效的列扫描
values, err := columnStore.ScanColumn(ctx, "analytics", "sales", "value")
```

列式存储特别适合大规模数据分析场景，如：
- 数据仓库
- 实时分析
- 报表生成
- 聚合计算

## 示例

连接数据库并创建表：

```sql
-- 创建数据库
CREATE DATABASE test;

-- 使用数据库
USE test;

-- 创建表
CREATE TABLE users (
  id INT PRIMARY KEY,
  name VARCHAR(255),
  age INT
);

-- 插入数据
INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);
INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25);

-- 查询数据
SELECT * FROM users;
```

## HTAP场景示例

CyberDB适用于需要同时处理事务和分析的场景：

```sql
-- 事务处理（OLTP）
BEGIN;
INSERT INTO orders (id, customer_id, amount) VALUES (101, 5, 199.99);
UPDATE customers SET total_spent = total_spent + 199.99 WHERE id = 5;
COMMIT;

-- 同时运行分析查询（OLAP）
SELECT 
    category, 
    SUM(amount) as total_sales,
    AVG(amount) as avg_sale,
    COUNT(*) as order_count
FROM orders
JOIN products ON orders.product_id = products.id
GROUP BY category
ORDER BY total_sales DESC;
```

## 贡献

欢迎贡献代码或提出问题！请遵循以下步骤：

1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'Add amazing feature'`)
4. 推送分支 (`git push origin feature/amazing-feature`)
5. 打开 Pull Request

## 许可证

本项目采用 MIT 许可证 - 详情请参阅 [LICENSE](LICENSE) 文件。 