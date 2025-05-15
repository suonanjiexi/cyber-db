package config

import (
	"fmt"
	"io/ioutil"
	"sync"

	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

// Config 表示应用程序的配置
type Config struct {
	Server      ServerConfig      `yaml:"server"`
	Storage     StorageConfig     `yaml:"storage"`
	Compute     ComputeConfig     `yaml:"compute"`
	HTAP        HTAPConfig        `yaml:"htap"`
	Cluster     ClusterConfig     `yaml:"cluster"`
	Replication ReplicationConfig `yaml:"replication"`
	Log         LogConfig         `yaml:"log"`
	Cache       CacheConfig       `yaml:"cache"`
	Performance PerformanceConfig `yaml:"performance"`
	Security    SecurityConfig    `yaml:"security"`
	Monitoring  MonitoringConfig  `yaml:"monitoring"`
}

// ServerConfig 定义服务器配置
type ServerConfig struct {
	ID                string `yaml:"id"`
	Host              string `yaml:"host"`
	Port              int    `yaml:"port"`
	MaxConnections    int    `yaml:"max_connections"`
	ConnectionTimeout int    `yaml:"connection_timeout_ms"`
	GracefulShutdown  int    `yaml:"graceful_shutdown_seconds"`
	EnableHTTP        bool   `yaml:"enable_http"`
	HTTPPort          int    `yaml:"http_port"`
	EnableGRPC        bool   `yaml:"enable_grpc"`
	GRPCPort          int    `yaml:"grpc_port"`
}

// StorageConfig 定义存储引擎配置
type StorageConfig struct {
	// 存储引擎类型: memory, disk, hybrid
	Type string `yaml:"type"`

	// 数据目录
	DataDir string `yaml:"data_dir"`

	// WAL目录，如与DataDir不同
	WALDir string `yaml:"wal_dir"`

	// 是否启用写前日志
	WALEnabled bool `yaml:"wal_enabled"`

	// WAL刷盘模式: sync, async, none
	WALSyncMode string `yaml:"wal_sync_mode"`

	// 异步刷盘间隔（毫秒）
	SyncIntervalMs int `yaml:"sync_interval_ms"`

	// 最大写入缓冲区大小（MB）
	MaxBufferSizeMB int `yaml:"max_buffer_size_mb"`

	// 是否启用自动刷盘
	AutoFlush bool `yaml:"auto_flush"`

	// 缓存大小（MB）
	CacheSizeMB int `yaml:"cache_size_mb"`

	// LSM树配置（用于基于LSM的存储引擎）
	LSM LSMConfig `yaml:"lsm"`

	// 压缩配置
	Compression CompressionConfig `yaml:"compression"`

	// 是否启用Bloom过滤器
	EnableBloomFilter bool `yaml:"enable_bloom_filter"`

	// Bloom过滤器的误判率
	BloomFilterErrorRate float64 `yaml:"bloom_filter_error_rate"`

	// 块大小（KB）
	BlockSizeKB int `yaml:"block_size_kb"`

	// 自动压缩触发间隔（秒）
	CompactionIntervalSec int `yaml:"compaction_interval_sec"`

	// 是否启用统计信息收集
	EnableStats bool `yaml:"enable_stats"`

	// 统计信息收集间隔（秒）
	StatsCollectionIntervalSec int `yaml:"stats_collection_interval_sec"`
}

// LSMConfig 定义LSM树配置
type LSMConfig struct {
	// LSM树层数
	Levels int `yaml:"levels"`

	// 每层大小乘数
	SizeRatio int `yaml:"size_ratio"`

	// 每层使用的压缩算法
	LevelCompression []string `yaml:"level_compression"`

	// 内存表最大大小（MB）
	MemtableSizeMB int `yaml:"memtable_size_mb"`

	// 最大内存表数量
	MaxMemtables int `yaml:"max_memtables"`

	// 基层（L0）的最大文件数量
	L0FileNumCompactionTrigger int `yaml:"l0_file_num_compaction_trigger"`

	// 是否启用并行压缩
	EnableParallelCompaction bool `yaml:"enable_parallel_compaction"`

	// 并行压缩的线程数
	CompactionThreads int `yaml:"compaction_threads"`
}

// CompressionConfig 定义压缩配置
type CompressionConfig struct {
	// 压缩算法: none, snappy, zstd, lz4
	Algorithm string `yaml:"algorithm"`

	// 压缩级别（对于支持级别的算法）
	Level int `yaml:"level"`

	// 最小压缩比例，低于此值不压缩
	MinRatio float64 `yaml:"min_ratio"`
}

// ComputeConfig 定义计算引擎配置
type ComputeConfig struct {
	// 查询优化级别（0-3）
	OptimizationLevel int `yaml:"optimization_level"`

	// 最大并行度
	MaxParallelism int `yaml:"max_parallelism"`

	// 查询超时（秒）
	QueryTimeoutSec int `yaml:"query_timeout_sec"`

	// 是否启用查询缓存
	EnableQueryCache bool `yaml:"enable_query_cache"`

	// 查询缓存大小（MB）
	QueryCacheSizeMB int `yaml:"query_cache_size_mb"`

	// 是否启用预编译语句缓存
	EnablePreparedStmtCache bool `yaml:"enable_prepared_stmt_cache"`

	// 预编译语句缓存大小（条数）
	PreparedStmtCacheSize int `yaml:"prepared_stmt_cache_size"`

	// 最大可扫描行数
	MaxScanRows int64 `yaml:"max_scan_rows"`

	// 单次查询最大内存使用（MB）
	MaxQueryMemoryMB int `yaml:"max_query_memory_mb"`

	// 是否启用统计信息驱动的查询优化
	EnableStatsBasedOptimization bool `yaml:"enable_stats_based_optimization"`
}

// HTAPConfig 定义HTAP相关配置
type HTAPConfig struct {
	// 是否分离OLTP和OLAP请求
	SeparateProcessing bool `yaml:"separate_processing"`

	// 分析型查询路由策略（memory, disk, auto）
	AnalyticalQueryRouting string `yaml:"analytical_query_routing"`

	// 是否启用查询缓存
	QueryCacheEnabled bool `yaml:"query_cache_enabled"`

	// 查询缓存大小（MB）
	QueryCacheSizeMB int `yaml:"query_cache_size_mb"`

	// 分析型查询最大内存使用（MB）
	AnalyticalMaxMemoryMB int `yaml:"analytical_max_memory_mb"`

	// 是否使用列式存储进行分析
	UseColumnStore bool `yaml:"use_column_store"`

	// 列存储的压缩算法
	ColumnStoreCompression string `yaml:"column_store_compression"`

	// 是否启用向量化执行
	EnableVectorizedExecution bool `yaml:"enable_vectorized_execution"`

	// 向量大小（行数）
	VectorSize int `yaml:"vector_size"`
}

// ClusterConfig 定义集群配置
type ClusterConfig struct {
	// 是否启用集群
	Enabled bool `yaml:"enabled"`

	// 集群角色: leader, follower, learner
	Role string `yaml:"role"`

	// 集群成员配置
	Members []MemberConfig `yaml:"members"`

	// 节点发现方式: static, dns, k8s
	Discovery string `yaml:"discovery"`

	// 自动发现参数
	DiscoveryConfig map[string]string `yaml:"discovery_config"`

	// 是否启用故障检测
	EnableFailureDetection bool `yaml:"enable_failure_detection"`

	// 故障检测超时（毫秒）
	FailureDetectionTimeoutMs int `yaml:"failure_detection_timeout_ms"`

	// 是否启用自动故障转移
	EnableAutoFailover bool `yaml:"enable_auto_failover"`

	// Gossip协议端口
	GossipPort int `yaml:"gossip_port"`

	// 集群心跳间隔（毫秒）
	HeartbeatIntervalMs int `yaml:"heartbeat_interval_ms"`
}

// MemberConfig 定义集群成员配置
type MemberConfig struct {
	ID       string `yaml:"id"`
	Address  string `yaml:"address"`
	Role     string `yaml:"role"`
	IsVoter  bool   `yaml:"is_voter"`
	Priority int    `yaml:"priority"`
	Zone     string `yaml:"zone"`
}

// ReplicationConfig 定义复制配置
type ReplicationConfig struct {
	// 是否启用复制
	Enabled bool `yaml:"enabled"`

	// 复制模式: sync, semi-sync, async
	Mode string `yaml:"mode"`

	// 复制端口
	Port int `yaml:"port"`

	// 同步复制超时（毫秒）
	SyncTimeoutMs int `yaml:"sync_timeout_ms"`

	// 最大并行复制线程数
	MaxWorkers int `yaml:"max_workers"`

	// 复制延迟阈值（毫秒）
	MaxLagMs int `yaml:"max_lag_ms"`

	// 是否启用带宽限制
	EnableBandwidthLimit bool `yaml:"enable_bandwidth_limit"`

	// 最大带宽使用（KB/s）
	MaxBandwidthKBps int `yaml:"max_bandwidth_kbps"`

	// Raft配置
	Raft RaftConfig `yaml:"raft"`
}

// RaftConfig 定义Raft复制算法配置
type RaftConfig struct {
	// 心跳间隔（毫秒）
	HeartbeatIntervalMs int `yaml:"heartbeat_interval_ms"`

	// 选举超时（毫秒）
	ElectionTimeoutMs int `yaml:"election_timeout_ms"`

	// 快照间隔（操作次数）
	SnapshotIntervalCount int `yaml:"snapshot_interval_count"`

	// 快照阈值（操作次数）
	SnapshotThresholdCount int `yaml:"snapshot_threshold_count"`

	// 最大批处理大小
	MaxBatchSize int `yaml:"max_batch_size"`

	// 最大未提交日志数
	MaxUncommittedLogs int `yaml:"max_uncommitted_logs"`
}

// LogConfig 定义日志配置
type LogConfig struct {
	// 日志级别: debug, info, warn, error
	Level string `yaml:"level"`

	// 日志输出方式: console, file, both
	Output string `yaml:"output"`

	// 日志文件路径
	FilePath string `yaml:"file_path"`

	// 是否启用结构化日志
	EnableStructuredLogging bool `yaml:"enable_structured_logging"`

	// 是否启用慢查询日志
	EnableSlowQueryLog bool `yaml:"enable_slow_query_log"`

	// 慢查询阈值（毫秒）
	SlowQueryThresholdMs int `yaml:"slow_query_threshold_ms"`

	// 是否启用SQL审计日志
	EnableSQLAuditLog bool `yaml:"enable_sql_audit_log"`

	// 日志轮转策略: size, daily, none
	LogRotation string `yaml:"log_rotation"`

	// 日志轮转大小（MB）
	LogRotationSizeMB int `yaml:"log_rotation_size_mb"`

	// 保留的最大日志文件数量
	MaxLogFiles int `yaml:"max_log_files"`
}

// CacheConfig 定义缓存配置
type CacheConfig struct {
	// 行缓存大小（MB）
	RowCacheSizeMB int `yaml:"row_cache_size_mb"`

	// 元数据缓存大小（MB）
	MetadataCacheSizeMB int `yaml:"metadata_cache_size_mb"`

	// 索引缓存大小（MB）
	IndexCacheSizeMB int `yaml:"index_cache_size_mb"`

	// 缓存淘汰策略: lru, lfu, clock
	EvictionPolicy string `yaml:"eviction_policy"`

	// 是否启用自适应缓存分配
	EnableAdaptiveCache bool `yaml:"enable_adaptive_cache"`

	// 缓存预热策略: none, recent, full
	WarmupStrategy string `yaml:"warmup_strategy"`

	// 是否启用查询结果缓存
	EnableResultCache bool `yaml:"enable_result_cache"`

	// 查询结果缓存大小（MB）
	ResultCacheSizeMB int `yaml:"result_cache_size_mb"`

	// 查询结果缓存过期时间（秒）
	ResultCacheExpirationSec int `yaml:"result_cache_expiration_sec"`
}

// PerformanceConfig 定义性能配置
type PerformanceConfig struct {
	// 读优化策略: throughput, latency, balanced
	ReadOptimization string `yaml:"read_optimization"`

	// 写优化策略: throughput, durability, balanced
	WriteOptimization string `yaml:"write_optimization"`

	// 预写日志（WAL）设置
	WAL WALConfig `yaml:"wal"`

	// 后台任务
	BackgroundTasks BackgroundTasksConfig `yaml:"background_tasks"`

	// IO线程池大小
	IOThreads int `yaml:"io_threads"`

	// 计算线程池大小
	ComputeThreads int `yaml:"compute_threads"`

	// 网络线程池大小
	NetworkThreads int `yaml:"network_threads"`

	// 是否使用直接IO
	UseDirectIO bool `yaml:"use_direct_io"`

	// 是否启用零拷贝优化
	EnableZeroCopy bool `yaml:"enable_zero_copy"`

	// 是否启用NUMA感知内存分配
	EnableNUMAAware bool `yaml:"enable_numa_aware"`

	// 最大写入批处理大小
	MaxWriteBatchSize int `yaml:"max_write_batch_size"`

	// 最大读取批处理大小
	MaxReadBatchSize int `yaml:"max_read_batch_size"`
}

// WALConfig 定义WAL配置
type WALConfig struct {
	// 是否启用WAL
	Enabled bool `yaml:"enabled"`

	// WAL同步模式: sync, async, none
	SyncMode string `yaml:"sync_mode"`

	// WAL刷盘间隔（毫秒）
	SyncIntervalMs int `yaml:"sync_interval_ms"`

	// WAL目录
	Directory string `yaml:"directory"`

	// 是否使用独立的WAL磁盘
	UseSeparateDisk bool `yaml:"use_separate_disk"`

	// WAL文件大小（MB）
	FileSizeMB int `yaml:"file_size_mb"`

	// WAL保留时间（小时）
	RetentionHours int `yaml:"retention_hours"`

	// 是否启用组提交
	EnableGroupCommit bool `yaml:"enable_group_commit"`

	// 组提交最大延迟（毫秒）
	GroupCommitDelayMs int `yaml:"group_commit_delay_ms"`
}

// BackgroundTasksConfig 定义后台任务配置
type BackgroundTasksConfig struct {
	// 表压缩任务
	Compaction struct {
		// 是否启用自动压缩
		Enabled bool `yaml:"enabled"`

		// 压缩间隔（秒）
		IntervalSec int `yaml:"interval_sec"`

		// 压缩优先级: size, age, mixed
		Priority string `yaml:"priority"`

		// 压缩并行度
		Parallelism int `yaml:"parallelism"`
	} `yaml:"compaction"`

	// 统计信息收集
	StatsCollection struct {
		// 是否启用统计信息收集
		Enabled bool `yaml:"enabled"`

		// 收集间隔（秒）
		IntervalSec int `yaml:"interval_sec"`

		// 统计信息采样率
		SamplingRate float64 `yaml:"sampling_rate"`
	} `yaml:"stats_collection"`

	// 自动回收
	GC struct {
		// 是否启用自动回收
		Enabled bool `yaml:"enabled"`

		// 回收间隔（秒）
		IntervalSec int `yaml:"interval_sec"`

		// 回收阈值
		Threshold float64 `yaml:"threshold"`
	} `yaml:"gc"`

	// 自动检查点
	Checkpoint struct {
		// 是否启用自动检查点
		Enabled bool `yaml:"enabled"`

		// 检查点间隔（秒）
		IntervalSec int `yaml:"interval_sec"`

		// 检查点最小大小（MB）
		MinSizeMB int `yaml:"min_size_mb"`
	} `yaml:"checkpoint"`
}

// SecurityConfig 定义安全配置
type SecurityConfig struct {
	// 身份验证方法
	AuthMethod string `yaml:"auth_method"`

	// 是否启用TLS
	TLSEnabled bool `yaml:"tls_enabled"`

	// TLS证书路径
	TLSCertPath string `yaml:"tls_cert_path"`

	// TLS密钥路径
	TLSKeyPath string `yaml:"tls_key_path"`

	// 是否需要客户端证书
	RequireClientCert bool `yaml:"require_client_cert"`

	// 是否启用数据加密
	EnableDataEncryption bool `yaml:"enable_data_encryption"`

	// 加密算法
	EncryptionAlgorithm string `yaml:"encryption_algorithm"`

	// 密钥轮换间隔（天）
	KeyRotationDays int `yaml:"key_rotation_days"`

	// 是否启用审计日志
	EnableAuditLog bool `yaml:"enable_audit_log"`

	// 权限模型: rbac, acl
	PermissionModel string `yaml:"permission_model"`
}

// MonitoringConfig 定义监控配置
type MonitoringConfig struct {
	// 是否启用指标收集
	EnableMetrics bool `yaml:"enable_metrics"`

	// 指标收集间隔（秒）
	MetricsIntervalSec int `yaml:"metrics_interval_sec"`

	// 是否启用Prometheus端点
	EnablePrometheus bool `yaml:"enable_prometheus"`

	// Prometheus端点端口
	PrometheusPort int `yaml:"prometheus_port"`

	// 是否启用健康检查
	EnableHealthCheck bool `yaml:"enable_health_check"`

	// 健康检查端口
	HealthCheckPort int `yaml:"health_check_port"`

	// 是否启用分布式追踪
	EnableTracing bool `yaml:"enable_tracing"`

	// 追踪采样率
	TracingSamplingRate float64 `yaml:"tracing_sampling_rate"`

	// 追踪导出器类型: jaeger, zipkin, otlp
	TracingExporter string `yaml:"tracing_exporter"`

	// 追踪导出器配置
	TracingExporterConfig map[string]string `yaml:"tracing_exporter_config"`

	// 是否启用性能分析
	EnableProfiling bool `yaml:"enable_profiling"`

	// 性能分析端口
	ProfilingPort int `yaml:"profiling_port"`
}

// LoadConfig 从指定路径加载配置文件
func LoadConfig(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}

	config := &Config{}
	if err := v.Unmarshal(config); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w", err)
	}

	return config, nil
}

// LoadConfigFromYAML 从YAML字符串加载配置
func LoadConfigFromYAML(yamlStr string) (*Config, error) {
	config := &Config{}
	err := yaml.Unmarshal([]byte(yamlStr), config)
	if err != nil {
		return nil, fmt.Errorf("解析YAML失败: %w", err)
	}
	return config, nil
}

// SaveConfig 将配置保存到指定路径
func SaveConfig(config *Config, path string) error {
	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("序列化配置失败: %w", err)
	}

	if err := ioutil.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("写入配置文件失败: %w", err)
	}

	return nil
}

// NewDefaultConfig 创建默认配置
func NewDefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Host:              "0.0.0.0",
			Port:              3306,
			MaxConnections:    1000,
			ConnectionTimeout: 30000,
			GracefulShutdown:  30,
		},
		Storage: StorageConfig{
			Type:                  "hybrid",
			DataDir:               "data",
			WALEnabled:            true,
			WALSyncMode:           "async",
			SyncIntervalMs:        1000,
			MaxBufferSizeMB:       128,
			AutoFlush:             true,
			CacheSizeMB:           512,
			EnableBloomFilter:     true,
			BloomFilterErrorRate:  0.01,
			BlockSizeKB:           32,
			CompactionIntervalSec: 3600,
			EnableStats:           true,
			LSM: LSMConfig{
				Levels:         6,
				SizeRatio:      10,
				MemtableSizeMB: 64,
				MaxMemtables:   2,
			},
			Compression: CompressionConfig{
				Algorithm: "snappy",
				Level:     0,
				MinRatio:  0.8,
			},
		},
		Compute: ComputeConfig{
			OptimizationLevel: 2,
			MaxParallelism:    8,
			QueryTimeoutSec:   300,
			EnableQueryCache:  true,
			QueryCacheSizeMB:  256,
			MaxQueryMemoryMB:  1024,
		},
		HTAP: HTAPConfig{
			SeparateProcessing:     true,
			AnalyticalQueryRouting: "auto",
			QueryCacheEnabled:      true,
			QueryCacheSizeMB:       256,
			AnalyticalMaxMemoryMB:  1024,
		},
		Cache: CacheConfig{
			RowCacheSizeMB:      512,
			MetadataCacheSizeMB: 64,
			IndexCacheSizeMB:    256,
			EvictionPolicy:      "lru",
		},
		Performance: PerformanceConfig{
			ReadOptimization:  "balanced",
			WriteOptimization: "balanced",
			IOThreads:         8,
			ComputeThreads:    8,
			NetworkThreads:    4,
			WAL: WALConfig{
				Enabled:        true,
				SyncMode:       "async",
				SyncIntervalMs: 100,
			},
		},
		Security: SecurityConfig{
			AuthMethod: "mysql_native",
			TLSEnabled: false,
		},
		Log: LogConfig{
			Level:  "info",
			Output: "console",
		},
		Monitoring: MonitoringConfig{
			EnableMetrics:      true,
			MetricsIntervalSec: 10,
		},
	}
}

// 全局配置实例
var currentConfig *Config
var configMutex sync.RWMutex

// SetCurrentConfig 设置当前配置
func SetCurrentConfig(cfg *Config) {
	configMutex.Lock()
	defer configMutex.Unlock()
	currentConfig = cfg
}

// GetCurrentConfig 获取当前配置
func GetCurrentConfig() (*Config, error) {
	configMutex.RLock()
	defer configMutex.RUnlock()

	if currentConfig == nil {
		return nil, fmt.Errorf("配置未初始化")
	}

	return currentConfig, nil
}
