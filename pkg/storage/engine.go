package storage

import (
	"context"
	"errors"
	"io"
)

var (
	// ErrDBNotFound 数据库不存在
	ErrDBNotFound = errors.New("数据库不存在")
	// ErrDBExists 数据库已存在
	ErrDBExists = errors.New("数据库已存在")
	// ErrTableNotFound 表不存在
	ErrTableNotFound = errors.New("表不存在")
	// ErrTableExists 表已存在
	ErrTableExists = errors.New("表已存在")
	// ErrKeyNotFound 键不存在
	ErrKeyNotFound = errors.New("键不存在")
	// ErrTransactionFailed 事务失败时返回的错误
	ErrTransactionFailed = errors.New("事务执行失败")
	// ErrCompactionFailed 压缩失败
	ErrCompactionFailed = errors.New("压缩失败")
	// ErrSnapshotFailed 快照创建失败
	ErrSnapshotFailed = errors.New("快照创建失败")
	// ErrFeatureNotSupported 当引擎不支持特定功能时返回
	ErrFeatureNotSupported = errors.New("功能不支持")
)

// Engine 存储引擎接口
type Engine interface {
	// Open 打开存储引擎
	Open() error
	// Close 关闭存储引擎
	Close() error
	// CreateDB 创建数据库
	CreateDB(ctx context.Context, dbName string) error
	// DropDB 删除数据库
	DropDB(ctx context.Context, dbName string) error
	// CreateTable 创建表
	CreateTable(ctx context.Context, dbName, tableName string, schema *TableSchema) error
	// DropTable 删除表
	DropTable(ctx context.Context, dbName, tableName string) error
	// Get 获取数据
	Get(ctx context.Context, dbName, tableName string, key []byte) ([]byte, error)
	// Put 存储数据
	Put(ctx context.Context, dbName, tableName string, key, value []byte) error
	// Delete 删除数据
	Delete(ctx context.Context, dbName, tableName string, key []byte) error
	// BeginTx 开始事务
	BeginTx(ctx context.Context) (Transaction, error)
	// Scan 扫描数据
	Scan(ctx context.Context, dbName, tableName string, startKey, endKey []byte) (Iterator, error)
	// SetReplicationHandler 设置复制处理器，用于同步复制操作
	SetReplicationHandler(handler ReplicationHandler)

	// ListDBs 获取所有数据库
	ListDBs(ctx context.Context) ([]string, error)
	// ListTables 获取指定数据库中的所有表
	ListTables(ctx context.Context, dbName string) ([]string, error)
	// GetTableSchema 获取表结构
	GetTableSchema(ctx context.Context, dbName, tableName string) (*TableSchema, error)

	// Batch 批量操作接口
	Batch(ctx context.Context, ops []BatchOperation) error

	// Stats 获取引擎状态统计信息
	Stats(ctx context.Context) (*EngineStats, error)

	// Snapshot 创建快照
	Snapshot(ctx context.Context) (EngineSnapshot, error)

	// Compact 压缩存储空间
	Compact(ctx context.Context) error

	// Backup 备份数据到指定写入器
	Backup(ctx context.Context, writer io.Writer) error

	// Restore 从指定读取器恢复数据
	Restore(ctx context.Context, reader io.Reader) error

	// Flush 将内存中的数据刷新到持久化存储
	Flush(ctx context.Context) error
}

// Transaction 事务接口
type Transaction interface {
	// Get 获取数据
	Get(ctx context.Context, dbName, tableName string, key []byte) ([]byte, error)
	// Put 存储数据
	Put(ctx context.Context, dbName, tableName string, key, value []byte) error
	// Delete 删除数据
	Delete(ctx context.Context, dbName, tableName string, key []byte) error
	// Commit 提交事务
	Commit(ctx context.Context) error
	// Rollback 回滚事务
	Rollback(ctx context.Context) error
	// Scan 在事务中扫描数据
	Scan(ctx context.Context, dbName, tableName string, startKey, endKey []byte) (Iterator, error)
	// BatchWrite 批量写入操作
	BatchWrite(ctx context.Context, ops []BatchOperation) error
}

// Iterator 迭代器接口
type Iterator interface {
	// Next 移动到下一条记录
	Next() bool
	// Key 获取当前记录的键
	Key() []byte
	// Value 获取当前记录的值
	Value() []byte
	// Error 获取迭代过程中的错误
	Error() error
	// Close 关闭迭代器
	Close() error
	// Seek 寻找特定键或其后继
	Seek(key []byte) bool
	// Valid 检查迭代器是否有效
	Valid() bool
}

// ColumnType 列类型
type ColumnType int

const (
	// TypeInt 整数类型
	TypeInt ColumnType = iota
	// TypeFloat 浮点数类型
	TypeFloat
	// TypeString 字符串类型
	TypeString
	// TypeBytes 字节数组类型
	TypeBytes
	// TypeBool 布尔类型
	TypeBool
	// TypeTimestamp 时间戳类型
	TypeTimestamp
	// TypeJSON JSON类型
	TypeJSON
	// TypeUUID UUID类型
	TypeUUID
)

// ColumnDefinition 列定义
type ColumnDefinition struct {
	Name     string      // 列名
	Type     ColumnType  // 列类型
	Nullable bool        // 是否可空
	Primary  bool        // 是否为主键
	Unique   bool        // 是否唯一
	Default  interface{} // 默认值
	Comment  string      // 列注释
}

// IndexDefinition 索引定义
type IndexDefinition struct {
	Name    string   // 索引名
	Columns []string // 包含的列
	Unique  bool     // 是否唯一索引
	Type    string   // 索引类型（btree, hash, etc.）
}

// TableSchema 表结构
type TableSchema struct {
	Columns    []ColumnDefinition // 列定义
	Indexes    []IndexDefinition  // 索引定义
	Comment    string             // 表注释
	CreateTime int64              // 创建时间
	UpdateTime int64              // 更新时间
}

// ReplicationHandler 复制处理器接口，用于将存储引擎的操作发送到复制系统
type ReplicationHandler interface {
	// ReplicateOperation 复制操作
	ReplicateOperation(operation string, dbName, tableName string, key, value []byte, schema *TableSchema) error
}

// BatchOperationType 批量操作类型
type BatchOperationType int

const (
	// BatchPut 批量写入操作
	BatchPut BatchOperationType = iota
	// BatchDelete 批量删除操作
	BatchDelete
)

// BatchOperation 批量操作
type BatchOperation struct {
	Type      BatchOperationType // 操作类型
	DBName    string             // 数据库名
	TableName string             // 表名
	Key       []byte             // 键
	Value     []byte             // 值（仅对于Put操作）
}

// EngineStats 引擎状态统计信息
type EngineStats struct {
	EngineType       string            // 引擎类型
	DBCount          int               // 数据库数量
	TableCount       int               // 表数量
	KeyCount         int64             // 键数量
	DiskUsage        int64             // 磁盘使用量（字节）
	MemoryUsage      int64             // 内存使用量（字节）
	ReadOps          int64             // 读操作计数
	WriteOps         int64             // 写操作计数
	ReadLatency      int64             // 平均读延迟（纳秒）
	WriteLatency     int64             // 平均写延迟（纳秒）
	CacheHitRatio    float64           // 缓存命中率
	CompactionStatus string            // 压缩状态
	UpTime           int64             // 运行时间（秒）
	LastError        string            // 最后一个错误
	AdditionalInfo   map[string]string // 额外信息
}

// EngineSnapshot 引擎快照接口
type EngineSnapshot interface {
	// ID 获取快照ID
	ID() string

	// Timestamp 获取快照时间戳
	Timestamp() int64

	// Release 释放快照
	Release() error

	// Export 导出快照到指定路径
	Export(path string) error
}
