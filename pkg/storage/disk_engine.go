package storage

import (
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cyberdb/cyberdb/pkg/common/logger"
)

// 前缀常量用于区分不同类型的键
const (
	// 表结构的前缀
	schemaKeyPrefix = "schema/"
	// 数据的前缀
	dataKeyPrefix = "data/"
	// 元数据前缀
	metaKeyPrefix = "meta/"
)

// DiskEngine 基于Pebble的持久化存储引擎
type DiskEngine struct {
	// 数据库目录
	dataDir string

	// Pebble实例
	db *pebble.DB

	// 缓存表结构以减少读取
	schemaCache sync.Map // map[string]*TableSchema

	// 全局锁
	mu sync.RWMutex

	// 复制处理器
	replicationHandler ReplicationHandler
}

// NewDiskEngine 创建新的基于磁盘的存储引擎
func NewDiskEngine(dataDir string) *DiskEngine {
	return &DiskEngine{
		dataDir: dataDir,
	}
}

// Open 打开磁盘引擎
func (e *DiskEngine) Open() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 创建Pebble数据库
	opts := &pebble.Options{
		// 设置缓存大小为1GB
		Cache: pebble.NewCache(1 << 30),
		// 启用压缩以节省空间
		Levels: []pebble.LevelOptions{
			{
				Compression: pebble.SnappyCompression,
			},
		},
	}

	db, err := pebble.Open(e.dataDir, opts)
	if err != nil {
		return fmt.Errorf("打开Pebble数据库失败: %w", err)
	}

	e.db = db

	// 初始化schema缓存
	e.initSchemaCache()

	return nil
}

// initSchemaCache 从磁盘中读取所有schema并初始化缓存
func (e *DiskEngine) initSchemaCache() error {
	// 使用迭代器查找所有schema
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(schemaKeyPrefix),
		UpperBound: []byte(schemaKeyPrefix + "\xff"),
	})
	if err != nil {
		return fmt.Errorf("创建迭代器失败: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		value := iter.Value()

		// 解析键（格式: schema/dbName/tableName）
		parts := strings.Split(key, "/")
		if len(parts) != 3 {
			continue
		}

		var schema TableSchema
		if err := json.Unmarshal(value, &schema); err != nil {
			logger.Error("解析表结构失败: " + err.Error())
			continue
		}

		// 将schema添加到缓存中
		e.schemaCache.Store(key, &schema)
	}

	return nil
}

// Close 关闭磁盘引擎
func (e *DiskEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.db != nil {
		return e.db.Close()
	}
	return nil
}

// SetReplicationHandler 设置复制处理器
func (e *DiskEngine) SetReplicationHandler(handler ReplicationHandler) {
	e.replicationHandler = handler
}

// CreateDB 创建数据库
func (e *DiskEngine) CreateDB(ctx context.Context, dbName string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 检查数据库是否已存在
	metaKey := []byte(fmt.Sprintf("%s%s", metaKeyPrefix, dbName))
	_, closer, err := e.db.Get(metaKey)
	if err == nil {
		closer.Close()
		return ErrDBExists
	}
	if err != pebble.ErrNotFound {
		return err
	}

	// 创建数据库元数据
	if err := e.db.Set(metaKey, []byte("exists"), pebble.Sync); err != nil {
		return err
	}

	// 复制操作
	if e.replicationHandler != nil {
		if err := e.replicationHandler.ReplicateOperation(
			"CreateDB", dbName, "", nil, nil, nil); err != nil {
			logger.Error("复制CreateDB操作失败: " + err.Error())
		}
	}

	return nil
}

// DropDB 删除数据库
func (e *DiskEngine) DropDB(ctx context.Context, dbName string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 检查数据库是否存在
	metaKey := []byte(fmt.Sprintf("%s%s", metaKeyPrefix, dbName))
	_, closer, err := e.db.Get(metaKey)
	if err == pebble.ErrNotFound {
		return ErrDBNotFound
	}
	if err != nil {
		return err
	}
	closer.Close()

	// 删除数据库元数据
	if err := e.db.Delete(metaKey, pebble.Sync); err != nil {
		return err
	}

	// 删除所有表数据和schema
	// 1. 先找出所有属于该数据库的表
	schemaPrefix := []byte(fmt.Sprintf("%s%s/", schemaKeyPrefix, dbName))
	dataPrefix := []byte(fmt.Sprintf("%s%s/", dataKeyPrefix, dbName))

	// 删除所有表结构
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: schemaPrefix,
		UpperBound: append(schemaPrefix, 0xFF),
	})
	if err != nil {
		return fmt.Errorf("创建迭代器失败: %w", err)
	}

	batch := e.db.NewBatch()
	defer batch.Close()

	// 收集所有需要删除的schema键
	var schemaKeys []string
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		batch.Delete(iter.Key(), nil)
		schemaKeys = append(schemaKeys, key)
	}
	iter.Close()

	// 删除所有数据
	dataIter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: dataPrefix,
		UpperBound: append(dataPrefix, 0xFF),
	})
	if err != nil {
		return fmt.Errorf("创建数据迭代器失败: %w", err)
	}

	for dataIter.First(); dataIter.Valid(); dataIter.Next() {
		batch.Delete(dataIter.Key(), nil)
	}
	dataIter.Close()

	// 批量提交删除操作
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}

	// 从缓存中移除所有相关表结构
	for _, key := range schemaKeys {
		e.schemaCache.Delete(key)
	}

	// 复制操作
	if e.replicationHandler != nil {
		if err := e.replicationHandler.ReplicateOperation(
			"DropDB", dbName, "", nil, nil, nil); err != nil {
			logger.Error("复制DropDB操作失败: " + err.Error())
		}
	}

	return nil
}

// CreateTable 创建表
func (e *DiskEngine) CreateTable(ctx context.Context, dbName, tableName string, schema *TableSchema) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 检查数据库是否存在
	metaKey := []byte(fmt.Sprintf("%s%s", metaKeyPrefix, dbName))
	_, closer, err := e.db.Get(metaKey)
	if err == pebble.ErrNotFound {
		return ErrDBNotFound
	}
	if err != nil {
		return err
	}
	closer.Close()

	// 检查表是否已存在
	schemaKey := fmt.Sprintf("%s%s/%s", schemaKeyPrefix, dbName, tableName)
	_, schemaCloser, err := e.db.Get([]byte(schemaKey))
	if err == nil {
		schemaCloser.Close()
		return ErrTableExists
	}
	if err != pebble.ErrNotFound {
		return err
	}

	// 序列化表结构
	schemaData, err := json.Marshal(schema)
	if err != nil {
		return err
	}

	// 存储表结构
	if err := e.db.Set([]byte(schemaKey), schemaData, pebble.Sync); err != nil {
		return err
	}

	// 更新内存缓存
	e.schemaCache.Store(schemaKey, schema)

	// 复制操作
	if e.replicationHandler != nil {
		if err := e.replicationHandler.ReplicateOperation(
			"CreateTable", dbName, tableName, nil, nil, schema); err != nil {
			logger.Error("复制CreateTable操作失败: " + err.Error())
		}
	}

	return nil
}

// DropTable 删除表
func (e *DiskEngine) DropTable(ctx context.Context, dbName, tableName string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 检查数据库是否存在
	metaKey := []byte(fmt.Sprintf("%s%s", metaKeyPrefix, dbName))
	_, closer, err := e.db.Get(metaKey)
	if err == pebble.ErrNotFound {
		return ErrDBNotFound
	}
	if err != nil {
		return err
	}
	closer.Close()

	// 检查表是否存在
	schemaKey := fmt.Sprintf("%s%s/%s", schemaKeyPrefix, dbName, tableName)
	_, schemaCloser, err := e.db.Get([]byte(schemaKey))
	if err == pebble.ErrNotFound {
		return ErrTableNotFound
	}
	if err != nil {
		return err
	}
	schemaCloser.Close()

	// 删除表结构
	if err := e.db.Delete([]byte(schemaKey), pebble.Sync); err != nil {
		return err
	}

	// 删除表中的所有数据
	dataPrefix := []byte(fmt.Sprintf("%s%s/%s/", dataKeyPrefix, dbName, tableName))

	batch := e.db.NewBatch()
	defer batch.Close()

	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: dataPrefix,
		UpperBound: append(dataPrefix, 0xFF),
	})
	if err != nil {
		return fmt.Errorf("创建迭代器失败: %w", err)
	}

	for iter.First(); iter.Valid(); iter.Next() {
		batch.Delete(iter.Key(), nil)
	}
	iter.Close()

	// 批量提交删除操作
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}

	// 从缓存中移除表结构
	e.schemaCache.Delete(schemaKey)

	// 复制操作
	if e.replicationHandler != nil {
		if err := e.replicationHandler.ReplicateOperation(
			"DropTable", dbName, tableName, nil, nil, nil); err != nil {
			logger.Error("复制DropTable操作失败: " + err.Error())
		}
	}

	return nil
}

// Get 获取数据
func (e *DiskEngine) Get(ctx context.Context, dbName, tableName string, key []byte) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 构建完整的键
	fullKey := makeDataKey(dbName, tableName, key)

	// 从Pebble中获取值
	value, closer, err := e.db.Get(fullKey)
	if err == pebble.ErrNotFound {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}

	// 复制值，因为一旦closer关闭，value就无效了
	result := make([]byte, len(value))
	copy(result, value)
	closer.Close()

	return result, nil
}

// Put 存储数据
func (e *DiskEngine) Put(ctx context.Context, dbName, tableName string, key, value []byte) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 检查表是否存在
	schemaKey := fmt.Sprintf("%s%s/%s", schemaKeyPrefix, dbName, tableName)
	_, ok := e.schemaCache.Load(schemaKey)
	if !ok {
		// 从磁盘检查
		_, schemaCloser, err := e.db.Get([]byte(schemaKey))
		if err == pebble.ErrNotFound {
			return ErrTableNotFound
		}
		if err != nil {
			return err
		}
		schemaCloser.Close()
	}

	// 构建完整的键
	fullKey := makeDataKey(dbName, tableName, key)

	// 存储数据
	if err := e.db.Set(fullKey, value, nil); err != nil {
		return err
	}

	// 复制操作
	if e.replicationHandler != nil {
		if err := e.replicationHandler.ReplicateOperation(
			"Put", dbName, tableName, key, value, nil); err != nil {
			logger.Error("复制Put操作失败: " + err.Error())
		}
	}

	return nil
}

// Delete 删除数据
func (e *DiskEngine) Delete(ctx context.Context, dbName, tableName string, key []byte) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 检查表是否存在
	schemaKey := fmt.Sprintf("%s%s/%s", schemaKeyPrefix, dbName, tableName)
	_, ok := e.schemaCache.Load(schemaKey)
	if !ok {
		// 从磁盘检查
		_, schemaCloser, err := e.db.Get([]byte(schemaKey))
		if err == pebble.ErrNotFound {
			return ErrTableNotFound
		}
		if err != nil {
			return err
		}
		schemaCloser.Close()
	}

	// 构建完整的键
	fullKey := makeDataKey(dbName, tableName, key)

	// 检查键是否存在
	_, closer, err := e.db.Get(fullKey)
	if err == pebble.ErrNotFound {
		return ErrKeyNotFound
	}
	if err != nil {
		return err
	}
	closer.Close()

	// 删除数据
	if err := e.db.Delete(fullKey, nil); err != nil {
		return err
	}

	// 复制操作
	if e.replicationHandler != nil {
		if err := e.replicationHandler.ReplicateOperation(
			"Delete", dbName, tableName, key, nil, nil); err != nil {
			logger.Error("复制Delete操作失败: " + err.Error())
		}
	}

	return nil
}

// BeginTx 开始事务
func (e *DiskEngine) BeginTx(ctx context.Context) (Transaction, error) {
	return &DiskTransaction{
		engine: e,
		batch:  e.db.NewBatch(),
	}, nil
}

// Scan 扫描指定范围的数据
func (e *DiskEngine) Scan(ctx context.Context, dbName, tableName string, startKey, endKey []byte) (Iterator, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.db == nil {
		return nil, fmt.Errorf("数据库未打开")
	}

	// 检查数据库是否存在
	metaKey := []byte(fmt.Sprintf("%s%s", metaKeyPrefix, dbName))
	_, closer, err := e.db.Get(metaKey)
	if err == pebble.ErrNotFound {
		return nil, ErrDBNotFound
	}
	if err != nil {
		return nil, err
	}
	closer.Close()

	// 构造前缀
	prefix := makeDataKey(dbName, tableName, nil)
	prefixLen := len(prefix)

	// 构造查询范围
	var lowerBound, upperBound []byte
	if startKey != nil {
		lowerBound = makeDataKey(dbName, tableName, startKey)
	} else {
		lowerBound = prefix
	}

	if endKey != nil {
		upperBound = makeDataKey(dbName, tableName, endKey)
	} else {
		// 使用前缀的下一个字节作为上界
		upperBound = append(append([]byte{}, prefix...), 0xFF)
	}

	// 创建迭代器
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("创建迭代器失败: %w", err)
	}

	return &DiskIterator{
		iter:      iter,
		prefix:    prefix,
		prefixLen: prefixLen,
	}, nil
}

// ListDBs 获取所有数据库
func (e *DiskEngine) ListDBs(ctx context.Context) ([]string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 使用前缀迭代器查找所有数据库元数据
	metaPrefix := []byte(metaKeyPrefix)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: metaPrefix,
		UpperBound: append(metaPrefix, 0xFF),
	})
	if err != nil {
		return nil, fmt.Errorf("创建迭代器失败: %w", err)
	}
	defer iter.Close()

	dbs := make([]string, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		dbName := key[len(metaKeyPrefix):]
		dbs = append(dbs, dbName)
	}

	return dbs, nil
}

// ListTables 获取指定数据库中的所有表
func (e *DiskEngine) ListTables(ctx context.Context, dbName string) ([]string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 检查数据库是否存在
	metaKey := []byte(fmt.Sprintf("%s%s", metaKeyPrefix, dbName))
	_, closer, err := e.db.Get(metaKey)
	if err == pebble.ErrNotFound {
		return nil, ErrDBNotFound
	}
	if err != nil {
		return nil, err
	}
	closer.Close()

	// 使用前缀迭代器查找所有表
	schemaPrefix := []byte(fmt.Sprintf("%s%s/", schemaKeyPrefix, dbName))
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: schemaPrefix,
		UpperBound: append(schemaPrefix, 0xFF),
	})
	if err != nil {
		return nil, fmt.Errorf("创建迭代器失败: %w", err)
	}
	defer iter.Close()

	tables := make([]string, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		parts := strings.Split(key, "/")
		if len(parts) == 3 {
			tables = append(tables, parts[2])
		}
	}

	return tables, nil
}

// GetTableSchema 获取表结构
func (e *DiskEngine) GetTableSchema(ctx context.Context, dbName, tableName string) (*TableSchema, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 先从缓存中查找
	schemaKey := fmt.Sprintf("%s%s/%s", schemaKeyPrefix, dbName, tableName)
	if cachedSchema, ok := e.schemaCache.Load(schemaKey); ok {
		return cachedSchema.(*TableSchema), nil
	}

	// 从磁盘读取
	key := []byte(schemaKey)
	value, closer, err := e.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, ErrTableNotFound
	}
	if err != nil {
		return nil, err
	}

	// 确保在函数返回前关闭
	defer closer.Close()

	// 解码表结构
	var schema TableSchema
	if err := json.Unmarshal(value, &schema); err != nil {
		return nil, err
	}

	// 更新缓存
	e.schemaCache.Store(schemaKey, &schema)

	return &schema, nil
}

// Batch 批量操作
func (e *DiskEngine) Batch(ctx context.Context, ops []BatchOperation) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	batch := e.db.NewBatch()
	defer batch.Close()

	for _, op := range ops {
		switch op.Type {
		case BatchPut:
			dataKey := makeDataKey(op.DBName, op.TableName, op.Key)
			if err := batch.Set(dataKey, op.Value, nil); err != nil {
				return err
			}
		case BatchDelete:
			dataKey := makeDataKey(op.DBName, op.TableName, op.Key)
			if err := batch.Delete(dataKey, nil); err != nil {
				return err
			}
		default:
			return fmt.Errorf("不支持的批量操作类型: %v", op.Type)
		}
	}

	// 复制操作
	if e.replicationHandler != nil {
		for _, op := range ops {
			var opType string
			switch op.Type {
			case BatchPut:
				opType = "Put"
			case BatchDelete:
				opType = "Delete"
			}
			if err := e.replicationHandler.ReplicateOperation(
				opType, op.DBName, op.TableName, op.Key, op.Value, nil); err != nil {
				logger.Error("复制批量操作失败: " + err.Error())
			}
		}
	}

	// 提交批量操作
	return batch.Commit(pebble.Sync)
}

// Stats 获取引擎状态统计信息
func (e *DiskEngine) Stats(ctx context.Context) (*EngineStats, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.db == nil {
		return nil, fmt.Errorf("引擎未打开")
	}

	// 获取Pebble的内部度量
	metrics := e.db.Metrics()

	// 计算数据库和表的数量
	dbs, err := e.ListDBs(ctx)
	if err != nil {
		return nil, err
	}

	dbCount := len(dbs)
	tableCount := 0
	keyCount := int64(0)

	for _, dbName := range dbs {
		tables, err := e.ListTables(ctx, dbName)
		if err != nil {
			continue
		}
		tableCount += len(tables)

		// 估计每个表的行数
		for _, tableName := range tables {
			// 为简化起见，使用迭代器进行简单计数
			dataPrefix := []byte(fmt.Sprintf("%s%s/%s/", dataKeyPrefix, dbName, tableName))
			iter, err := e.db.NewIter(&pebble.IterOptions{
				LowerBound: dataPrefix,
				UpperBound: append(dataPrefix, 0xFF),
			})
			if err != nil {
				continue
			}

			count := int64(0)
			for iter.First(); iter.Valid(); iter.Next() {
				count++
			}
			iter.Close()
			keyCount += count
		}
	}

	// 创建状态信息
	stats := &EngineStats{
		EngineType:       "disk",
		DBCount:          dbCount,
		TableCount:       tableCount,
		KeyCount:         keyCount,
		DiskUsage:        int64(metrics.DiskSpaceUsage()),
		MemoryUsage:      int64(metrics.BlockCache.Size),
		ReadOps:          int64(metrics.WAL.BytesIn),
		WriteOps:         int64(metrics.WAL.BytesWritten),
		ReadLatency:      0, // Pebble不直接提供此指标
		WriteLatency:     0, // Pebble不直接提供此指标
		CacheHitRatio:    0, // 需要自行计算或提供替代指标
		CompactionStatus: fmt.Sprintf("活跃: %d", metrics.Compact.Count),
		UpTime:           0, // 此处需要存储引擎启动时间
		AdditionalInfo:   make(map[string]string),
	}

	// 添加额外信息
	stats.AdditionalInfo["levels"] = fmt.Sprintf("%d", len(metrics.Levels))
	stats.AdditionalInfo["mem_table_size"] = fmt.Sprintf("%d", metrics.MemTable.Size)
	stats.AdditionalInfo["sst_file_count"] = fmt.Sprintf("%d", metrics.Table.ZombieCount+metrics.Table.ObsoleteCount)
	stats.AdditionalInfo["bytes_written"] = fmt.Sprintf("%d", metrics.WAL.BytesWritten)

	return stats, nil
}

// PebbleSnapshot Pebble快照实现
type PebbleSnapshot struct {
	id        string
	timestamp int64
	snapshot  *pebble.Snapshot
}

// ID 获取快照ID
func (s *PebbleSnapshot) ID() string {
	return s.id
}

// Timestamp 获取快照时间戳
func (s *PebbleSnapshot) Timestamp() int64 {
	return s.timestamp
}

// Release 释放快照
func (s *PebbleSnapshot) Release() error {
	return s.snapshot.Close()
}

// Export 导出快照到指定路径
func (s *PebbleSnapshot) Export(path string) error {
	// Pebble不直接支持导出快照到文件
	// 此处需要使用迭代器手动导出数据
	return ErrFeatureNotSupported
}

// Snapshot 创建快照
func (e *DiskEngine) Snapshot(ctx context.Context) (EngineSnapshot, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.db == nil {
		return nil, fmt.Errorf("引擎未打开")
	}

	// 创建Pebble快照
	snapshot := e.db.NewSnapshot()

	// 生成唯一ID
	id := fmt.Sprintf("snapshot-%d", time.Now().UnixNano())

	return &PebbleSnapshot{
		id:        id,
		timestamp: time.Now().Unix(),
		snapshot:  snapshot,
	}, nil
}

// Compact 压缩存储空间
func (e *DiskEngine) Compact(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.db == nil {
		return fmt.Errorf("引擎未打开")
	}

	// 执行整个键空间的压缩
	return e.db.Compact(nil, []byte{0xFF}, false)
}

// Backup 备份数据到指定写入器
func (e *DiskEngine) Backup(ctx context.Context, writer io.Writer) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.db == nil {
		return fmt.Errorf("引擎未打开")
	}

	// 这里简化实现，实际应该使用Pebble的Backup或者Checkpoint功能
	// 或者使用迭代器导出所有数据

	// 创建一个encoder
	enc := gob.NewEncoder(writer)

	// 备份所有数据库
	dbs, err := e.ListDBs(ctx)
	if err != nil {
		return err
	}

	// 写入数据库数量
	if err := enc.Encode(len(dbs)); err != nil {
		return err
	}

	// 遍历每个数据库
	for _, dbName := range dbs {
		// 写入数据库名
		if err := enc.Encode(dbName); err != nil {
			return err
		}

		// 获取数据库中的所有表
		tables, err := e.ListTables(ctx, dbName)
		if err != nil {
			return err
		}

		// 写入表数量
		if err := enc.Encode(len(tables)); err != nil {
			return err
		}

		// 遍历每个表
		for _, tableName := range tables {
			// 写入表名
			if err := enc.Encode(tableName); err != nil {
				return err
			}

			// 获取表结构
			schema, err := e.GetTableSchema(ctx, dbName, tableName)
			if err != nil {
				return err
			}

			// 写入表结构
			if err := enc.Encode(schema); err != nil {
				return err
			}

			// 读取表中的所有数据
			dataPrefix := []byte(fmt.Sprintf("%s%s/%s/", dataKeyPrefix, dbName, tableName))
			iter, err := e.db.NewIter(&pebble.IterOptions{
				LowerBound: dataPrefix,
				UpperBound: append(dataPrefix, 0xFF),
			})
			if err != nil {
				return fmt.Errorf("创建迭代器失败: %w", err)
			}
			defer iter.Close()

			// 统计键值对数量
			keyCount := 0
			for iter.First(); iter.Valid(); iter.Next() {
				keyCount++
			}

			// 写入键值对数量
			if err := enc.Encode(keyCount); err != nil {
				return err
			}

			// 重置迭代器并导出所有键值对
			for iter.First(); iter.Valid(); iter.Next() {
				key := iter.Key()
				value := iter.Value()

				// 提取实际的键（去除前缀）
				actualKey := key[len(dataPrefix):]

				// 写入键和值
				if err := enc.Encode(actualKey); err != nil {
					return err
				}
				if err := enc.Encode(value); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// Restore 从指定读取器恢复数据
func (e *DiskEngine) Restore(ctx context.Context, reader io.Reader) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.db == nil {
		return fmt.Errorf("引擎未打开")
	}

	// 清空schema缓存
	e.schemaCache = sync.Map{}

	// 创建decoder
	dec := gob.NewDecoder(reader)

	// 读取数据库数量
	var dbCount int
	if err := dec.Decode(&dbCount); err != nil {
		return err
	}

	// 创建一个大的batch来提高性能
	batch := e.db.NewBatch()
	defer batch.Close()

	// 遍历每个数据库
	for i := 0; i < dbCount; i++ {
		// 读取数据库名
		var dbName string
		if err := dec.Decode(&dbName); err != nil {
			return err
		}

		// 创建数据库
		metaKey := []byte(fmt.Sprintf("%s%s", metaKeyPrefix, dbName))
		if err := batch.Set(metaKey, []byte("exists"), nil); err != nil {
			return err
		}

		// 读取表数量
		var tableCount int
		if err := dec.Decode(&tableCount); err != nil {
			return err
		}

		// 遍历每个表
		for j := 0; j < tableCount; j++ {
			// 读取表名
			var tableName string
			if err := dec.Decode(&tableName); err != nil {
				return err
			}

			// 读取表结构
			var schema TableSchema
			if err := dec.Decode(&schema); err != nil {
				return err
			}

			// 保存表结构
			schemaKey := []byte(fmt.Sprintf("%s%s/%s", schemaKeyPrefix, dbName, tableName))
			schemaData, err := json.Marshal(&schema)
			if err != nil {
				return err
			}
			if err := batch.Set(schemaKey, schemaData, nil); err != nil {
				return err
			}

			// 读取键值对数量
			var keyCount int
			if err := dec.Decode(&keyCount); err != nil {
				return err
			}

			// 读取并保存所有键值对
			for k := 0; k < keyCount; k++ {
				var actualKey []byte
				if err := dec.Decode(&actualKey); err != nil {
					return err
				}

				var value []byte
				if err := dec.Decode(&value); err != nil {
					return err
				}

				// 构造完整的键
				dataKey := makeDataKey(dbName, tableName, actualKey)

				// 保存键值对
				if err := batch.Set(dataKey, value, nil); err != nil {
					return err
				}

				// 定期提交batch以避免过大
				if batch.Count() > 1000 {
					if err := batch.Commit(pebble.Sync); err != nil {
						return err
					}
					batch = e.db.NewBatch()
				}
			}
		}
	}

	// 提交所有剩余的操作
	return batch.Commit(pebble.Sync)
}

// Flush 将内存中的数据刷新到持久化存储
func (e *DiskEngine) Flush(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.db == nil {
		return fmt.Errorf("引擎未打开")
	}

	// 使用Pebble的Flush方法
	return e.db.Flush()
}

// DiskTransaction Pebble事务实现
type DiskTransaction struct {
	engine *DiskEngine
	batch  *pebble.Batch
	closed bool
	mu     sync.Mutex
}

// Get 事务中获取数据
func (tx *DiskTransaction) Get(ctx context.Context, dbName, tableName string, key []byte) ([]byte, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.closed {
		return nil, fmt.Errorf("事务已关闭")
	}

	// 构建完整的键
	fullKey := makeDataKey(dbName, tableName, key)

	// 先从batch中查找
	value, closer, err := tx.batch.Get(fullKey)
	if err != pebble.ErrNotFound {
		if err != nil {
			return nil, err
		}
		result := make([]byte, len(value))
		copy(result, value)
		closer.Close()
		return result, nil
	}

	// 从基础引擎中查找
	return tx.engine.Get(ctx, dbName, tableName, key)
}

// Put 事务中存储数据
func (tx *DiskTransaction) Put(ctx context.Context, dbName, tableName string, key, value []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.closed {
		return fmt.Errorf("事务已关闭")
	}

	// 检查表是否存在
	schemaKey := fmt.Sprintf("%s%s/%s", schemaKeyPrefix, dbName, tableName)
	_, ok := tx.engine.schemaCache.Load(schemaKey)
	if !ok {
		// 从磁盘检查
		_, schemaCloser, err := tx.engine.db.Get([]byte(schemaKey))
		if err == pebble.ErrNotFound {
			return ErrTableNotFound
		}
		if err != nil {
			return err
		}
		schemaCloser.Close()
	}

	// 构建完整的键
	fullKey := makeDataKey(dbName, tableName, key)

	// 将操作添加到batch中
	return tx.batch.Set(fullKey, value, nil)
}

// Delete 事务中删除数据
func (tx *DiskTransaction) Delete(ctx context.Context, dbName, tableName string, key []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.closed {
		return fmt.Errorf("事务已关闭")
	}

	// 检查表是否存在
	schemaKey := fmt.Sprintf("%s%s/%s", schemaKeyPrefix, dbName, tableName)
	_, ok := tx.engine.schemaCache.Load(schemaKey)
	if !ok {
		// 从磁盘检查
		_, schemaCloser, err := tx.engine.db.Get([]byte(schemaKey))
		if err == pebble.ErrNotFound {
			return ErrTableNotFound
		}
		if err != nil {
			return err
		}
		schemaCloser.Close()
	}

	// 构建完整的键
	fullKey := makeDataKey(dbName, tableName, key)

	// 检查键是否存在
	exists := false

	// 先检查batch中是否有这个键
	_, closer, err := tx.batch.Get(fullKey)
	if err == nil {
		exists = true
		closer.Close()
	} else if err != pebble.ErrNotFound {
		return err
	}

	// 如果batch中没有，检查基础DB
	if !exists {
		_, dbCloser, err := tx.engine.db.Get(fullKey)
		if err == pebble.ErrNotFound {
			return ErrKeyNotFound
		}
		if err != nil {
			return err
		}
		dbCloser.Close()
	}

	// 将删除操作添加到batch中
	return tx.batch.Delete(fullKey, nil)
}

// Commit 提交事务
func (tx *DiskTransaction) Commit(ctx context.Context) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.closed {
		return fmt.Errorf("事务已关闭")
	}

	// 提交batch
	err := tx.batch.Commit(pebble.Sync)

	// 无论是否成功，都关闭batch
	tx.batch.Close()
	tx.closed = true

	if err != nil {
		return fmt.Errorf("提交事务失败: %w", err)
	}

	// 如果设置了复制处理器，可以在这里添加事务的批量复制逻辑

	return nil
}

// Rollback 回滚事务
func (tx *DiskTransaction) Rollback(ctx context.Context) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.closed {
		return fmt.Errorf("事务已关闭")
	}

	// 直接关闭batch即可，不提交就是回滚
	tx.batch.Close()
	tx.closed = true

	return nil
}

// BatchWrite 批量写入操作
func (tx *DiskTransaction) BatchWrite(ctx context.Context, ops []BatchOperation) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.closed {
		return ErrTransactionFailed
	}

	for _, op := range ops {
		switch op.Type {
		case BatchPut:
			dataKey := makeDataKey(op.DBName, op.TableName, op.Key)
			if err := tx.batch.Set(dataKey, op.Value, nil); err != nil {
				return err
			}
		case BatchDelete:
			dataKey := makeDataKey(op.DBName, op.TableName, op.Key)
			if err := tx.batch.Delete(dataKey, nil); err != nil {
				return err
			}
		default:
			return fmt.Errorf("不支持的批量操作类型: %v", op.Type)
		}
	}

	return nil
}

// Scan 在事务中扫描指定范围的数据
func (tx *DiskTransaction) Scan(ctx context.Context, dbName, tableName string, startKey, endKey []byte) (Iterator, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.closed {
		return nil, fmt.Errorf("事务已关闭")
	}

	if tx.engine.db == nil {
		return nil, fmt.Errorf("数据库未打开")
	}

	// 检查数据库是否存在
	metaKey := []byte(fmt.Sprintf("%s%s", metaKeyPrefix, dbName))
	_, closer, err := tx.engine.db.Get(metaKey)
	if err == pebble.ErrNotFound {
		return nil, ErrDBNotFound
	}
	if err != nil {
		return nil, err
	}
	closer.Close()

	// 构造前缀
	prefix := makeDataKey(dbName, tableName, nil)
	prefixLen := len(prefix)

	// 构造查询范围
	var lowerBound, upperBound []byte
	if startKey != nil {
		lowerBound = makeDataKey(dbName, tableName, startKey)
	} else {
		lowerBound = prefix
	}

	if endKey != nil {
		upperBound = makeDataKey(dbName, tableName, endKey)
	} else {
		// 当endKey为nil时，使用前缀的下一个字节作为上界
		upperBound = append(append([]byte{}, prefix...), 0xFF)
	}

	// 创建迭代器选项
	iterOpts := &pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	}

	// 创建Pebble迭代器
	// 注意：在事务中使用batch的NewIter
	iter, err := tx.batch.NewIter(iterOpts)
	if err != nil {
		return nil, fmt.Errorf("创建迭代器失败: %w", err)
	}

	// 创建DiskIterator，包装Pebble迭代器
	return &DiskIterator{
		iter:      iter,
		prefix:    prefix,
		prefixLen: prefixLen,
	}, nil
}

// DiskIterator 基于Pebble迭代器的实现
type DiskIterator struct {
	iter      *pebble.Iterator
	prefix    []byte
	prefixLen int
	closed    bool
	err       error
}

// Next 移动到下一个键值对
func (it *DiskIterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}

	// 如果迭代器无效，返回false
	if !it.iter.Valid() {
		return false
	}

	// 移动到下一个键值对
	if !it.iter.Next() {
		// 检查是否有错误发生
		if err := it.iter.Error(); err != nil {
			it.err = err
		}
		return false
	}

	// 检查是否超出前缀范围
	if it.prefix != nil && !it.isWithinPrefix() {
		return false
	}

	return it.iter.Valid()
}

// isWithinPrefix 检查当前键是否在前缀范围内
func (it *DiskIterator) isWithinPrefix() bool {
	if !it.iter.Valid() {
		return false
	}

	key := it.iter.Key()
	if len(key) < it.prefixLen {
		return false
	}

	for i := 0; i < it.prefixLen; i++ {
		if key[i] != it.prefix[i] {
			return false
		}
	}

	return true
}

// Key 获取当前键
func (it *DiskIterator) Key() []byte {
	if it.closed || !it.iter.Valid() {
		return nil
	}

	key := it.iter.Key()
	// 返回不带前缀的键
	if it.prefix != nil && len(key) >= it.prefixLen {
		return key[it.prefixLen:]
	}
	return key
}

// Value 获取当前值
func (it *DiskIterator) Value() []byte {
	if it.closed || !it.iter.Valid() {
		return nil
	}

	return it.iter.Value()
}

// Error 返回迭代过程中的错误
func (it *DiskIterator) Error() error {
	if it.err != nil {
		return it.err
	}

	if it.iter != nil {
		return it.iter.Error()
	}

	return nil
}

// Close 关闭迭代器
func (it *DiskIterator) Close() error {
	if it.closed {
		return nil
	}

	it.closed = true
	if it.iter != nil {
		return it.iter.Close()
	}

	return nil
}

// Seek 定位到指定的键或其后继
func (it *DiskIterator) Seek(key []byte) bool {
	if it.closed || it.err != nil {
		return false
	}

	// 如果有前缀，需要附加到搜索键上
	seekKey := key
	if it.prefix != nil {
		seekKey = append(append([]byte{}, it.prefix...), key...)
	}

	if !it.iter.SeekGE(seekKey) {
		if err := it.iter.Error(); err != nil {
			it.err = err
		}
		return false
	}

	// 检查是否仍在前缀范围内
	if it.prefix != nil && !it.isWithinPrefix() {
		return false
	}

	return it.iter.Valid()
}

// Valid 检查迭代器是否有效
func (it *DiskIterator) Valid() bool {
	return !it.closed && it.err == nil && it.iter.Valid() && (it.prefix == nil || it.isWithinPrefix())
}

// 辅助函数：构建数据键
func makeDataKey(dbName, tableName string, key []byte) []byte {
	prefix := fmt.Sprintf("%s%s/%s/", dataKeyPrefix, dbName, tableName)
	fullKey := make([]byte, len(prefix)+len(key))
	copy(fullKey, prefix)
	copy(fullKey[len(prefix):], key)
	return fullKey
}
