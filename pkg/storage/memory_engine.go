package storage

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"encoding/gob"

	"github.com/cyberdb/cyberdb/pkg/common/logger"
)

// MemoryEngine 基于内存的存储引擎实现
type MemoryEngine struct {
	// 数据库名到表名的映射
	databases map[string]map[string]*MemoryTable

	// 表结构映射
	schemas map[string]map[string]*TableSchema

	mu sync.RWMutex

	// replicationHandler 复制处理器
	replicationHandler ReplicationHandler
}

// MemoryTable 内存表结构
type MemoryTable struct {
	schema *TableSchema
	data   map[string][]byte // key -> value
	mu     sync.RWMutex
}

// NewMemoryEngine 创建新的内存引擎
func NewMemoryEngine() *MemoryEngine {
	return &MemoryEngine{
		databases: make(map[string]map[string]*MemoryTable),
		schemas:   make(map[string]map[string]*TableSchema),
	}
}

// Open 打开内存引擎
func (e *MemoryEngine) Open() error {
	// 内存引擎不需要特殊的打开操作
	return nil
}

// Close 关闭内存引擎
func (e *MemoryEngine) Close() error {
	// 内存引擎不需要特殊的关闭操作
	e.mu.Lock()
	defer e.mu.Unlock()

	// 清空所有数据
	e.databases = make(map[string]map[string]*MemoryTable)
	e.schemas = make(map[string]map[string]*TableSchema)
	return nil
}

// SetReplicationHandler 设置复制处理器
func (e *MemoryEngine) SetReplicationHandler(handler ReplicationHandler) {
	e.replicationHandler = handler
}

// CreateDB 创建数据库
func (e *MemoryEngine) CreateDB(ctx context.Context, dbName string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.databases[dbName]; exists {
		return fmt.Errorf("数据库 %s 已存在", dbName)
	}

	e.databases[dbName] = make(map[string]*MemoryTable)
	e.schemas[dbName] = make(map[string]*TableSchema)

	// 如果设置了复制处理器，复制此操作
	if e.replicationHandler != nil {
		if err := e.replicationHandler.ReplicateOperation(
			"CreateDB", dbName, "", nil, nil, nil); err != nil {
			logger.Error("复制CreateDB操作失败: " + err.Error())
		}
	}

	return nil
}

// DropDB 删除数据库
func (e *MemoryEngine) DropDB(ctx context.Context, dbName string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.databases[dbName]; !exists {
		return ErrDBNotFound
	}

	delete(e.databases, dbName)
	delete(e.schemas, dbName)

	// 如果设置了复制处理器，复制此操作
	if e.replicationHandler != nil {
		if err := e.replicationHandler.ReplicateOperation(
			"DropDB", dbName, "", nil, nil, nil); err != nil {
			logger.Error("复制DropDB操作失败: " + err.Error())
		}
	}

	return nil
}

// CreateTable 创建表
func (e *MemoryEngine) CreateTable(ctx context.Context, dbName, tableName string, schema *TableSchema) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	db, exists := e.databases[dbName]
	if !exists {
		return ErrDBNotFound
	}

	if _, exists := db[tableName]; exists {
		return fmt.Errorf("表 %s 已存在", tableName)
	}

	// 创建新表
	db[tableName] = &MemoryTable{
		schema: schema,
		data:   make(map[string][]byte),
	}

	// 保存表结构
	e.schemas[dbName][tableName] = schema

	// 如果设置了复制处理器，复制此操作
	if e.replicationHandler != nil {
		if err := e.replicationHandler.ReplicateOperation(
			"CreateTable", dbName, tableName, nil, nil, schema); err != nil {
			logger.Error("复制CreateTable操作失败: " + err.Error())
		}
	}

	return nil
}

// DropTable 删除表
func (e *MemoryEngine) DropTable(ctx context.Context, dbName, tableName string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	db, exists := e.databases[dbName]
	if !exists {
		return ErrDBNotFound
	}

	if _, exists := db[tableName]; !exists {
		return ErrTableNotFound
	}

	delete(db, tableName)
	delete(e.schemas[dbName], tableName)

	// 如果设置了复制处理器，复制此操作
	if e.replicationHandler != nil {
		if err := e.replicationHandler.ReplicateOperation(
			"DropTable", dbName, tableName, nil, nil, nil); err != nil {
			logger.Error("复制DropTable操作失败: " + err.Error())
		}
	}

	return nil
}

// Get 获取数据
func (e *MemoryEngine) Get(ctx context.Context, dbName, tableName string, key []byte) ([]byte, error) {
	e.mu.RLock()
	db, exists := e.databases[dbName]
	if !exists {
		e.mu.RUnlock()
		return nil, ErrDBNotFound
	}

	table, exists := db[tableName]
	e.mu.RUnlock()

	if !exists {
		return nil, ErrTableNotFound
	}

	table.mu.RLock()
	defer table.mu.RUnlock()

	value, exists := table.data[string(key)]
	if !exists {
		return nil, ErrKeyNotFound
	}

	return value, nil
}

// Put 存储数据
func (e *MemoryEngine) Put(ctx context.Context, dbName, tableName string, key, value []byte) error {
	e.mu.RLock()
	db, exists := e.databases[dbName]
	if !exists {
		e.mu.RUnlock()
		return ErrDBNotFound
	}

	table, exists := db[tableName]
	e.mu.RUnlock()

	if !exists {
		return ErrTableNotFound
	}

	table.mu.Lock()
	defer table.mu.Unlock()

	table.data[string(key)] = value

	// 如果设置了复制处理器，复制此操作
	if e.replicationHandler != nil {
		if err := e.replicationHandler.ReplicateOperation(
			"Put", dbName, tableName, key, value, nil); err != nil {
			logger.Error("复制Put操作失败: " + err.Error())
		}
	}

	return nil
}

// Delete 删除数据
func (e *MemoryEngine) Delete(ctx context.Context, dbName, tableName string, key []byte) error {
	e.mu.RLock()
	db, exists := e.databases[dbName]
	if !exists {
		e.mu.RUnlock()
		return ErrDBNotFound
	}

	table, exists := db[tableName]
	e.mu.RUnlock()

	if !exists {
		return ErrTableNotFound
	}

	table.mu.Lock()
	defer table.mu.Unlock()

	if _, exists := table.data[string(key)]; !exists {
		return ErrKeyNotFound
	}

	delete(table.data, string(key))

	// 如果设置了复制处理器，复制此操作
	if e.replicationHandler != nil {
		if err := e.replicationHandler.ReplicateOperation(
			"Delete", dbName, tableName, key, nil, nil); err != nil {
			logger.Error("复制Delete操作失败: " + err.Error())
		}
	}

	return nil
}

// MemoryTransaction 内存事务实现
type MemoryTransaction struct {
	engine  *MemoryEngine
	writes  map[string]map[string]map[string][]byte // dbName -> tableName -> key -> value
	deletes map[string]map[string]map[string]bool   // dbName -> tableName -> key -> true
}

// BeginTx 开始事务
func (e *MemoryEngine) BeginTx(ctx context.Context) (Transaction, error) {
	return &MemoryTransaction{
		engine:  e,
		writes:  make(map[string]map[string]map[string][]byte),
		deletes: make(map[string]map[string]map[string]bool),
	}, nil
}

// Get 事务中获取数据
func (tx *MemoryTransaction) Get(ctx context.Context, dbName, tableName string, key []byte) ([]byte, error) {
	// 检查是否在事务中被删除
	if dbMap, exists := tx.deletes[dbName]; exists {
		if tableMap, exists := dbMap[tableName]; exists {
			if _, deleted := tableMap[string(key)]; deleted {
				return nil, ErrKeyNotFound
			}
		}
	}

	// 检查是否在事务中被写入
	if dbMap, exists := tx.writes[dbName]; exists {
		if tableMap, exists := dbMap[tableName]; exists {
			if value, exists := tableMap[string(key)]; exists {
				return value, nil
			}
		}
	}

	// 从引擎中获取数据
	return tx.engine.Get(ctx, dbName, tableName, key)
}

// Put 事务中写入数据
func (tx *MemoryTransaction) Put(ctx context.Context, dbName, tableName string, key, value []byte) error {
	// 确保数据库和表存在
	tx.engine.mu.RLock()
	db, exists := tx.engine.databases[dbName]
	if !exists {
		tx.engine.mu.RUnlock()
		return ErrDBNotFound
	}

	_, exists = db[tableName]
	tx.engine.mu.RUnlock()

	if !exists {
		return ErrTableNotFound
	}

	// 在事务中写入数据
	if _, exists := tx.writes[dbName]; !exists {
		tx.writes[dbName] = make(map[string]map[string][]byte)
	}
	if _, exists := tx.writes[dbName][tableName]; !exists {
		tx.writes[dbName][tableName] = make(map[string][]byte)
	}

	tx.writes[dbName][tableName][string(key)] = value

	// 如果之前被标记为删除，则移除删除标记
	if dbMap, exists := tx.deletes[dbName]; exists {
		if tableMap, exists := dbMap[tableName]; exists {
			delete(tableMap, string(key))
		}
	}

	return nil
}

// Delete 事务中删除数据
func (tx *MemoryTransaction) Delete(ctx context.Context, dbName, tableName string, key []byte) error {
	// 确保数据库和表存在
	tx.engine.mu.RLock()
	db, exists := tx.engine.databases[dbName]
	if !exists {
		tx.engine.mu.RUnlock()
		return ErrDBNotFound
	}

	_, exists = db[tableName]
	tx.engine.mu.RUnlock()

	if !exists {
		return ErrTableNotFound
	}

	// 在事务中标记为删除
	if _, exists := tx.deletes[dbName]; !exists {
		tx.deletes[dbName] = make(map[string]map[string]bool)
	}
	if _, exists := tx.deletes[dbName][tableName]; !exists {
		tx.deletes[dbName][tableName] = make(map[string]bool)
	}

	tx.deletes[dbName][tableName][string(key)] = true

	// 如果之前在事务中写入，则移除写入
	if dbMap, exists := tx.writes[dbName]; exists {
		if tableMap, exists := dbMap[tableName]; exists {
			delete(tableMap, string(key))
		}
	}

	return nil
}

// Commit 提交事务
func (tx *MemoryTransaction) Commit(ctx context.Context) error {
	// 先应用删除操作
	for dbName, dbMap := range tx.deletes {
		for tableName, tableMap := range dbMap {
			for key := range tableMap {
				if err := tx.engine.Delete(ctx, dbName, tableName, []byte(key)); err != nil && err != ErrKeyNotFound {
					return err
				}
			}
		}
	}

	// 再应用写入操作
	for dbName, dbMap := range tx.writes {
		for tableName, tableMap := range dbMap {
			for key, value := range tableMap {
				if err := tx.engine.Put(ctx, dbName, tableName, []byte(key), value); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// Rollback 回滚事务
func (tx *MemoryTransaction) Rollback(ctx context.Context) error {
	// 清空事务中的操作
	tx.writes = make(map[string]map[string]map[string][]byte)
	tx.deletes = make(map[string]map[string]map[string]bool)
	return nil
}

// MemoryIterator 内存引擎迭代器
type MemoryIterator struct {
	keys   []string
	values [][]byte
	pos    int
	err    error
}

// Scan 扫描数据
func (e *MemoryEngine) Scan(ctx context.Context, dbName, tableName string, startKey, endKey []byte) (Iterator, error) {
	e.mu.RLock()
	db, exists := e.databases[dbName]
	if !exists {
		e.mu.RUnlock()
		return nil, ErrDBNotFound
	}

	table, exists := db[tableName]
	e.mu.RUnlock()

	if !exists {
		return nil, ErrTableNotFound
	}

	table.mu.RLock()
	defer table.mu.RUnlock()

	startKeyStr := string(startKey)
	endKeyStr := string(endKey)

	// 收集所有在范围内的键和值
	keys := make([]string, 0)
	values := make([][]byte, 0)

	for k, v := range table.data {
		// 如果startKey为空，则从头开始
		// 如果endKey为空，则到结尾结束
		if (len(startKey) == 0 || k >= startKeyStr) && (len(endKey) == 0 || k < endKeyStr) {
			keys = append(keys, k)
			values = append(values, v)
		}
	}

	// 按键排序（简化版，实际实现可能需要更复杂的排序逻辑）
	// 这里为了简单，不做实际排序

	return &MemoryIterator{
		keys:   keys,
		values: values,
		pos:    -1,
	}, nil
}

// Next 移动到下一条记录
func (it *MemoryIterator) Next() bool {
	if it.err != nil || it.pos >= len(it.keys)-1 {
		return false
	}
	it.pos++
	return true
}

// Key 获取当前记录的键
func (it *MemoryIterator) Key() []byte {
	if it.pos < 0 || it.pos >= len(it.keys) {
		return nil
	}
	return []byte(it.keys[it.pos])
}

// Value 获取当前记录的值
func (it *MemoryIterator) Value() []byte {
	if it.pos < 0 || it.pos >= len(it.values) {
		return nil
	}
	return it.values[it.pos]
}

// Error 获取迭代过程中的错误
func (it *MemoryIterator) Error() error {
	return it.err
}

// Close 关闭迭代器
func (it *MemoryIterator) Close() error {
	it.keys = nil
	it.values = nil
	return nil
}

// ListDBs 获取所有数据库
func (e *MemoryEngine) ListDBs(ctx context.Context) ([]string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	dbs := make([]string, 0, len(e.databases))
	for dbName := range e.databases {
		dbs = append(dbs, dbName)
	}
	return dbs, nil
}

// ListTables 获取指定数据库中的所有表
func (e *MemoryEngine) ListTables(ctx context.Context, dbName string) ([]string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	db, exists := e.databases[dbName]
	if !exists {
		return nil, ErrDBNotFound
	}

	tables := make([]string, 0, len(db))
	for tableName := range db {
		tables = append(tables, tableName)
	}
	return tables, nil
}

// GetTableSchema 获取表结构
func (e *MemoryEngine) GetTableSchema(ctx context.Context, dbName, tableName string) (*TableSchema, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	schemas, exists := e.schemas[dbName]
	if !exists {
		return nil, ErrDBNotFound
	}

	schema, exists := schemas[tableName]
	if !exists {
		return nil, ErrTableNotFound
	}

	return schema, nil
}

// Batch 批量操作
func (e *MemoryEngine) Batch(ctx context.Context, ops []BatchOperation) error {
	// 开始一个事务来处理批量操作
	tx, err := e.BeginTx(ctx)
	if err != nil {
		return err
	}

	// 使用事务的批量写入功能
	if err := tx.BatchWrite(ctx, ops); err != nil {
		tx.Rollback(ctx)
		return err
	}

	// 提交事务
	return tx.Commit(ctx)
}

// Stats 获取引擎状态统计信息
func (e *MemoryEngine) Stats(ctx context.Context) (*EngineStats, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 计算总行数和内存使用量（估计值）
	var keyCount int64
	var memoryUsage int64

	for _, db := range e.databases {
		for _, table := range db {
			table.mu.RLock()
			tableSize := len(table.data)
			keyCount += int64(tableSize)

			// 估计内存使用量
			for k, v := range table.data {
				memoryUsage += int64(len(k) + len(v))
			}
			table.mu.RUnlock()
		}
	}

	// 创建状态信息
	stats := &EngineStats{
		EngineType:       "memory",
		DBCount:          len(e.databases),
		TableCount:       0, // 将在下面计算
		KeyCount:         keyCount,
		DiskUsage:        0, // 内存引擎不使用磁盘
		MemoryUsage:      memoryUsage,
		ReadOps:          0, // 内存引擎不跟踪这些统计信息
		WriteOps:         0,
		ReadLatency:      0,
		WriteLatency:     0,
		CacheHitRatio:    1.0, // 内存引擎总是"命中"
		CompactionStatus: "N/A",
		UpTime:           0,
		AdditionalInfo:   make(map[string]string),
	}

	// 计算总表数
	for _, db := range e.databases {
		stats.TableCount += len(db)
	}

	return stats, nil
}

// Snapshot 创建快照（内存引擎不支持）
func (e *MemoryEngine) Snapshot(ctx context.Context) (EngineSnapshot, error) {
	return nil, ErrFeatureNotSupported
}

// Compact 压缩存储空间（内存引擎不支持）
func (e *MemoryEngine) Compact(ctx context.Context) error {
	return nil // 内存引擎不需要压缩
}

// Backup 备份数据
func (e *MemoryEngine) Backup(ctx context.Context, writer io.Writer) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 序列化整个内存数据库
	data := struct {
		Databases map[string]map[string]*MemoryTable
		Schemas   map[string]map[string]*TableSchema
	}{
		Databases: e.databases,
		Schemas:   e.schemas,
	}

	// 使用gob编码
	enc := gob.NewEncoder(writer)
	return enc.Encode(data)
}

// Restore 从备份恢复数据
func (e *MemoryEngine) Restore(ctx context.Context, reader io.Reader) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 清空现有数据
	e.databases = make(map[string]map[string]*MemoryTable)
	e.schemas = make(map[string]map[string]*TableSchema)

	// 解码备份数据
	data := struct {
		Databases map[string]map[string]*MemoryTable
		Schemas   map[string]map[string]*TableSchema
	}{}

	dec := gob.NewDecoder(reader)
	if err := dec.Decode(&data); err != nil {
		return err
	}

	// 恢复数据
	e.databases = data.Databases
	e.schemas = data.Schemas

	return nil
}

// Flush 将内存中的数据刷新到持久化存储（内存引擎不支持）
func (e *MemoryEngine) Flush(ctx context.Context) error {
	return nil // 内存引擎不需要刷盘
}

// BatchWrite 批量写入操作
func (tx *MemoryTransaction) BatchWrite(ctx context.Context, ops []BatchOperation) error {
	for _, op := range ops {
		switch op.Type {
		case BatchPut:
			if err := tx.Put(ctx, op.DBName, op.TableName, op.Key, op.Value); err != nil {
				return err
			}
		case BatchDelete:
			if err := tx.Delete(ctx, op.DBName, op.TableName, op.Key); err != nil && err != ErrKeyNotFound {
				return err
			}
		default:
			return fmt.Errorf("不支持的批量操作类型: %v", op.Type)
		}
	}
	return nil
}

// Scan 在事务中扫描数据
func (tx *MemoryTransaction) Scan(ctx context.Context, dbName, tableName string, startKey, endKey []byte) (Iterator, error) {
	// 使用基础引擎的Scan方法，但确保能够看到事务中的修改
	// 先获取基础引擎的数据视图
	keys, values, err := tx.getTableSnapshot(ctx, dbName, tableName, startKey, endKey)
	if err != nil {
		return nil, err
	}

	return &MemoryIterator{
		keys:   keys,
		values: values,
		pos:    -1,
	}, nil
}

// getTableSnapshot 获取表的快照，包括事务中的修改
func (tx *MemoryTransaction) getTableSnapshot(ctx context.Context, dbName, tableName string, startKey, endKey []byte) ([]string, [][]byte, error) {
	// 从引擎中获取当前表数据
	e := tx.engine
	e.mu.RLock()
	db, exists := e.databases[dbName]
	if !exists {
		e.mu.RUnlock()
		return nil, nil, ErrDBNotFound
	}

	table, exists := db[tableName]
	if !exists {
		e.mu.RUnlock()
		return nil, nil, ErrTableNotFound
	}

	// 复制表数据
	table.mu.RLock()
	dataMap := make(map[string][]byte)
	for k, v := range table.data {
		dataMap[k] = v
	}
	table.mu.RUnlock()
	e.mu.RUnlock()

	// 应用事务中的修改
	if writes, ok := tx.writes[dbName]; ok {
		if tableWrites, ok := writes[tableName]; ok {
			for k, v := range tableWrites {
				dataMap[k] = v
			}
		}
	}

	// 应用事务中的删除
	if deletes, ok := tx.deletes[dbName]; ok {
		if tableDeletes, ok := deletes[tableName]; ok {
			for k, del := range tableDeletes {
				if del {
					delete(dataMap, k)
				}
			}
		}
	}

	// 提取所有键并排序
	keys := make([]string, 0, len(dataMap))
	for k := range dataMap {
		// 如果有范围限制，检查键是否在范围内
		if startKey != nil && strings.Compare(k, string(startKey)) < 0 {
			continue
		}
		if endKey != nil && strings.Compare(k, string(endKey)) >= 0 {
			continue
		}
		keys = append(keys, k)
	}

	// 排序键
	sort.Strings(keys)

	// 提取值
	values := make([][]byte, len(keys))
	for i, k := range keys {
		values[i] = dataMap[k]
	}

	return keys, values, nil
}

// Seek 查找特定键或其后继
func (it *MemoryIterator) Seek(key []byte) bool {
	if it.err != nil {
		return false
	}

	keyStr := string(key)
	for i, k := range it.keys {
		if strings.Compare(k, keyStr) >= 0 {
			it.pos = i
			return true
		}
	}

	// 如果没有找到大于等于key的键，将位置设置到最后
	it.pos = len(it.keys)
	return false
}

// Valid 检查迭代器是否有效
func (it *MemoryIterator) Valid() bool {
	return it.err == nil && it.pos >= 0 && it.pos < len(it.keys)
}
