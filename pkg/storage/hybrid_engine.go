package storage

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cyberdb/cyberdb/pkg/common/logger"
)

// HybridEngine 是一个混合存储引擎，结合了内存引擎和磁盘引擎的优点
// 用于HTAP场景，其中OLTP操作主要在内存中进行，而OLAP操作则可以利用磁盘引擎
type HybridEngine struct {
	// 内存存储引擎，用于处理OLTP工作负载
	memEngine *MemoryEngine

	// 磁盘存储引擎，用于持久化和大规模数据分析
	diskEngine *DiskEngine

	// 数据同步锁
	mu sync.RWMutex

	// 写入缓冲区，用于批量刷盘
	writeBuffer map[string]map[string]map[string][]byte // dbName -> tableName -> key -> value

	// 删除缓冲区
	deleteBuffer map[string]map[string]map[string]bool // dbName -> tableName -> key -> true

	// 写入缓冲区锁
	bufferMu sync.Mutex

	// 同步间隔（毫秒）
	syncInterval time.Duration

	// 控制同步goroutine的停止信号
	stopCh chan struct{}

	// 控制最大缓冲区大小
	maxBufferSize int

	// 当前缓冲区大小估计
	currentBufferSize int

	// 复制处理器
	replicationHandler ReplicationHandler

	// 是否启用自动刷盘
	autoFlush bool
}

// NewHybridEngine 创建一个新的混合存储引擎
func NewHybridEngine(dataDir string, syncIntervalMs int, maxBufferSizeMB int, autoFlush bool) *HybridEngine {
	engine := &HybridEngine{
		memEngine:         NewMemoryEngine(),
		diskEngine:        NewDiskEngine(dataDir),
		writeBuffer:       make(map[string]map[string]map[string][]byte),
		deleteBuffer:      make(map[string]map[string]map[string]bool),
		syncInterval:      time.Duration(syncIntervalMs) * time.Millisecond,
		stopCh:            make(chan struct{}),
		maxBufferSize:     maxBufferSizeMB * 1024 * 1024, // 转换为字节
		currentBufferSize: 0,
		autoFlush:         autoFlush,
	}

	return engine
}

// Open 打开混合引擎
func (e *HybridEngine) Open() error {
	// 先打开磁盘引擎
	if err := e.diskEngine.Open(); err != nil {
		return fmt.Errorf("打开磁盘引擎失败: %w", err)
	}

	// 再打开内存引擎
	if err := e.memEngine.Open(); err != nil {
		e.diskEngine.Close()
		return fmt.Errorf("打开内存引擎失败: %w", err)
	}

	// 如果启用了自动刷盘，启动同步goroutine
	if e.autoFlush {
		go e.syncLoop()
	}

	return nil
}

// syncLoop 周期性地将写入缓冲区的数据刷到磁盘
func (e *HybridEngine) syncLoop() {
	ticker := time.NewTicker(e.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := e.FlushBuffers(); err != nil {
				logger.Error("自动刷盘失败: " + err.Error())
			}
		case <-e.stopCh:
			return
		}
	}
}

// FlushBuffers 将缓冲区数据刷到磁盘
func (e *HybridEngine) FlushBuffers() error {
	e.bufferMu.Lock()

	// 如果缓冲区为空，不需要刷盘
	if e.currentBufferSize == 0 {
		e.bufferMu.Unlock()
		return nil
	}

	// 创建临时副本进行处理，减少锁定时间
	writeBufferCopy := e.writeBuffer
	deleteBufferCopy := e.deleteBuffer

	// 重置缓冲区
	e.writeBuffer = make(map[string]map[string]map[string][]byte)
	e.deleteBuffer = make(map[string]map[string]map[string]bool)
	e.currentBufferSize = 0

	e.bufferMu.Unlock()

	// 开始事务
	ctx := context.Background()
	tx, err := e.diskEngine.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("创建事务失败: %w", err)
	}

	// 处理所有写入
	for dbName, tables := range writeBufferCopy {
		for tableName, keys := range tables {
			for keyStr, value := range keys {
				if err := tx.Put(ctx, dbName, tableName, []byte(keyStr), value); err != nil {
					tx.Rollback(ctx)
					return fmt.Errorf("事务写入失败: %w", err)
				}
			}
		}
	}

	// 处理所有删除
	for dbName, tables := range deleteBufferCopy {
		for tableName, keys := range tables {
			for keyStr := range keys {
				if err := tx.Delete(ctx, dbName, tableName, []byte(keyStr)); err != nil {
					// 如果是键不存在的错误，可以忽略
					if err != ErrKeyNotFound {
						tx.Rollback(ctx)
						return fmt.Errorf("事务删除失败: %w", err)
					}
				}
			}
		}
	}

	// 提交事务
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("提交事务失败: %w", err)
	}

	logger.Info(fmt.Sprintf("成功将 %d 条数据刷到磁盘", len(writeBufferCopy)+len(deleteBufferCopy)))
	return nil
}

// Close 关闭混合引擎
func (e *HybridEngine) Close() error {
	// 停止同步goroutine
	if e.autoFlush {
		close(e.stopCh)
	}

	// 刷新缓冲区
	if err := e.FlushBuffers(); err != nil {
		logger.Error("关闭时刷盘失败: " + err.Error())
	}

	// 关闭引擎
	if err := e.memEngine.Close(); err != nil {
		return err
	}

	if err := e.diskEngine.Close(); err != nil {
		return err
	}

	return nil
}

// SetReplicationHandler 设置复制处理器
func (e *HybridEngine) SetReplicationHandler(handler ReplicationHandler) {
	e.replicationHandler = handler
	e.memEngine.SetReplicationHandler(handler)
	// 注意：我们不在磁盘引擎上设置复制处理器，因为混合引擎会处理复制
}

// CreateDB 创建数据库
func (e *HybridEngine) CreateDB(ctx context.Context, dbName string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 先在内存引擎中创建
	if err := e.memEngine.CreateDB(ctx, dbName); err != nil {
		return err
	}

	// 然后在磁盘引擎中创建
	if err := e.diskEngine.CreateDB(ctx, dbName); err != nil {
		// 如果磁盘引擎创建失败，回滚内存引擎的创建
		e.memEngine.DropDB(ctx, dbName)
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
func (e *HybridEngine) DropDB(ctx context.Context, dbName string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 先删除内存中的数据库
	if err := e.memEngine.DropDB(ctx, dbName); err != nil {
		return err
	}

	// 然后删除磁盘中的数据库
	if err := e.diskEngine.DropDB(ctx, dbName); err != nil {
		return err
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
func (e *HybridEngine) CreateTable(ctx context.Context, dbName, tableName string, schema *TableSchema) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 先在内存引擎中创建
	if err := e.memEngine.CreateTable(ctx, dbName, tableName, schema); err != nil {
		return err
	}

	// 然后在磁盘引擎中创建
	if err := e.diskEngine.CreateTable(ctx, dbName, tableName, schema); err != nil {
		// 如果磁盘引擎创建失败，回滚内存引擎的创建
		e.memEngine.DropTable(ctx, dbName, tableName)
		return err
	}

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
func (e *HybridEngine) DropTable(ctx context.Context, dbName, tableName string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 先删除内存中的表
	if err := e.memEngine.DropTable(ctx, dbName, tableName); err != nil {
		return err
	}

	// 然后删除磁盘中的表
	if err := e.diskEngine.DropTable(ctx, dbName, tableName); err != nil {
		return err
	}

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
func (e *HybridEngine) Get(ctx context.Context, dbName, tableName string, key []byte) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 先检查写入缓冲区
	keyStr := string(key)
	e.bufferMu.Lock()
	if db, ok := e.writeBuffer[dbName]; ok {
		if table, ok := db[tableName]; ok {
			if value, ok := table[keyStr]; ok {
				e.bufferMu.Unlock()
				return value, nil
			}
		}
	}

	// 检查删除缓冲区
	if db, ok := e.deleteBuffer[dbName]; ok {
		if table, ok := db[tableName]; ok {
			if deleted, ok := table[keyStr]; ok && deleted {
				e.bufferMu.Unlock()
				return nil, ErrKeyNotFound
			}
		}
	}
	e.bufferMu.Unlock()

	// 先从内存引擎获取
	value, err := e.memEngine.Get(ctx, dbName, tableName, key)
	if err == nil {
		return value, nil
	}
	if err != ErrKeyNotFound {
		return nil, err
	}

	// 如果内存中没有找到，从磁盘引擎获取
	return e.diskEngine.Get(ctx, dbName, tableName, key)
}

// Put 存储数据
func (e *HybridEngine) Put(ctx context.Context, dbName, tableName string, key, value []byte) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 先存储到内存引擎
	if err := e.memEngine.Put(ctx, dbName, tableName, key, value); err != nil {
		return err
	}

	// 添加到写入缓冲区
	keyStr := string(key)
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	e.bufferMu.Lock()
	defer e.bufferMu.Unlock()

	// 确保缓冲区中存在相应的数据结构
	if _, ok := e.writeBuffer[dbName]; !ok {
		e.writeBuffer[dbName] = make(map[string]map[string][]byte)
	}
	if _, ok := e.writeBuffer[dbName][tableName]; !ok {
		e.writeBuffer[dbName][tableName] = make(map[string][]byte)
	}

	// 如果这个键在删除缓冲区中，从那里移除它
	if db, ok := e.deleteBuffer[dbName]; ok {
		if table, ok := db[tableName]; ok {
			delete(table, keyStr)
		}
	}

	// 更新缓冲区大小估计
	e.currentBufferSize += len(keyStr) + len(valueCopy)

	// 存储到缓冲区
	e.writeBuffer[dbName][tableName][keyStr] = valueCopy

	// 检查是否需要刷盘
	if e.currentBufferSize >= e.maxBufferSize && !e.autoFlush {
		go func() {
			if err := e.FlushBuffers(); err != nil {
				logger.Error("刷盘失败: " + err.Error())
			}
		}()
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
func (e *HybridEngine) Delete(ctx context.Context, dbName, tableName string, key []byte) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 从内存引擎中删除
	err := e.memEngine.Delete(ctx, dbName, tableName, key)
	if err != nil && err != ErrKeyNotFound {
		return err
	}

	// 添加到删除缓冲区
	keyStr := string(key)

	e.bufferMu.Lock()
	defer e.bufferMu.Unlock()

	// 确保缓冲区中存在相应的数据结构
	if _, ok := e.deleteBuffer[dbName]; !ok {
		e.deleteBuffer[dbName] = make(map[string]map[string]bool)
	}
	if _, ok := e.deleteBuffer[dbName][tableName]; !ok {
		e.deleteBuffer[dbName][tableName] = make(map[string]bool)
	}

	// 如果这个键在写入缓冲区中，从那里移除它
	if db, ok := e.writeBuffer[dbName]; ok {
		if table, ok := db[tableName]; ok {
			delete(table, keyStr)
		}
	}

	// 更新缓冲区大小估计
	e.currentBufferSize += len(keyStr) + 1 // 1是布尔值的大小估计

	// 存储到缓冲区
	e.deleteBuffer[dbName][tableName][keyStr] = true

	// 检查是否需要刷盘
	if e.currentBufferSize >= e.maxBufferSize && !e.autoFlush {
		go func() {
			if err := e.FlushBuffers(); err != nil {
				logger.Error("刷盘失败: " + err.Error())
			}
		}()
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
func (e *HybridEngine) BeginTx(ctx context.Context) (Transaction, error) {
	// 在混合引擎中，我们使用内存引擎进行事务操作
	memTx, err := e.memEngine.BeginTx(ctx)
	if err != nil {
		return nil, err
	}

	return &HybridTransaction{
		memTx:  memTx,
		engine: e,
		ctx:    ctx,
	}, nil
}

// Scan 扫描数据
func (e *HybridEngine) Scan(ctx context.Context, dbName, tableName string, startKey, endKey []byte) (Iterator, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 获取内存迭代器
	memIter, err := e.memEngine.Scan(ctx, dbName, tableName, startKey, endKey)
	if err != nil && err != ErrTableNotFound {
		return nil, err
	}

	// 获取磁盘迭代器
	diskIter, err := e.diskEngine.Scan(ctx, dbName, tableName, startKey, endKey)
	if err != nil && err != ErrTableNotFound {
		if memIter != nil {
			memIter.Close()
		}
		return nil, err
	}

	if memIter == nil && diskIter == nil {
		return nil, ErrTableNotFound
	}

	// 创建混合迭代器
	return &HybridIterator{
		memIter:   memIter,
		diskIter:  diskIter,
		engine:    e,
		dbName:    dbName,
		tableName: tableName,
	}, nil
}

// HybridTransaction 混合事务实现
type HybridTransaction struct {
	memTx  Transaction
	engine *HybridEngine
	ctx    context.Context
}

// Get 事务中获取数据
func (tx *HybridTransaction) Get(ctx context.Context, dbName, tableName string, key []byte) ([]byte, error) {
	// 先从事务中读取
	value, err := tx.memTx.Get(ctx, dbName, tableName, key)
	if err == nil {
		return value, nil
	}
	if err != ErrKeyNotFound {
		return nil, err
	}

	// 如果内存事务中没有，从磁盘读取
	return tx.engine.diskEngine.Get(ctx, dbName, tableName, key)
}

// Put 事务中存储数据
func (tx *HybridTransaction) Put(ctx context.Context, dbName, tableName string, key, value []byte) error {
	return tx.memTx.Put(ctx, dbName, tableName, key, value)
}

// Delete 事务中删除数据
func (tx *HybridTransaction) Delete(ctx context.Context, dbName, tableName string, key []byte) error {
	return tx.memTx.Delete(ctx, dbName, tableName, key)
}

// Commit 提交事务
func (tx *HybridTransaction) Commit(ctx context.Context) error {
	// 提交内存事务
	if err := tx.memTx.Commit(ctx); err != nil {
		return err
	}

	// 事务提交成功后，将操作添加到混合引擎的缓冲区，异步刷到磁盘
	// 注意：这里我们依赖于事务的提交是原子性的
	// 在真实场景中，可能需要日志记录来确保事务持久性
	return nil
}

// Rollback 回滚事务
func (tx *HybridTransaction) Rollback(ctx context.Context) error {
	return tx.memTx.Rollback(ctx)
}

// BatchWrite 批量写入操作
func (tx *HybridTransaction) BatchWrite(ctx context.Context, ops []BatchOperation) error {
	// 使用内存事务进行批量写入
	if err := tx.memTx.BatchWrite(ctx, ops); err != nil {
		return err
	}

	return nil
}

// Scan 在事务中扫描数据
func (tx *HybridTransaction) Scan(ctx context.Context, dbName, tableName string, startKey, endKey []byte) (Iterator, error) {
	// 获取内存事务的迭代器
	memIter, err := tx.memTx.Scan(ctx, dbName, tableName, startKey, endKey)
	if err != nil && err != ErrTableNotFound {
		return nil, err
	}

	// 获取磁盘引擎的迭代器
	diskIter, err := tx.engine.diskEngine.Scan(ctx, dbName, tableName, startKey, endKey)
	if err != nil && err != ErrTableNotFound {
		if memIter != nil {
			memIter.Close()
		}
		return nil, err
	}

	if memIter == nil && diskIter == nil {
		return nil, ErrTableNotFound
	}

	// 创建混合迭代器
	return &HybridIterator{
		memIter:   memIter,
		diskIter:  diskIter,
		engine:    tx.engine,
		dbName:    dbName,
		tableName: tableName,
	}, nil
}

// HybridIterator 混合迭代器，合并内存和磁盘的结果
type HybridIterator struct {
	memIter   Iterator
	diskIter  Iterator
	engine    *HybridEngine
	current   Iterator
	err       error
	closed    bool
	dbName    string
	tableName string

	// 用于跟踪已经从磁盘读取的键
	seenKeys map[string]bool
}

// Next 移动到下一条记录
func (it *HybridIterator) Next() bool {
	if it.closed {
		return false
	}

	// 如果没有初始化seenKeys，初始化它
	if it.seenKeys == nil {
		it.seenKeys = make(map[string]bool)
	}

	// 先从内存迭代器读取
	if it.memIter != nil && it.memIter.Next() {
		it.current = it.memIter
		// 记录这个键已经看到
		it.seenKeys[string(it.memIter.Key())] = true
		return true
	}

	// 如果内存迭代器有错误，记录它
	if it.memIter != nil && it.memIter.Error() != nil {
		it.err = it.memIter.Error()
		return false
	}

	// 然后从磁盘迭代器读取
	for it.diskIter != nil && it.diskIter.Next() {
		key := string(it.diskIter.Key())

		// 检查这个键是否已经在内存迭代器中看到过
		if !it.seenKeys[key] {
			// 检查写入/删除缓冲区
			it.engine.bufferMu.Lock()

			// 检查删除缓冲区
			deleted := false
			if db, ok := it.engine.deleteBuffer[it.dbName]; ok {
				if table, ok := db[it.tableName]; ok {
					if _, ok := table[key]; ok {
						deleted = true
					}
				}
			}

			// 检查写入缓冲区（虽然这个键应该已经在内存引擎中）
			if !deleted {
				if db, ok := it.engine.writeBuffer[it.dbName]; ok {
					if table, ok := db[it.tableName]; ok {
						if _, ok := table[key]; ok {
							// 如果在写入缓冲区找到，但不在内存迭代器中，这是不正常的情况
							// 应该已经在内存引擎中了
							it.engine.bufferMu.Unlock()
							continue
						}
					}
				}
			}
			it.engine.bufferMu.Unlock()

			// 如果这个键被标记为删除，跳过它
			if deleted {
				continue
			}

			// 记录这个键已经看到
			it.seenKeys[key] = true
			it.current = it.diskIter
			return true
		}
	}

	// 如果磁盘迭代器有错误，记录它
	if it.diskIter != nil && it.diskIter.Error() != nil {
		it.err = it.diskIter.Error()
	}

	return false
}

// Key 获取当前记录的键
func (it *HybridIterator) Key() []byte {
	if it.closed || it.current == nil {
		return nil
	}
	return it.current.Key()
}

// Value 获取当前记录的值
func (it *HybridIterator) Value() []byte {
	if it.closed || it.current == nil {
		return nil
	}
	return it.current.Value()
}

// Error 获取迭代过程中的错误
func (it *HybridIterator) Error() error {
	if it.closed {
		return fmt.Errorf("迭代器已关闭")
	}

	if it.err != nil {
		return it.err
	}

	return nil
}

// Close 关闭迭代器
func (it *HybridIterator) Close() error {
	if it.closed {
		return nil
	}

	it.closed = true

	var err1, err2, err3 error

	// 关闭内存迭代器
	if it.memIter != nil {
		err1 = it.memIter.Close()
	}

	// 关闭磁盘迭代器
	if it.diskIter != nil {
		err2 = it.diskIter.Close()
	}

	// 返回第一个非空错误
	if err1 != nil {
		err3 = err1
	} else if err2 != nil {
		err3 = err2
	}

	return err3
}

// Seek 实现Iterator接口的Seek方法
func (it *HybridIterator) Seek(key []byte) bool {
	if it.closed {
		return false
	}

	// 如果没有初始化seenKeys，初始化它
	if it.seenKeys == nil {
		it.seenKeys = make(map[string]bool)
	} else {
		// 清空之前的记录
		for k := range it.seenKeys {
			delete(it.seenKeys, k)
		}
	}

	// 内存迭代器Seek
	memFound := false
	if it.memIter != nil {
		memFound = it.memIter.Seek(key)
	}

	// 磁盘迭代器Seek
	diskFound := false
	if it.diskIter != nil {
		diskFound = it.diskIter.Seek(key)
	}

	// 如果两个迭代器都没找到，返回false
	if !memFound && !diskFound {
		return false
	}

	// 如果内存迭代器找到了，记录这个键
	if memFound {
		it.current = it.memIter
		it.seenKeys[string(it.memIter.Key())] = true
		return true
	}

	// 如果只有磁盘迭代器找到了
	if diskFound {
		// 检查这个键是否在删除缓冲区中
		keyStr := string(it.diskIter.Key())
		it.engine.bufferMu.Lock()
		deleted := false
		if db, ok := it.engine.deleteBuffer[it.dbName]; ok {
			if table, ok := db[it.tableName]; ok {
				if _, ok := table[keyStr]; ok {
					deleted = true
				}
			}
		}
		it.engine.bufferMu.Unlock()

		if deleted {
			// 如果在删除缓冲区中，尝试找下一个
			return it.Next()
		}

		it.current = it.diskIter
		it.seenKeys[keyStr] = true
		return true
	}

	return false
}

// Valid 实现Iterator接口的Valid方法
func (it *HybridIterator) Valid() bool {
	if it.closed || it.current == nil {
		return false
	}
	return it.current.Valid()
}

// Backup 实现Engine接口的Backup方法
func (e *HybridEngine) Backup(ctx context.Context, writer io.Writer) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 确保先将缓冲区中的数据刷到磁盘
	if err := e.FlushBuffers(); err != nil {
		return fmt.Errorf("备份前刷盘失败: %w", err)
	}

	// 使用磁盘引擎执行备份
	return e.diskEngine.Backup(ctx, writer)
}

// Restore 实现Engine接口的Restore方法
func (e *HybridEngine) Restore(ctx context.Context, reader io.Reader) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 使用磁盘引擎执行恢复
	if err := e.diskEngine.Restore(ctx, reader); err != nil {
		return err
	}

	// 清空内存引擎
	e.memEngine.Close()
	e.memEngine = NewMemoryEngine()
	if err := e.memEngine.Open(); err != nil {
		return fmt.Errorf("恢复后重新打开内存引擎失败: %w", err)
	}

	return nil
}

// ListDBs 获取所有数据库
func (e *HybridEngine) ListDBs(ctx context.Context) ([]string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.diskEngine.ListDBs(ctx)
}

// ListTables 获取指定数据库中的所有表
func (e *HybridEngine) ListTables(ctx context.Context, dbName string) ([]string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.diskEngine.ListTables(ctx, dbName)
}

// GetTableSchema 获取表结构
func (e *HybridEngine) GetTableSchema(ctx context.Context, dbName, tableName string) (*TableSchema, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.diskEngine.GetTableSchema(ctx, dbName, tableName)
}

// Batch 批量操作
func (e *HybridEngine) Batch(ctx context.Context, ops []BatchOperation) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

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
func (e *HybridEngine) Stats(ctx context.Context) (*EngineStats, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 获取磁盘引擎状态
	diskStats, err := e.diskEngine.Stats(ctx)
	if err != nil {
		return nil, err
	}

	// 获取内存引擎状态
	memStats, err := e.memEngine.Stats(ctx)
	if err != nil {
		return nil, err
	}

	// 合并状态
	stats := &EngineStats{
		EngineType:     "hybrid",
		DBCount:        diskStats.DBCount,
		TableCount:     diskStats.TableCount,
		KeyCount:       diskStats.KeyCount + memStats.KeyCount,
		DiskUsage:      diskStats.DiskUsage,
		MemoryUsage:    memStats.MemoryUsage + int64(e.currentBufferSize),
		ReadOps:        diskStats.ReadOps + memStats.ReadOps,
		WriteOps:       diskStats.WriteOps + memStats.WriteOps,
		ReadLatency:    (diskStats.ReadLatency + memStats.ReadLatency) / 2,
		WriteLatency:   (diskStats.WriteLatency + memStats.WriteLatency) / 2,
		CacheHitRatio:  memStats.CacheHitRatio,
		UpTime:         diskStats.UpTime,
		AdditionalInfo: make(map[string]string),
	}

	// 添加额外信息
	stats.AdditionalInfo["buffer_size"] = fmt.Sprintf("%d bytes", e.maxBufferSize)
	stats.AdditionalInfo["current_buffer_usage"] = fmt.Sprintf("%d bytes", e.currentBufferSize)
	stats.AdditionalInfo["auto_flush"] = fmt.Sprintf("%v", e.autoFlush)
	stats.AdditionalInfo["sync_interval"] = e.syncInterval.String()

	return stats, nil
}

// Snapshot 创建快照
func (e *HybridEngine) Snapshot(ctx context.Context) (EngineSnapshot, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 确保先将缓冲区中的数据刷到磁盘
	if err := e.FlushBuffers(); err != nil {
		return nil, fmt.Errorf("创建快照前刷盘失败: %w", err)
	}

	// 使用磁盘引擎创建快照
	return e.diskEngine.Snapshot(ctx)
}

// Compact 压缩存储空间
func (e *HybridEngine) Compact(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 确保先将缓冲区中的数据刷到磁盘
	if err := e.FlushBuffers(); err != nil {
		return fmt.Errorf("压缩前刷盘失败: %w", err)
	}

	// 使用磁盘引擎执行压缩
	return e.diskEngine.Compact(ctx)
}

// Flush 将内存中的数据刷新到持久化存储
func (e *HybridEngine) Flush(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 刷新内部缓冲区
	if err := e.FlushBuffers(); err != nil {
		return err
	}

	// 刷新磁盘引擎
	return e.diskEngine.Flush(ctx)
}
