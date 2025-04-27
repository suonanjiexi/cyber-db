package storage

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
	"unicode/utf8"

	// 高性能LRU缓存
	lru "github.com/hashicorp/golang-lru/v2"
)

// NewDB 创建一个新的数据库实例
func NewDB(path string, persistent bool) *DB {
	// 增大缓存容量，提高缓存命中率
	cache, _ := lru.New[string, []byte](100000)
	// 创建查询结果缓存
	resultCache, _ := lru.New[string, []byte](10000)

	// 创建分片锁，提高并发性能
	shardCount := 32 // 使用32个分片锁
	shardMutexes := make([]sync.RWMutex, shardCount)

	db := &DB{
		path:          path,
		isOpen:        false,
		data:          make(map[string][]byte),
		cache:         cache,
		resultCache:   resultCache,
		persistent:    persistent,
		counters:      make(map[string]int),
		charset:       "utf8mb4",
		batchSize:     10000,                                  // 增大批量写入大小
		batchBuffer:   make([]writeOp, 0, 10000),              // 预分配容量
		flushTicker:   time.NewTicker(500 * time.Millisecond), // 更频繁地刷新
		flushDone:     make(chan struct{}),
		preReadSize:   2 * 1024 * 1024, // 增大预读取大小
		preReadBuffer: &sync.Map{},
		writeBuffer:   bytes.NewBuffer(make([]byte, 0, 1024*1024)), // 预分配容量
		compression:   true,
		shardCount:    shardCount,
		shardMutexes:  shardMutexes,
		memoryPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 4096) // 增大缓冲区大小
			},
		},
		startTime: time.Now(),
	}
	// 创建索引管理器
	db.indexManager = NewIndexManager(db)

	// 启动后台刷新goroutine
	go db.backgroundFlush()

	return db
}

// GetNextID 获取指定表的下一个自增ID
func (db *DB) GetNextID(table string) int {
	db.counterMutex.Lock()
	defer db.counterMutex.Unlock()

	// 获取当前计数器值并增加
	currentID := db.counters[table]
	currentID++

	// 更新计数器
	db.counters[table] = currentID

	return currentID
}

// Open 打开数据库
func (db *DB) Open() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.isOpen {
		return nil
	}

	// 如果是持久化数据库，尝试从文件加载数据
	if db.persistent {
		// 确保使用的是系统无关的路径格式
		db.path = filepath.Clean(db.path)

		// 确保目录存在
		dir := filepath.Dir(db.path)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}

		// 检查目录是否存在及有访问权限
		if _, err := os.Stat(dir); err != nil {
			return fmt.Errorf("无法访问数据目录 %s: %w", dir, err)
		}

		// 尝试读取现有数据文件
		if fileInfo, err := os.Stat(db.path); err == nil && !fileInfo.IsDir() {
			// 确保不是目录，而是文件
			file, err := os.OpenFile(db.path, os.O_RDONLY, 0644)
			if err != nil {
				return fmt.Errorf("failed to open database file: %w", err)
			}
			defer file.Close()

			// 使用gob解码数据
			decoder := gob.NewDecoder(file)
			if err := decoder.Decode(&db.data); err != nil {
				return fmt.Errorf("failed to decode database data: %w", err)
			}

			// 预热缓存，提高初始性能
			count := 0
			for k, v := range db.data {
				if count < 10000 { // 限制预热数量
					db.cache.Add(k, v)
					count++
				} else {
					break
				}
			}
		} else if !os.IsNotExist(err) && err != nil {
			// 如果是存在但有其他错误
			return fmt.Errorf("读取数据文件状态失败: %w", err)
		}
		// 如果文件不存在，会在第一次保存时创建
	}

	db.isOpen = true

	// 加载索引
	if err := db.indexManager.LoadIndexes(); err != nil {
		return fmt.Errorf("failed to load indexes: %w", err)
	}

	return nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if !db.isOpen {
		return nil
	}

	// 停止后台刷新goroutine
	if db.flushTicker != nil {
		db.flushTicker.Stop()
		close(db.flushDone)
	}

	// 刷新所有未写入的数据
	if err := db.flushBatch(); err != nil {
		return err
	}

	// 如果是持久化数据库，将数据写入文件
	if db.persistent {
		if err := db.persistToDisk(); err != nil {
			return err
		}
	}

	// 清理资源
	db.cache = nil
	db.resultCache = nil
	db.preReadBuffer = nil

	db.isOpen = false
	return nil
}

// Get 获取键对应的值
func (db *DB) Get(key string) ([]byte, error) {
	startTime := time.Now()

	// 首先检查查询缓存
	if value, ok := db.resultCache.Get(key); ok {
		// 更新统计信息
		db.cacheHits++
		db.getOps++
		return value, nil
	}

	// 获取分片锁
	shardID := db.getShardID(key)
	db.shardMutexes[shardID].RLock()
	defer db.shardMutexes[shardID].RUnlock()

	if !db.isOpen {
		return nil, ErrDBNotOpen
	}

	// 检查LRU缓存
	if value, ok := db.cache.Get(key); ok {
		// 更新统计信息
		db.cacheHits++
		db.getOps++

		// 将结果添加到查询缓存
		db.resultCache.Add(key, value)

		// 更新平均查询时间
		elapsed := time.Since(startTime)
		db.avgGetTime = time.Duration((int64(db.avgGetTime)*int64(db.getOps-1) + int64(elapsed)) / int64(db.getOps))

		return value, nil
	}

	// 从数据存储中获取
	value, ok := db.data[key]
	if !ok {
		// 更新统计信息
		db.cacheMisses++
		db.getOps++
		return nil, ErrKeyNotFound
	}

	// 更新缓存
	db.cache.Add(key, value)
	db.resultCache.Add(key, value)

	// 更新统计信息
	db.cacheMisses++
	db.getOps++

	// 更新平均查询时间
	elapsed := time.Since(startTime)
	db.avgGetTime = time.Duration((int64(db.avgGetTime)*int64(db.getOps-1) + int64(elapsed)) / int64(db.getOps))

	return value, nil
}

// getShardID 计算键的分片ID
func (db *DB) getShardID(key string) int {
	hash := 0
	for i := 0; i < len(key); i++ {
		hash = 31*hash + int(key[i])
	}
	return (hash & 0x7FFFFFFF) % db.shardCount
}

// preReadNearbyKeys 预读取附近的键值对
func (db *DB) preReadNearbyKeys(key string) {
	if db.preReadBuffer == nil || db.preReadSize <= 0 {
		return
	}

	db.mutex.RLock()
	defer db.mutex.RUnlock()

	if !db.isOpen {
		return
	}

	// 简单实现：预读取字典序相近的键
	count := 0
	size := 0
	for k, v := range db.data {
		// 跳过已经读取的键
		if k == key {
			continue
		}

		// 只预读取字典序相近的键
		if len(k) > 0 && len(key) > 0 && k[0] == key[0] {
			db.preReadBuffer.Store(k, v)
			count++
			size += len(v)

			// 限制预读数量和大小
			if count >= 10 || size >= db.preReadSize {
				break
			}
		}
	}
}

// Put 存储键值对
func (db *DB) Put(key string, value []byte) error {
	startTime := time.Now()

	// 验证字符串是否为有效的UTF-8MB4编码
	if !utf8.Valid(value) {
		return errors.New("invalid UTF-8 encoding")
	}

	// 使用批量写入缓冲区
	db.batchMutex.Lock()
	defer db.batchMutex.Unlock()

	if !db.isOpen {
		return ErrDBNotOpen
	}

	// 存储值的副本，避免外部修改影响内部数据
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	// 添加到批量写入缓冲区
	db.batchBuffer = append(db.batchBuffer, writeOp{key: key, value: valueCopy})

	// 更新修改时间
	db.modificationTimes[key] = time.Now()

	// 更新统计信息
	db.putOps++
	elapsed := time.Since(startTime)
	db.avgPutTime = time.Duration((int64(db.avgPutTime)*int64(db.putOps-1) + int64(elapsed)) / int64(db.putOps))

	// 如果缓冲区达到批量写入大小，执行批量写入
	if len(db.batchBuffer) >= db.batchSize {
		return db.flushBatch()
	}

	return nil
}

// Delete 删除键值对
func (db *DB) Delete(key string) error {
	// 获取分片锁
	shardID := db.getShardID(key)
	db.shardMutexes[shardID].Lock()
	defer db.shardMutexes[shardID].Unlock()

	if !db.isOpen {
		return ErrDBNotOpen
	}

	// 从数据存储中删除
	delete(db.data, key)

	// 从缓存中删除
	db.cache.Remove(key)
	db.resultCache.Remove(key)

	// 更新统计信息
	db.deleteOps++

	return nil
}

// Has 检查键是否存在
func (db *DB) Has(key string) (bool, error) {
	// 获取分片锁
	shardID := db.getShardID(key)
	db.shardMutexes[shardID].RLock()
	defer db.shardMutexes[shardID].RUnlock()

	if !db.isOpen {
		return false, ErrDBNotOpen
	}

	// 首先检查缓存
	if _, ok := db.cache.Get(key); ok {
		return true, nil
	}

	// 然后检查数据存储
	_, ok := db.data[key]
	return ok, nil
}

// GetMulti 批量获取多个键的值
func (db *DB) GetMulti(keys []string) (map[string][]byte, error) {
	if !db.isOpen {
		return nil, ErrDBNotOpen
	}

	result := make(map[string][]byte, len(keys))
	var missingKeys []string

	// 首先检查缓存
	for _, key := range keys {
		if value, ok := db.cache.Get(key); ok {
			result[key] = value
			db.cacheHits++
		} else {
			missingKeys = append(missingKeys, key)
		}
	}

	// 如果所有键都在缓存中找到，直接返回
	if len(missingKeys) == 0 {
		return result, nil
	}

	// 对于缓存中没有的键，从数据存储中获取
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	for _, key := range missingKeys {
		if value, ok := db.data[key]; ok {
			result[key] = value
			// 更新缓存
			db.cache.Add(key, value)
			db.cacheMisses++
		}
	}

	db.getOps += int64(len(keys))
	return result, nil
}

// PutMulti 批量存储多个键值对
func (db *DB) PutMulti(entries map[string][]byte) error {
	if !db.isOpen {
		return ErrDBNotOpen
	}

	// 使用批量写入缓冲区
	db.batchMutex.Lock()
	defer db.batchMutex.Unlock()

	// 预分配足够的容量
	if cap(db.batchBuffer)-len(db.batchBuffer) < len(entries) {
		newBuffer := make([]writeOp, len(db.batchBuffer), len(db.batchBuffer)+len(entries))
		copy(newBuffer, db.batchBuffer)
		db.batchBuffer = newBuffer
	}

	// 添加所有键值对到批量写入缓冲区
	for key, value := range entries {
		// 验证字符串是否为有效的UTF-8编码
		if !utf8.Valid(value) {
			return fmt.Errorf("invalid UTF-8 encoding for key: %s", key)
		}

		// 存储值的副本
		valueCopy := make([]byte, len(value))
		copy(valueCopy, value)

		db.batchBuffer = append(db.batchBuffer, writeOp{key: key, value: valueCopy})
	}

	// 更新统计信息
	db.putOps += int64(len(entries))

	// 如果缓冲区达到批量写入大小，执行批量写入
	if len(db.batchBuffer) >= db.batchSize {
		return db.flushBatch()
	}

	return nil
}

// DeleteMulti 批量删除多个键
func (db *DB) DeleteMulti(keys []string) error {
	if !db.isOpen {
		return ErrDBNotOpen
	}

	db.mutex.Lock()
	defer db.mutex.Unlock()

	for _, key := range keys {
		// 从数据存储中删除
		delete(db.data, key)

		// 从缓存中删除
		db.cache.Remove(key)
		db.resultCache.Remove(key)
	}

	// 更新统计信息
	db.deleteOps += int64(len(keys))

	return nil
}

// GetKeys 获取所有键
func (db *DB) GetKeys() ([]string, error) {
	if !db.isOpen {
		return nil, ErrDBNotOpen
	}

	db.mutex.RLock()
	defer db.mutex.RUnlock()

	keys := make([]string, 0, len(db.data))
	for k := range db.data {
		keys = append(keys, k)
	}

	return keys, nil
}

// GetKeysWithPrefix 获取具有指定前缀的所有键
func (db *DB) GetKeysWithPrefix(prefix string) ([]string, error) {
	if !db.isOpen {
		return nil, ErrDBNotOpen
	}

	db.mutex.RLock()
	defer db.mutex.RUnlock()

	keys := make([]string, 0)
	for k := range db.data {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k)
		}
	}

	return keys, nil
}

// Count 获取数据库中键值对的数量
func (db *DB) Count() (int, error) {
	if !db.isOpen {
		return 0, ErrDBNotOpen
	}

	db.mutex.RLock()
	defer db.mutex.RUnlock()

	return len(db.data), nil
}
