package storage

import (
	"bufio"
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"time"
	"unicode/utf8"
)

// flushBatch 执行批量写入
func (db *DB) flushBatch() error {
	if len(db.batchBuffer) == 0 {
		return nil
	}

	// 使用写缓冲区
	db.writeBuffer.Reset()

	// 压缩数据
	var compressedWriter *gzip.Writer
	if db.compression {
		compressedWriter = gzip.NewWriter(db.writeBuffer)
	}

	// 批量处理写操作
	for _, op := range db.batchBuffer {
		// 如果启用了压缩
		if db.compression {
			if _, err := compressedWriter.Write(op.value); err != nil {
				return err
			}
		}

		// 更新内存数据
		db.data[op.key] = op.value

		// 更新缓存
		db.cache.Add(op.key, op.value)
	}

	// 如果启用了压缩，确保所有数据都被写入
	if db.compression {
		if err := compressedWriter.Close(); err != nil {
			return err
		}
	}

	// 清空批处理缓冲区
	db.batchBuffer = db.batchBuffer[:0]

	// 更新最后刷新时间
	db.lastFlushTime = time.Now()

	// 如果是持久化存储，写入文件
	if db.persistent {
		return db.persistToDisk()
	}

	return nil
}

// backgroundFlush 后台定期刷新批量写入缓冲区
func (db *DB) backgroundFlush() {
	for {
		select {
		case <-db.flushTicker.C:
			// 获取批量写入锁
			db.batchMutex.Lock()

			// 如果缓冲区有数据，执行刷新
			if len(db.batchBuffer) > 0 {
				db.flushBatch()
			}

			db.batchMutex.Unlock()
		case <-db.flushDone:
			return // 退出goroutine
		}
	}
}

// persistToDisk 将数据持久化到磁盘
func (db *DB) persistToDisk() error {
	// 创建临时文件
	tmpFile := db.path + ".tmp"
	dir := filepath.Dir(db.path)

	// 确保使用系统无关的路径格式
	tmpFile = filepath.Clean(tmpFile)

	// 确保目录存在
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// 创建临时文件
	file, err := os.OpenFile(tmpFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer file.Close()

	// 使用缓冲写入提高性能
	bufWriter := bufio.NewWriterSize(file, 4*1024*1024) // 4MB 缓冲区

	// 使用gob编码器
	var encoder *gob.Encoder

	// 如果启用了压缩
	if db.compression {
		compressedWriter, err := gzip.NewWriterLevel(bufWriter, gzip.BestSpeed) // 使用最快的压缩级别
		if err != nil {
			return fmt.Errorf("failed to create compressed writer: %w", err)
		}
		defer compressedWriter.Close()
		encoder = gob.NewEncoder(compressedWriter)
	} else {
		encoder = gob.NewEncoder(bufWriter)
	}

	// 编码数据
	if err := encoder.Encode(db.data); err != nil {
		return fmt.Errorf("failed to encode database data: %w", err)
	}

	// 刷新缓冲区
	if err := bufWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}

	// 确保数据写入磁盘
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	// 关闭文件
	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	// 替换原文件 - 在Windows上，可能需要先删除目标文件
	if _, err := os.Stat(db.path); err == nil {
		// 文件存在，尝试删除
		if err := os.Remove(db.path); err != nil {
			return fmt.Errorf("failed to remove existing file: %w", err)
		}
	}

	// 重命名临时文件
	if err := os.Rename(tmpFile, db.path); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// BatchPut 批量写入键值对
func (db *DB) BatchPut(entries map[string][]byte) error {
	startTime := time.Now()

	// 使用批量写入缓冲区
	db.batchMutex.Lock()
	defer db.batchMutex.Unlock()

	if !db.isOpen {
		return ErrDBNotOpen
	}

	// 预分配足够的空间
	if cap(db.batchBuffer)-len(db.batchBuffer) < len(entries) {
		newBuffer := make([]writeOp, len(db.batchBuffer), len(db.batchBuffer)+len(entries))
		copy(newBuffer, db.batchBuffer)
		db.batchBuffer = newBuffer
	}

	// 添加所有条目到批量写入缓冲区
	for key, value := range entries {
		// 验证字符串是否为有效的UTF-8编码
		if !utf8.Valid(value) {
			return fmt.Errorf("invalid UTF-8 encoding for key: %s", key)
		}

		// 存储值的副本，避免外部修改影响内部数据
		valueCopy := make([]byte, len(value))
		copy(valueCopy, value)

		// 添加到批量写入缓冲区
		db.batchBuffer = append(db.batchBuffer, writeOp{key: key, value: valueCopy})

		// 更新统计信息
		db.putOps++
	}

	// 计算条目数量
	entriesCount := int64(len(entries))

	// 更新平均写入时间
	elapsed := time.Since(startTime)
	db.avgPutTime = time.Duration((int64(db.avgPutTime)*(db.putOps-entriesCount) + int64(elapsed)) / db.putOps)

	// 如果缓冲区达到批量写入大小，执行批量写入
	if len(db.batchBuffer) >= db.batchSize {
		return db.flushBatch()
	}

	return nil
}

// BatchGet 批量获取键值对
func (db *DB) BatchGet(keys []string) (map[string][]byte, error) {
	startTime := time.Now()

	result := make(map[string][]byte, len(keys))
	missingKeys := make([]string, 0)

	// 首先检查缓存
	for _, key := range keys {
		// 获取分片锁
		shardID := db.getShardID(key)
		db.shardMutexes[shardID].RLock()

		if !db.isOpen {
			db.shardMutexes[shardID].RUnlock()
			return nil, ErrDBNotOpen
		}

		// 检查缓存
		if value, ok := db.cache.Get(key); ok {
			// 缓存命中
			db.cacheHits++

			// 创建值的副本
			valueCopy := make([]byte, len(value))
			copy(valueCopy, value)
			result[key] = valueCopy
		} else {
			// 缓存未命中
			db.cacheMisses++
			missingKeys = append(missingKeys, key)
		}

		db.shardMutexes[shardID].RUnlock()
	}

	// 然后从存储中获取缓存未命中的键
	for _, key := range missingKeys {
		// 获取分片锁
		shardID := db.getShardID(key)
		db.shardMutexes[shardID].RLock()

		// 从存储中获取
		if value, ok := db.data[key]; ok {
			// 创建值的副本
			valueCopy := make([]byte, len(value))
			copy(valueCopy, value)
			result[key] = valueCopy

			// 更新缓存
			db.cache.Add(key, value)
		}

		db.shardMutexes[shardID].RUnlock()
	}

	// 更新统计信息
	db.getOps += int64(len(keys))
	elapsed := time.Since(startTime)
	db.avgGetTime = time.Duration((int64(db.avgGetTime)*int64(db.getOps-int64(len(keys))) + int64(elapsed)) / int64(db.getOps))

	return result, nil
}
