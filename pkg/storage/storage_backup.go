package storage

import (
	"bufio"
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// RecoverFromBackup 从备份文件恢复数据库
func (db *DB) RecoverFromBackup(backupPath string) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if !db.isOpen {
		return ErrDBNotOpen
	}

	// 打开备份文件
	file, err := os.Open(backupPath)
	if err != nil {
		return fmt.Errorf("failed to open backup file: %w", err)
	}
	defer file.Close()

	// 检查文件是否为gzip压缩
	var reader io.Reader = file
	bufReader := bufio.NewReader(file)

	// 读取前几个字节检查是否为gzip格式
	header, err := bufReader.Peek(2)
	if err != nil {
		return fmt.Errorf("failed to read file header: %w", err)
	}

	// gzip文件的魔数是0x1f 0x8b
	if header[0] == 0x1f && header[1] == 0x8b {
		gzipReader, err := gzip.NewReader(bufReader)
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzipReader.Close()
		reader = gzipReader
	} else {
		reader = bufReader
	}

	// 使用gob解码数据
	decoder := gob.NewDecoder(reader)

	// 创建临时数据存储
	tempData := make(map[string][]byte)

	if err := decoder.Decode(&tempData); err != nil {
		return fmt.Errorf("failed to decode backup data: %w", err)
	}

	// 替换当前数据
	db.data = tempData

	// 清空并重建缓存
	db.cache.Purge()
	for k, v := range db.data {
		db.cache.Add(k, v)
	}

	// 重建索引
	if err := db.indexManager.LoadIndexes(); err != nil {
		return fmt.Errorf("failed to rebuild indexes: %w", err)
	}

	return nil
}

// CreateBackup 创建数据库备份
func (db *DB) CreateBackup(backupPath string) error {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	if !db.isOpen {
		return ErrDBNotOpen
	}

	// 确保目录存在
	dir := filepath.Dir(backupPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// 创建备份文件
	file, err := os.Create(backupPath)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %w", err)
	}
	defer file.Close()

	// 使用缓冲写入
	bufWriter := bufio.NewWriterSize(file, 4*1024*1024)

	// 如果启用了压缩
	var encoder *gob.Encoder
	if db.compression {
		compressedWriter, err := gzip.NewWriterLevel(bufWriter, gzip.BestCompression)
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

	// 更新最后备份时间
	db.lastBackupTime = time.Now()

	return nil
}

// IncrementalBackup 创建增量备份
func (db *DB) IncrementalBackup(backupPath string, lastBackupTime time.Time) error {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	if !db.isOpen {
		return ErrDBNotOpen
	}

	// 确保目录存在
	dir := filepath.Dir(backupPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// 创建备份文件
	file, err := os.Create(backupPath)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %w", err)
	}
	defer file.Close()

	// 使用缓冲写入
	bufWriter := bufio.NewWriterSize(file, 4*1024*1024)

	// 如果启用了压缩
	var encoder *gob.Encoder
	var compressedWriter *gzip.Writer
	if db.compression {
		compressedWriter, err = gzip.NewWriterLevel(bufWriter, gzip.BestSpeed)
		if err != nil {
			return fmt.Errorf("failed to create compressed writer: %w", err)
		}
		defer compressedWriter.Close()
		encoder = gob.NewEncoder(compressedWriter)
	} else {
		encoder = gob.NewEncoder(bufWriter)
	}

	// 创建增量数据映射
	incrementalData := make(map[string][]byte)

	// 遍历所有数据，找出自上次备份以来修改的数据
	for key, value := range db.data {
		// 检查键是否在上次备份后修改
		// 这里假设我们有一个修改时间映射，实际实现需要添加这个功能
		if db.getModificationTime(key).After(lastBackupTime) {
			incrementalData[key] = value
		}
	}

	// 编码增量数据
	if err := encoder.Encode(incrementalData); err != nil {
		return fmt.Errorf("failed to encode incremental data: %w", err)
	}

	// 如果使用了压缩，关闭压缩写入器
	if db.compression {
		if err := compressedWriter.Close(); err != nil {
			return fmt.Errorf("failed to close compressed writer: %w", err)
		}
	}

	// 刷新缓冲区
	if err := bufWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}

	// 更新最后备份时间
	db.lastBackupTime = time.Now()

	return nil
}

// getModificationTime 获取键的最后修改时间
// 这个函数需要添加到DB结构体中，并在Put操作时更新修改时间
func (db *DB) getModificationTime(key string) time.Time {
	if t, ok := db.modificationTimes[key]; ok {
		return t
	}
	// 如果没有记录修改时间，返回零值
	return time.Time{}
}

// MergeBackups 合并多个备份文件
func (db *DB) MergeBackups(backupPaths []string, outputPath string) error {
	if !db.isOpen {
		return ErrDBNotOpen
	}

	// 确保输出目录存在
	dir := filepath.Dir(outputPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// 创建合并后的数据映射
	mergedData := make(map[string][]byte)

	// 按时间顺序处理每个备份文件
	for _, backupPath := range backupPaths {
		// 打开备份文件
		file, err := os.Open(backupPath)
		if err != nil {
			return fmt.Errorf("failed to open backup file %s: %w", backupPath, err)
		}

		// 检查文件是否为gzip压缩
		var reader io.Reader = file
		bufReader := bufio.NewReader(file)

		// 读取前几个字节检查是否为gzip格式
		header, err := bufReader.Peek(2)
		if err != nil {
			file.Close()
			return fmt.Errorf("failed to read file header: %w", err)
		}

		// gzip文件的魔数是0x1f 0x8b
		if header[0] == 0x1f && header[1] == 0x8b {
			gzipReader, err := gzip.NewReader(bufReader)
			if err != nil {
				file.Close()
				return fmt.Errorf("failed to create gzip reader: %w", err)
			}
			defer gzipReader.Close()
			reader = gzipReader
		} else {
			reader = bufReader
		}

		// 使用gob解码数据
		decoder := gob.NewDecoder(reader)

		// 创建临时数据存储
		tempData := make(map[string][]byte)

		if err := decoder.Decode(&tempData); err != nil {
			file.Close()
			return fmt.Errorf("failed to decode backup data from %s: %w", backupPath, err)
		}

		// 合并数据（后面的备份会覆盖前面的备份）
		for k, v := range tempData {
			mergedData[k] = v
		}

		file.Close()
	}

	// 创建输出文件
	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	// 使用缓冲写入
	bufWriter := bufio.NewWriterSize(outFile, 4*1024*1024)

	// 如果启用了压缩
	var encoder *gob.Encoder
	var compressedWriter *gzip.Writer
	if db.compression {
		compressedWriter, err = gzip.NewWriterLevel(bufWriter, gzip.BestCompression)
		if err != nil {
			return fmt.Errorf("failed to create compressed writer: %w", err)
		}
		defer compressedWriter.Close()
		encoder = gob.NewEncoder(compressedWriter)
	} else {
		encoder = gob.NewEncoder(bufWriter)
	}

	// 编码合并后的数据
	if err := encoder.Encode(mergedData); err != nil {
		return fmt.Errorf("failed to encode merged data: %w", err)
	}

	// 如果使用了压缩，关闭压缩写入器
	if db.compression {
		if err := compressedWriter.Close(); err != nil {
			return fmt.Errorf("failed to close compressed writer: %w", err)
		}
	}

	// 刷新缓冲区
	if err := bufWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}

	return nil
}

// ScheduleBackup 设置定期备份计划
func (db *DB) ScheduleBackup(backupDir string, interval time.Duration, keepCount int) error {
	if !db.isOpen {
		return ErrDBNotOpen
	}

	// 确保备份目录存在
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// 停止现有的备份计划（如果有）
	if db.backupTicker != nil {
		db.backupTicker.Stop()
	}

	// 创建新的备份计划
	db.backupTicker = time.NewTicker(interval)
	db.backupKeepCount = keepCount

	// 启动备份goroutine
	go func() {
		for range db.backupTicker.C {
			// 创建备份文件名，包含时间戳
			timestamp := time.Now().Format("20060102-150405")
			backupPath := filepath.Join(backupDir, fmt.Sprintf("backup-%s.db", timestamp))

			// 创建备份
			if err := db.CreateBackup(backupPath); err != nil {
				log.Printf("定期备份失败: %v", err)
				continue
			}

			// 清理旧备份
			if keepCount > 0 {
				if err := db.cleanupOldBackups(backupDir, keepCount); err != nil {
					log.Printf("清理旧备份失败: %v", err)
				}
			}
		}
	}()

	return nil
}

// StopScheduledBackup 停止定期备份
func (db *DB) StopScheduledBackup() {
	if db.backupTicker != nil {
		db.backupTicker.Stop()
		db.backupTicker = nil
	}
}

// cleanupOldBackups 清理旧备份文件，只保留最新的n个
func (db *DB) cleanupOldBackups(backupDir string, keepCount int) error {
	// 读取备份目录中的所有文件
	files, err := os.ReadDir(backupDir)
	if err != nil {
		return fmt.Errorf("failed to read backup directory: %w", err)
	}

	// 过滤出备份文件并按修改时间排序
	type backupFile struct {
		path    string
		modTime time.Time
	}

	var backupFiles []backupFile
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".db" {
			info, err := file.Info()
			if err != nil {
				continue
			}

			backupFiles = append(backupFiles, backupFile{
				path:    filepath.Join(backupDir, file.Name()),
				modTime: info.ModTime(),
			})
		}
	}

	// 按修改时间排序（从新到旧）
	sort.Slice(backupFiles, func(i, j int) bool {
		return backupFiles[i].modTime.After(backupFiles[j].modTime)
	})

	// 删除多余的旧备份
	if len(backupFiles) > keepCount {
		for i := keepCount; i < len(backupFiles); i++ {
			if err := os.Remove(backupFiles[i].path); err != nil {
				log.Printf("删除旧备份文件 %s 失败: %v", backupFiles[i].path, err)
			}
		}
	}

	return nil
}

// VerifyBackup 验证备份文件的完整性
func (db *DB) VerifyBackup(backupPath string) error {
	// 打开备份文件
	file, err := os.Open(backupPath)
	if err != nil {
		return fmt.Errorf("failed to open backup file: %w", err)
	}
	defer file.Close()

	// 检查文件是否为gzip压缩
	var reader io.Reader = file
	bufReader := bufio.NewReader(file)

	// 读取前几个字节检查是否为gzip格式
	header, err := bufReader.Peek(2)
	if err != nil {
		return fmt.Errorf("failed to read file header: %w", err)
	}

	// gzip文件的魔数是0x1f 0x8b
	if header[0] == 0x1f && header[1] == 0x8b {
		gzipReader, err := gzip.NewReader(bufReader)
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzipReader.Close()
		reader = gzipReader
	} else {
		reader = bufReader
	}

	// 使用gob解码数据
	decoder := gob.NewDecoder(reader)

	// 创建临时数据存储
	tempData := make(map[string][]byte)

	// 尝试解码数据，验证备份文件格式是否正确
	if err := decoder.Decode(&tempData); err != nil {
		return fmt.Errorf("备份文件格式无效: %w", err)
	}

	// 验证数据完整性
	keyCount := len(tempData)
	log.Printf("备份文件 %s 包含 %d 个键值对", backupPath, keyCount)

	return nil
}

// RestoreBackupToPoint 恢复到指定时间点的状态
func (db *DB) RestoreBackupToPoint(backupDir string, targetTime time.Time) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if !db.isOpen {
		return ErrDBNotOpen
	}

	// 读取备份目录中的所有文件
	files, err := os.ReadDir(backupDir)
	if err != nil {
		return fmt.Errorf("failed to read backup directory: %w", err)
	}

	// 过滤出备份文件并按修改时间排序
	type backupFile struct {
		path    string
		modTime time.Time
	}

	var backupFiles []backupFile
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".db" {
			info, err := file.Info()
			if err != nil {
				continue
			}

			backupFiles = append(backupFiles, backupFile{
				path:    filepath.Join(backupDir, file.Name()),
				modTime: info.ModTime(),
			})
		}
	}

	// 按修改时间排序（从旧到新）
	sort.Slice(backupFiles, func(i, j int) bool {
		return backupFiles[i].modTime.Before(backupFiles[j].modTime)
	})

	// 找到最接近目标时间的备份
	var selectedBackup string
	for i := len(backupFiles) - 1; i >= 0; i-- {
		if backupFiles[i].modTime.Before(targetTime) || backupFiles[i].modTime.Equal(targetTime) {
			selectedBackup = backupFiles[i].path
			break
		}
	}

	if selectedBackup == "" {
		return fmt.Errorf("没有找到在 %v 之前的备份", targetTime)
	}

	// 恢复选定的备份
	log.Printf("正在恢复备份: %s (创建于 %v)", selectedBackup, backupFiles[len(backupFiles)-1].modTime)
	return db.RecoverFromBackup(selectedBackup)
}
