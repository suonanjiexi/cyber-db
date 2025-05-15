package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// 常量定义
const (
	// DefaultDirPerm 默认目录权限
	DefaultDirPerm = 0755
	// DefaultFilePerm 默认文件权限
	DefaultFilePerm = 0644
)

// StatusInfo 存储引擎状态信息
type StatusInfo struct {
	Type            string            // 引擎类型
	OpenTime        time.Time         // 引擎打开时间
	DatabaseCount   int               // 数据库数量
	TableCount      int               // 表数量
	RowCount        int64             // 总行数
	DiskSpaceUsed   int64             // 磁盘空间使用量（字节）
	MemoryUsed      int64             // 内存使用量（字节）
	ReadOperations  int64             // 读操作次数
	WriteOperations int64             // 写操作次数
	PendingWrites   int               // 待写入操作数
	AdditionalInfo  map[string]string // 附加信息
}

// CreateDirIfNotExist 如果目录不存在则创建
func CreateDirIfNotExist(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return os.MkdirAll(path, DefaultDirPerm)
	}
	return nil
}

// RemoveDirIfExist 如果目录存在则删除
func RemoveDirIfExist(path string) error {
	if _, err := os.Stat(path); err == nil {
		return os.RemoveAll(path)
	}
	return nil
}

// IsDir 检查路径是否为目录
func IsDir(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}

// EncodeInt64 将int64编码为字节数组
func EncodeInt64(num int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(num))
	return buf
}

// DecodeInt64 将字节数组解码为int64
func DecodeInt64(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))
}

// BackupEngine 备份存储引擎数据
func BackupEngine(engine Engine, backupDir string) error {
	// 确保备份目录存在
	if err := CreateDirIfNotExist(backupDir); err != nil {
		return fmt.Errorf("创建备份目录失败: %w", err)
	}

	ctx := context.Background()

	// 获取所有数据库
	// 注意：Engine接口目前不支持列出所有数据库的方法
	// 这里需要扩展Engine接口或者从配置中获取数据库列表

	// 示例：假设有一个名为"db1"的数据库
	databases := []string{"db1"}

	for _, dbName := range databases {
		// 为每个数据库创建备份目录
		dbBackupDir := filepath.Join(backupDir, dbName)
		if err := CreateDirIfNotExist(dbBackupDir); err != nil {
			return fmt.Errorf("创建数据库备份目录失败: %w", err)
		}

		// 获取数据库中的所有表
		// 注意：Engine接口目前不支持列出所有表的方法
		// 这里需要扩展Engine接口或者从配置中获取表列表

		// 示例：假设有一个名为"table1"的表
		tables := []string{"table1"}

		for _, tableName := range tables {
			// 备份表数据
			tableBackupFile := filepath.Join(dbBackupDir, tableName+".bak")
			file, err := os.Create(tableBackupFile)
			if err != nil {
				return fmt.Errorf("创建表备份文件失败: %w", err)
			}

			// 扫描表中的所有数据
			iter, err := engine.Scan(ctx, dbName, tableName, nil, nil)
			if err != nil {
				file.Close()
				return fmt.Errorf("扫描表数据失败: %w", err)
			}

			// 写入表数据
			for iter.Next() {
				key := iter.Key()
				value := iter.Value()

				// 写入键长度
				keyLen := int32(len(key))
				if err := binary.Write(file, binary.BigEndian, keyLen); err != nil {
					iter.Close()
					file.Close()
					return fmt.Errorf("写入键长度失败: %w", err)
				}

				// 写入键
				if _, err := file.Write(key); err != nil {
					iter.Close()
					file.Close()
					return fmt.Errorf("写入键失败: %w", err)
				}

				// 写入值长度
				valueLen := int32(len(value))
				if err := binary.Write(file, binary.BigEndian, valueLen); err != nil {
					iter.Close()
					file.Close()
					return fmt.Errorf("写入值长度失败: %w", err)
				}

				// 写入值
				if _, err := file.Write(value); err != nil {
					iter.Close()
					file.Close()
					return fmt.Errorf("写入值失败: %w", err)
				}
			}

			if err := iter.Error(); err != nil {
				iter.Close()
				file.Close()
				return fmt.Errorf("迭代表数据失败: %w", err)
			}

			iter.Close()
			file.Close()
		}
	}

	return nil
}

// RestoreEngine 从备份恢复存储引擎数据
func RestoreEngine(engine Engine, backupDir string) error {
	// 确保备份目录存在
	if !IsDir(backupDir) {
		return fmt.Errorf("备份目录不存在或不是目录")
	}

	ctx := context.Background()

	// 遍历备份目录获取所有数据库
	entries, err := os.ReadDir(backupDir)
	if err != nil {
		return fmt.Errorf("读取备份目录失败: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		dbName := entry.Name()
		dbBackupDir := filepath.Join(backupDir, dbName)

		// 创建数据库
		if err := engine.CreateDB(ctx, dbName); err != nil && err != ErrDBExists {
			return fmt.Errorf("创建数据库失败: %w", err)
		}

		// 遍历数据库备份目录获取所有表
		tableEntries, err := os.ReadDir(dbBackupDir)
		if err != nil {
			return fmt.Errorf("读取数据库备份目录失败: %w", err)
		}

		for _, tableEntry := range tableEntries {
			if tableEntry.IsDir() {
				continue
			}

			// 解析表名（去除.bak后缀）
			tableName := tableEntry.Name()
			if filepath.Ext(tableName) == ".bak" {
				tableName = tableName[:len(tableName)-4]
			} else {
				continue
			}

			// 创建表（需要表结构信息，这里简化处理）
			// 在实际应用中，应该将表结构信息作为元数据一同备份
			schema := &TableSchema{
				Columns: []ColumnDefinition{
					{
						Name: "id",
						Type: TypeInt,
					},
				},
			}

			if err := engine.CreateTable(ctx, dbName, tableName, schema); err != nil && err != ErrTableExists {
				return fmt.Errorf("创建表失败: %w", err)
			}

			// 开始事务来恢复表数据
			tx, err := engine.BeginTx(ctx)
			if err != nil {
				return fmt.Errorf("开始事务失败: %w", err)
			}

			// 打开表备份文件
			tableBackupFile := filepath.Join(dbBackupDir, tableEntry.Name())
			file, err := os.Open(tableBackupFile)
			if err != nil {
				tx.Rollback(ctx)
				return fmt.Errorf("打开表备份文件失败: %w", err)
			}

			// 读取和恢复表数据
			recordCount := 0
			for {
				// 读取键长度
				var keyLen int32
				if err := binary.Read(file, binary.BigEndian, &keyLen); err != nil {
					// 文件结束
					break
				}

				// 读取键
				key := make([]byte, keyLen)
				if _, err := file.Read(key); err != nil {
					file.Close()
					tx.Rollback(ctx)
					return fmt.Errorf("读取键失败: %w", err)
				}

				// 读取值长度
				var valueLen int32
				if err := binary.Read(file, binary.BigEndian, &valueLen); err != nil {
					file.Close()
					tx.Rollback(ctx)
					return fmt.Errorf("读取值长度失败: %w", err)
				}

				// 读取值
				value := make([]byte, valueLen)
				if _, err := file.Read(value); err != nil {
					file.Close()
					tx.Rollback(ctx)
					return fmt.Errorf("读取值失败: %w", err)
				}

				// 写入数据
				if err := tx.Put(ctx, dbName, tableName, key, value); err != nil {
					file.Close()
					tx.Rollback(ctx)
					return fmt.Errorf("写入数据失败: %w", err)
				}

				recordCount++

				// 每1000条记录提交一次事务，避免事务过大
				if recordCount%1000 == 0 {
					if err := tx.Commit(ctx); err != nil {
						file.Close()
						return fmt.Errorf("提交事务失败: %w", err)
					}

					// 开始新事务
					tx, err = engine.BeginTx(ctx)
					if err != nil {
						file.Close()
						return fmt.Errorf("开始新事务失败: %w", err)
					}
				}
			}

			file.Close()

			// 提交最后一批数据
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("提交最后一批数据失败: %w", err)
			}
		}
	}

	return nil
}

// GetEngineStatus 获取存储引擎状态信息
func GetEngineStatus(engine Engine) (*StatusInfo, error) {
	// 这是一个示例实现，实际应用中需要从存储引擎获取真实信息

	// 引擎类型判断（示例）
	var engineType string
	switch engine.(type) {
	case *MemoryEngine:
		engineType = "memory"
	case *DiskEngine:
		engineType = "disk"
	case *HybridEngine:
		engineType = "hybrid"
	default:
		engineType = "unknown"
	}

	// 创建状态信息
	info := &StatusInfo{
		Type:            engineType,
		OpenTime:        time.Now().Add(-time.Hour), // 假设引擎在1小时前打开
		DatabaseCount:   2,                          // 示例值
		TableCount:      5,                          // 示例值
		RowCount:        1000,                       // 示例值
		DiskSpaceUsed:   1024 * 1024 * 10,           // 示例值：10MB
		MemoryUsed:      1024 * 1024 * 2,            // 示例值：2MB
		ReadOperations:  5000,                       // 示例值
		WriteOperations: 1000,                       // 示例值
		PendingWrites:   0,                          // 示例值
		AdditionalInfo: map[string]string{
			"version": "1.0.0",
		},
	}

	// 对于混合引擎，添加额外信息
	if hybridEngine, ok := engine.(*HybridEngine); ok {
		// 在实际实现中，可以从hybridEngine中获取真实信息
		info.AdditionalInfo["buffer_size"] = fmt.Sprintf("%d bytes", hybridEngine.maxBufferSize)
		info.AdditionalInfo["current_buffer_usage"] = fmt.Sprintf("%d bytes", hybridEngine.currentBufferSize)
		info.AdditionalInfo["auto_flush"] = fmt.Sprintf("%v", hybridEngine.autoFlush)
		info.AdditionalInfo["sync_interval"] = hybridEngine.syncInterval.String()
	}

	return info, nil
}

// EngineStatistics 引擎统计信息
type EngineStatistics struct {
	ReadCount     int64         // 读操作次数
	WriteCount    int64         // 写操作次数
	ScanCount     int64         // 扫描操作次数
	ReadLatency   time.Duration // 平均读操作延迟
	WriteLatency  time.Duration // 平均写操作延迟
	ScanLatency   time.Duration // 平均扫描操作延迟
	CacheHitRate  float64       // 缓存命中率
	LastResetTime time.Time     // 上次重置统计信息的时间
}

// StatisticsCollector 统计信息采集器接口
// 在实际使用中，存储引擎可以实现此接口来提供性能统计信息
type StatisticsCollector interface {
	// GetStatistics 获取统计信息
	GetStatistics() *EngineStatistics

	// ResetStatistics 重置统计信息
	ResetStatistics()
}

// NewStatisticsCollector 创建新的统计信息采集器
func NewStatisticsCollector() StatisticsCollector {
	return &defaultStatisticsCollector{
		stats: &EngineStatistics{
			LastResetTime: time.Now(),
		},
	}
}

// defaultStatisticsCollector 默认的统计信息采集器实现
type defaultStatisticsCollector struct {
	stats *EngineStatistics
	mu    sync.Mutex
}

// GetStatistics 获取统计信息
func (c *defaultStatisticsCollector) GetStatistics() *EngineStatistics {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 返回统计信息的副本
	statsCopy := *c.stats
	return &statsCopy
}

// ResetStatistics 重置统计信息
func (c *defaultStatisticsCollector) ResetStatistics() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stats = &EngineStatistics{
		LastResetTime: time.Now(),
	}
}

// RecordRead 记录读操作
func (c *defaultStatisticsCollector) RecordRead(latency time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stats.ReadCount++
	c.stats.ReadLatency = (c.stats.ReadLatency*time.Duration(c.stats.ReadCount-1) + latency) / time.Duration(c.stats.ReadCount)
}

// RecordWrite 记录写操作
func (c *defaultStatisticsCollector) RecordWrite(latency time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stats.WriteCount++
	c.stats.WriteLatency = (c.stats.WriteLatency*time.Duration(c.stats.WriteCount-1) + latency) / time.Duration(c.stats.WriteCount)
}

// RecordScan 记录扫描操作
func (c *defaultStatisticsCollector) RecordScan(latency time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stats.ScanCount++
	c.stats.ScanLatency = (c.stats.ScanLatency*time.Duration(c.stats.ScanCount-1) + latency) / time.Duration(c.stats.ScanCount)
}

// RecordCacheHit 记录缓存命中
func (c *defaultStatisticsCollector) RecordCacheHit(hit bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 更新缓存命中率
	if hit {
		c.stats.CacheHitRate = (c.stats.CacheHitRate*float64(c.stats.ReadCount) + 1.0) / float64(c.stats.ReadCount+1)
	} else {
		c.stats.CacheHitRate = (c.stats.CacheHitRate * float64(c.stats.ReadCount)) / float64(c.stats.ReadCount+1)
	}
}
