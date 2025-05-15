package storage

import (
	"fmt"
	"path/filepath"
)

// EngineType 存储引擎类型
type EngineType string

const (
	// EngineTypeMemory 内存引擎
	EngineTypeMemory EngineType = "memory"
	// EngineTypeDisk 磁盘引擎
	EngineTypeDisk EngineType = "disk"
	// EngineTypeHybrid 混合引擎
	EngineTypeHybrid EngineType = "hybrid"
)

// EngineConfig 存储引擎配置
type EngineConfig struct {
	// 引擎类型
	Type EngineType `yaml:"type"`

	// 数据目录（对于磁盘和混合引擎）
	DataDir string `yaml:"data_dir"`

	// 同步间隔（毫秒），适用于混合引擎
	SyncIntervalMs int `yaml:"sync_interval_ms"`

	// 最大写入缓冲区大小（MB），适用于混合引擎
	MaxBufferSizeMB int `yaml:"max_buffer_size_mb"`

	// 是否启用自动刷盘，适用于混合引擎
	AutoFlush bool `yaml:"auto_flush"`
}

// NewDefaultEngineConfig 创建默认的引擎配置
func NewDefaultEngineConfig() EngineConfig {
	return EngineConfig{
		Type:            EngineTypeHybrid,
		DataDir:         "data",
		SyncIntervalMs:  1000, // 1秒
		MaxBufferSizeMB: 64,   // 64MB
		AutoFlush:       true,
	}
}

// NewEngine 根据配置创建存储引擎
func NewEngine(config EngineConfig) (Engine, error) {
	switch config.Type {
	case EngineTypeMemory:
		return NewMemoryEngine(), nil

	case EngineTypeDisk:
		// 确保数据目录存在
		if config.DataDir == "" {
			return nil, fmt.Errorf("磁盘引擎需要指定数据目录")
		}

		// 使用绝对路径
		absPath, err := filepath.Abs(config.DataDir)
		if err != nil {
			return nil, fmt.Errorf("计算数据目录绝对路径失败: %w", err)
		}

		return NewDiskEngine(absPath), nil

	case EngineTypeHybrid:
		// 确保数据目录存在
		if config.DataDir == "" {
			return nil, fmt.Errorf("混合引擎需要指定数据目录")
		}

		// 使用绝对路径
		absPath, err := filepath.Abs(config.DataDir)
		if err != nil {
			return nil, fmt.Errorf("计算数据目录绝对路径失败: %w", err)
		}

		// 使用默认值处理未设置的配置
		syncInterval := config.SyncIntervalMs
		if syncInterval <= 0 {
			syncInterval = 1000 // 默认1秒
		}

		maxBufferSize := config.MaxBufferSizeMB
		if maxBufferSize <= 0 {
			maxBufferSize = 64 // 默认64MB
		}

		return NewHybridEngine(absPath, syncInterval, maxBufferSize, config.AutoFlush), nil

	default:
		return nil, fmt.Errorf("不支持的存储引擎类型: %s", config.Type)
	}
}
