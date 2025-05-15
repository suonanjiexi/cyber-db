package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cyberdb/cyberdb/pkg/storage"
	"github.com/spf13/cobra"
)

// 定义恢复命令
func getRestoreCmd() *cobra.Command {
	var (
		inputDir   string
		engineType string
		dataDir    string
		forced     bool
		verbose    bool
	)

	restoreCmd := &cobra.Command{
		Use:   "restore",
		Short: "恢复数据库",
		Long:  `从备份恢复CyberDB数据库数据`,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("开始恢复数据库...")

			// 检查备份目录是否存在
			if !storage.IsDir(inputDir) {
				return fmt.Errorf("备份目录不存在或不是目录: %s", inputDir)
			}

			// 查找backup.data文件
			backupFile := filepath.Join(inputDir, "backup.data")
			if _, err := os.Stat(backupFile); os.IsNotExist(err) {
				return fmt.Errorf("备份文件不存在: %s", backupFile)
			}

			// 检查数据目录是否存在
			if storage.IsDir(dataDir) && !forced {
				return fmt.Errorf("数据目录已存在，若要覆盖现有数据，请使用 --force 参数")
			}

			// 如果强制恢复，清空数据目录
			if forced && storage.IsDir(dataDir) {
				fmt.Println("警告: 正在清空数据目录...")
				if err := storage.RemoveDirIfExist(dataDir); err != nil {
					return fmt.Errorf("清空数据目录失败: %w", err)
				}
			}

			// 创建数据目录
			if err := storage.CreateDirIfNotExist(dataDir); err != nil {
				return fmt.Errorf("创建数据目录失败: %w", err)
			}

			// 创建存储引擎
			engine, err := createRestoreEngine(engineType, dataDir)
			if err != nil {
				return fmt.Errorf("创建存储引擎失败: %w", err)
			}
			defer engine.Close()

			// 打开备份文件
			file, err := os.Open(backupFile)
			if err != nil {
				return fmt.Errorf("打开备份文件失败: %w", err)
			}
			defer file.Close()

			// 执行恢复
			fmt.Printf("从文件恢复数据: %s\n", backupFile)
			startTime := time.Now()

			if err := engine.Restore(cmd.Context(), file); err != nil {
				return fmt.Errorf("恢复失败: %w", err)
			}

			duration := time.Since(startTime)

			// 记录恢复信息
			restoreInfoFile := filepath.Join(dataDir, "restore_info.txt")
			info := fmt.Sprintf("恢复时间: %s\n源备份: %s\n引擎类型: %s\n",
				time.Now().Format("2006-01-02 15:04:05"), inputDir, engineType)

			if err := os.WriteFile(restoreInfoFile, []byte(info), 0644); err != nil {
				fmt.Printf("警告: 写入恢复信息文件失败: %v\n", err)
			}

			fmt.Printf("恢复成功，用时: %v\n", duration)
			return nil
		},
	}

	// 设置命令参数
	restoreCmd.Flags().StringVarP(&inputDir, "input", "i", "", "备份输入目录 (必需)")
	restoreCmd.Flags().StringVarP(&engineType, "engine", "e", "hybrid", "存储引擎类型 (memory, disk, hybrid)")
	restoreCmd.Flags().StringVarP(&dataDir, "data-dir", "d", "data", "数据目录 (对于磁盘和混合引擎)")
	restoreCmd.Flags().BoolVarP(&forced, "force", "f", false, "强制恢复 (覆盖现有数据)")
	restoreCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "显示详细输出")
	restoreCmd.MarkFlagRequired("input")

	return restoreCmd
}

// 创建用于恢复的存储引擎
func createRestoreEngine(engineType, dataDir string) (storage.Engine, error) {
	// 创建存储引擎配置
	config := storage.EngineConfig{
		DataDir: dataDir,
	}

	// 设置引擎类型
	switch engineType {
	case "memory":
		config.Type = storage.EngineTypeMemory
	case "disk":
		config.Type = storage.EngineTypeDisk
	case "hybrid":
		config.Type = storage.EngineTypeHybrid
		config.SyncIntervalMs = 1000
		config.MaxBufferSizeMB = 64
		config.AutoFlush = true
	default:
		return nil, fmt.Errorf("不支持的存储引擎类型: %s", engineType)
	}

	// 创建存储引擎
	engine, err := storage.NewEngine(config)
	if err != nil {
		return nil, err
	}

	// 打开存储引擎
	if err := engine.Open(); err != nil {
		return nil, err
	}

	return engine, nil
}
