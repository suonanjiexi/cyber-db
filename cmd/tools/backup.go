package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cyberdb/cyberdb/pkg/storage"
	"github.com/spf13/cobra"
)

// 定义备份命令
func getBackupCmd() *cobra.Command {
	var (
		outputDir  string
		engineType string
		dataDir    string
		verbose    bool
	)

	backupCmd := &cobra.Command{
		Use:   "backup",
		Short: "备份数据库",
		Long:  `备份CyberDB数据库中的数据到指定目录`,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("开始备份数据库...")

			// 创建存储引擎
			engine, err := createBackupEngine(engineType, dataDir)
			if err != nil {
				return fmt.Errorf("创建存储引擎失败: %w", err)
			}
			defer engine.Close()

			// 确保输出目录存在
			if err := storage.CreateDirIfNotExist(outputDir); err != nil {
				return fmt.Errorf("创建备份目录失败: %w", err)
			}

			// 生成带有时间戳的备份子目录
			timestamp := time.Now().Format("20060102_150405")
			backupSubDir := filepath.Join(outputDir, "backup_"+timestamp)
			if err := storage.CreateDirIfNotExist(backupSubDir); err != nil {
				return fmt.Errorf("创建备份子目录失败: %w", err)
			}

			// 创建latest符号链接
			latestLink := filepath.Join(outputDir, "backup_latest")
			// 删除已存在的符号链接（如果有）
			os.Remove(latestLink)
			// 创建新的符号链接
			if err := os.Symlink(backupSubDir, latestLink); err != nil {
				fmt.Printf("警告: 创建latest符号链接失败: %v\n", err)
			}

			// 执行备份
			fmt.Printf("备份数据到目录: %s\n", backupSubDir)
			startTime := time.Now()

			// 创建备份文件
			backupFile := filepath.Join(backupSubDir, "backup.data")
			file, err := os.Create(backupFile)
			if err != nil {
				return fmt.Errorf("创建备份文件失败: %w", err)
			}
			defer file.Close()

			// 执行备份
			if err := engine.Backup(cmd.Context(), file); err != nil {
				return fmt.Errorf("备份失败: %w", err)
			}

			// 记录备份信息
			infoFile := filepath.Join(backupSubDir, "backup_info.txt")
			info := fmt.Sprintf("备份时间: %s\n引擎类型: %s\n数据目录: %s\n",
				time.Now().Format("2006-01-02 15:04:05"), engineType, dataDir)

			if err := os.WriteFile(infoFile, []byte(info), 0644); err != nil {
				fmt.Printf("警告: 写入备份信息文件失败: %v\n", err)
			}

			duration := time.Since(startTime)
			fmt.Printf("备份成功，用时: %v\n", duration)
			return nil
		},
	}

	// 设置命令参数
	backupCmd.Flags().StringVarP(&outputDir, "output", "o", "", "备份输出目录 (必需)")
	backupCmd.Flags().StringVarP(&engineType, "engine", "e", "hybrid", "存储引擎类型 (memory, disk, hybrid)")
	backupCmd.Flags().StringVarP(&dataDir, "data-dir", "d", "data", "数据目录 (对于磁盘和混合引擎)")
	backupCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "显示详细输出")
	backupCmd.MarkFlagRequired("output")

	return backupCmd
}

// 创建用于备份的存储引擎
func createBackupEngine(engineType, dataDir string) (storage.Engine, error) {
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
