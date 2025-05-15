package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cyberdb/cyberdb/pkg/storage"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile    string
	outputDir  string
	inputDir   string
	engineType string
	dataDir    string
	forced     bool
	verbose    bool
)

func main() {
	// 定义根命令
	rootCmd := &cobra.Command{
		Use:   "cyberdb-tools",
		Short: "CyberDB工具集",
		Long:  `CyberDB工具集 - 提供数据库备份、恢复等管理功能`,
	}

	// 定义备份命令
	backupCmd := &cobra.Command{
		Use:   "backup",
		Short: "备份数据库",
		Long:  `备份CyberDB数据库中的数据到指定目录`,
		RunE:  runBackup,
	}

	// 定义恢复命令
	restoreCmd := &cobra.Command{
		Use:   "restore",
		Short: "恢复数据库",
		Long:  `从备份恢复CyberDB数据库数据`,
		RunE:  runRestore,
	}

	// 设置备份命令的参数
	backupCmd.Flags().StringVarP(&cfgFile, "config", "c", "", "配置文件路径")
	backupCmd.Flags().StringVarP(&outputDir, "output", "o", "", "备份输出目录 (必需)")
	backupCmd.Flags().StringVarP(&engineType, "engine", "e", "hybrid", "存储引擎类型 (memory, disk, hybrid)")
	backupCmd.Flags().StringVarP(&dataDir, "data-dir", "d", "data", "数据目录 (对于磁盘和混合引擎)")
	backupCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "显示详细输出")
	backupCmd.MarkFlagRequired("output")

	// 设置恢复命令的参数
	restoreCmd.Flags().StringVarP(&cfgFile, "config", "c", "", "配置文件路径")
	restoreCmd.Flags().StringVarP(&inputDir, "input", "i", "", "备份输入目录 (必需)")
	restoreCmd.Flags().StringVarP(&engineType, "engine", "e", "hybrid", "存储引擎类型 (memory, disk, hybrid)")
	restoreCmd.Flags().StringVarP(&dataDir, "data-dir", "d", "data", "数据目录 (对于磁盘和混合引擎)")
	restoreCmd.Flags().BoolVarP(&forced, "force", "f", false, "强制恢复 (覆盖现有数据)")
	restoreCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "显示详细输出")
	restoreCmd.MarkFlagRequired("input")

	// 添加子命令
	rootCmd.AddCommand(backupCmd)
	rootCmd.AddCommand(restoreCmd)

	// 执行命令
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// 运行备份命令
func runBackup(cmd *cobra.Command, args []string) error {
	fmt.Println("开始备份数据库...")

	// 获取配置文件
	if cfgFile != "" {
		// 使用指定的配置文件
		viper.SetConfigFile(cfgFile)
		if err := viper.ReadInConfig(); err != nil {
			return fmt.Errorf("读取配置文件失败: %w", err)
		}
		fmt.Println("使用配置文件:", viper.ConfigFileUsed())

		// 从配置文件获取存储引擎配置
		if viper.IsSet("storage.type") {
			engineType = viper.GetString("storage.type")
		}
		if viper.IsSet("storage.data_dir") {
			dataDir = viper.GetString("storage.data_dir")
		}
	}

	// 创建存储引擎
	engine, err := createEngine()
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
	if err := storage.BackupEngine(engine, backupSubDir); err != nil {
		return fmt.Errorf("备份失败: %w", err)
	}
	duration := time.Since(startTime)

	fmt.Printf("备份成功，用时: %v\n", duration)
	return nil
}

// 运行恢复命令
func runRestore(cmd *cobra.Command, args []string) error {
	fmt.Println("开始恢复数据库...")

	// 获取配置文件
	if cfgFile != "" {
		// 使用指定的配置文件
		viper.SetConfigFile(cfgFile)
		if err := viper.ReadInConfig(); err != nil {
			return fmt.Errorf("读取配置文件失败: %w", err)
		}
		fmt.Println("使用配置文件:", viper.ConfigFileUsed())

		// 从配置文件获取存储引擎配置
		if viper.IsSet("storage.type") {
			engineType = viper.GetString("storage.type")
		}
		if viper.IsSet("storage.data_dir") {
			dataDir = viper.GetString("storage.data_dir")
		}
	}

	// 检查备份目录是否存在
	if !storage.IsDir(inputDir) {
		return fmt.Errorf("备份目录不存在或不是目录: %s", inputDir)
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
	engine, err := createEngine()
	if err != nil {
		return fmt.Errorf("创建存储引擎失败: %w", err)
	}
	defer engine.Close()

	// 执行恢复
	fmt.Printf("从目录恢复数据: %s\n", inputDir)
	startTime := time.Now()
	if err := storage.RestoreEngine(engine, inputDir); err != nil {
		return fmt.Errorf("恢复失败: %w", err)
	}
	duration := time.Since(startTime)

	fmt.Printf("恢复成功，用时: %v\n", duration)
	return nil
}

// 创建存储引擎
func createEngine() (storage.Engine, error) {
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
