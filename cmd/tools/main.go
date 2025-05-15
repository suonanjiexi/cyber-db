package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
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

	// 获取备份、恢复和监控命令
	backupCmd := getBackupCmd()
	restoreCmd := getRestoreCmd()
	monitorCmd := getMonitorCmd()

	// 添加子命令
	rootCmd.AddCommand(backupCmd)
	rootCmd.AddCommand(restoreCmd)
	rootCmd.AddCommand(monitorCmd)

	// 执行命令
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
