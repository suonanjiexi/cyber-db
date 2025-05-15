package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/cyberdb/cyberdb/pkg/storage"
	"github.com/spf13/cobra"
)

// 定义监控命令
func getMonitorCmd() *cobra.Command {
	var (
		engineType     string
		dataDir        string
		intervalSecond int
		monitorTime    int
	)

	monitorCmd := &cobra.Command{
		Use:   "monitor",
		Short: "监控数据库性能",
		Long:  `监控CyberDB数据库的性能指标和运行状态`,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("开始监控数据库性能...")

			// 创建存储引擎
			engine, err := createMonitorEngine(engineType, dataDir)
			if err != nil {
				return fmt.Errorf("创建存储引擎失败: %w", err)
			}
			defer engine.Close()

			// 创建性能监控器
			monitor := storage.NewPerformanceMonitor(engine, time.Duration(intervalSecond)*time.Second, 100)
			monitor.Start()
			defer monitor.Stop()

			// 处理Ctrl+C信号
			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, os.Interrupt)

			// 创建定时器
			ticker := time.NewTicker(time.Duration(intervalSecond) * time.Second)
			defer ticker.Stop()

			// 设置监控时间
			var monitorEndTime <-chan time.Time
			if monitorTime > 0 {
				monitorEndTime = time.After(time.Duration(monitorTime) * time.Second)
			}

			// 打印表头
			fmt.Println("\n时间\t\t\t读OPS\t写OPS\t删OPS\t扫OPS\t读延迟(ns)\t写延迟(ns)\t内存(MB)\t磁盘(MB)")
			fmt.Println("--------------------------------------------------------------------------------------------------------")

			// 监控循环
			for {
				select {
				case <-ticker.C:
					// 获取最新的性能样本
					sample := monitor.GetLatestSample()
					if sample == nil {
						continue
					}

					// 打印简要统计信息
					fmt.Printf("%s\t%.2f\t%.2f\t%.2f\t%.2f\t%d\t\t%d\t\t%.2f\t\t%.2f\n",
						sample.Timestamp.Format("2006-01-02 15:04:05"),
						sample.Throughput.ReadsPerSec,
						sample.Throughput.WritesPerSec,
						sample.Throughput.DeletesPerSec,
						sample.Throughput.ScansPerSec,
						sample.Latencies.AvgReadLatency,
						sample.Latencies.AvgWriteLatency,
						float64(sample.Stats.MemoryUsage)/(1024*1024),
						float64(sample.Stats.DiskUsage)/(1024*1024))

				case <-sigCh:
					// 用户按下Ctrl+C，打印完整报告并退出
					fmt.Println("\n\n收到中断信号，打印完整报告：")
					fmt.Println(monitor.GenerateReport())
					return nil

				case <-monitorEndTime:
					// 监控时间到，打印完整报告并退出
					if monitorTime > 0 {
						fmt.Printf("\n\n监控时间(%d秒)结束，打印完整报告：\n", monitorTime)
						fmt.Println(monitor.GenerateReport())
						return nil
					}
				}
			}
		},
	}

	// 设置命令参数
	monitorCmd.Flags().StringVarP(&engineType, "engine", "e", "hybrid", "存储引擎类型 (memory, disk, hybrid)")
	monitorCmd.Flags().StringVarP(&dataDir, "data-dir", "d", "data", "数据目录 (对于磁盘和混合引擎)")
	monitorCmd.Flags().IntVarP(&intervalSecond, "interval", "i", 5, "采样间隔(秒)")
	monitorCmd.Flags().IntVarP(&monitorTime, "time", "t", 0, "监控时间(秒)，0表示持续监控直到手动中断")

	return monitorCmd
}

// 创建用于监控的存储引擎
func createMonitorEngine(engineType, dataDir string) (storage.Engine, error) {
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
