package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/suonanjiexi/cyber-db/pkg/failover"
)

func main() {
	fmt.Println("Cyber-DB 故障转移测试程序")
	fmt.Println("=========================")

	// 设置日志
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)

	// 初始化随机数生成器
	rand.Seed(time.Now().UnixNano())

	// 初始化故障转移管理器
	failoverConfig := failover.FailoverConfig{
		EnableAutoFailover: true,
		FailoverTimeout:    10 * time.Second,
		MaxFailovers:       3,
		FailoverPeriod:     1 * time.Hour,
		WaitTime:           2 * time.Second, // 减少等待时间以加快测试
	}
	fm := failover.NewFailoverManager(failoverConfig)

	// 初始化健康检查器
	healthConfig := failover.HealthConfig{
		CheckInterval:      1 * time.Second, // 减少检查间隔以加快测试
		FailureThreshold:   2,               // 减少阈值以加快测试
		RecoveryThreshold:  2,
		CPUThreshold:       80.0, // 设置较低的阈值以便测试
		MemoryThreshold:    80.0,
		DiskThreshold:      80.0,
		EnableAutoFailover: true,
	}
	hc := failover.NewHealthChecker(healthConfig, fm)

	// 注册节点
	fmt.Println("注册测试节点...")
	registerTestNodes(fm)

	// 启动健康检查
	fmt.Println("启动健康检查...")
	hc.Start()
	defer hc.Stop()

	// 启动模拟负载
	fmt.Println("开始模拟节点负载变化...")
	stopChan := make(chan struct{})
	done := make(chan struct{})
	go simulateNodeLoad(fm, stopChan, done)

	// 等待用户中断
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println("按 Ctrl+C 停止测试")

	<-sigCh
	fmt.Println("\n停止测试...")
	close(stopChan)
	<-done

	// 打印健康摘要
	clusterHealth := hc.GetClusterHealthSummary()
	fmt.Println("\n集群健康摘要:")
	for k, v := range clusterHealth {
		fmt.Printf("%s: %v\n", k, v)
	}

	// 打印故障转移历史
	failoverHistory := fm.GetFailoverHistory()
	fmt.Println("\n故障转移历史:")
	if len(failoverHistory) == 0 {
		fmt.Println("无故障转移事件")
	} else {
		for i, event := range failoverHistory {
			fmt.Printf("%d. %s -> %s (时间: %s, 原因: %s, 成功: %v)\n",
				i+1, event.OldPrimaryID, event.NewPrimaryID,
				event.Timestamp.Format("15:04:05"), event.Reason, event.Success)
		}
	}

	fmt.Println("\n测试完成")
}

// 注册测试节点
func registerTestNodes(fm *failover.FailoverManager) {
	// 注册主节点
	primary := &failover.Node{
		ID:         "node1",
		Address:    "192.168.1.1",
		Port:       3306,
		Role:       "primary",
		State:      failover.NodeStateUp,
		CPU:        30.0,
		Memory:     40.0,
		DiskUsage:  50.0,
		JoinedTime: time.Now(),
		Metadata: map[string]string{
			"zone": "zone1",
		},
	}
	fm.RegisterNode(primary)
	fmt.Printf("注册节点: %s (角色: %s)\n", primary.ID, primary.Role)

	// 注册副本节点
	for i := 2; i <= 4; i++ {
		node := &failover.Node{
			ID:         fmt.Sprintf("node%d", i),
			Address:    fmt.Sprintf("192.168.1.%d", i),
			Port:       3306,
			Role:       "replica",
			State:      failover.NodeStateUp,
			CPU:        20.0 + float64(i*5),
			Memory:     30.0 + float64(i*5),
			DiskUsage:  40.0 + float64(i*5),
			JoinedTime: time.Now(),
			Metadata: map[string]string{
				"zone": fmt.Sprintf("zone%d", (i%2)+1),
			},
		}
		fm.RegisterNode(node)
		fmt.Printf("注册节点: %s (角色: %s)\n", node.ID, node.Role)
	}
}

// 模拟节点负载变化
func simulateNodeLoad(fm *failover.FailoverManager, stopChan <-chan struct{}, done chan<- struct{}) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	defer close(done)

	scenario := 0
	scenarioTicker := time.NewTicker(15 * time.Second)
	defer scenarioTicker.Stop()

	for {
		select {
		case <-stopChan:
			return
		case <-scenarioTicker.C:
			scenario = (scenario + 1) % 3
			switch scenario {
			case 0:
				fmt.Println("\n场景：正常操作")
			case 1:
				fmt.Println("\n场景：主节点负载增加")
			case 2:
				fmt.Println("\n场景：主节点故障")
			}
		case <-ticker.C:
			nodes := fm.GetAllNodes()

			for id, node := range nodes {
				switch scenario {
				case 0: // 正常操作
					// 添加一些随机变化
					node.CPU = min(70.0, 20.0+rand.Float64()*20.0)
					node.Memory = min(70.0, 30.0+rand.Float64()*20.0)
					node.DiskUsage = min(70.0, 40.0+rand.Float64()*20.0)
				case 1: // 主节点负载增加
					if node.Role == "primary" {
						node.CPU = min(95.0, node.CPU+rand.Float64()*10.0)
						node.Memory = min(95.0, node.Memory+rand.Float64()*5.0)
						node.DiskUsage = min(90.0, node.DiskUsage+rand.Float64()*3.0)
					} else {
						node.CPU = min(70.0, 20.0+rand.Float64()*20.0)
						node.Memory = min(70.0, 30.0+rand.Float64()*20.0)
						node.DiskUsage = min(70.0, 40.0+rand.Float64()*20.0)
					}
				case 2: // 主节点故障
					if node.Role == "primary" {
						// 模拟节点故障
						node.State = failover.NodeStateDown
						node.CPU = 100.0
						node.Memory = 100.0
						node.DiskUsage = 100.0
					} else {
						// 保持副本节点正常
						node.State = failover.NodeStateUp
						node.CPU = min(60.0, 20.0+rand.Float64()*20.0)
						node.Memory = min(60.0, 30.0+rand.Float64()*20.0)
						node.DiskUsage = min(60.0, 40.0+rand.Float64()*20.0)
					}
				}

				// 更新节点指标
				fm.UpdateNodeMetrics(id, node.CPU, node.Memory, node.DiskUsage)

				// 手动更新状态（实际情况下这由健康检查器完成）
				if scenario == 2 && node.Role == "primary" {
					fm.UpdateNodeState(id, failover.NodeStateDown)
				}

				// 打印节点状态
				fmt.Printf("\r节点 %-5s [角色: %-7s 状态: %-10s] CPU: %5.1f%% 内存: %5.1f%% 磁盘: %5.1f%%",
					id, node.Role, node.State, node.CPU, node.Memory, node.DiskUsage)
			}
			fmt.Println()
		}
	}
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
