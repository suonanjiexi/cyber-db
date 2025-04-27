package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/suonanjiexi/cyber-db/pkg/failover"
	"github.com/suonanjiexi/cyber-db/pkg/server"
	"github.com/suonanjiexi/cyber-db/pkg/storage"
)

func main() {
	// 命令行参数
	var (
		host               string
		port               int
		dataDir            string
		clusterMode        bool
		nodeID             string
		clusterAddr        string
		joinAddr           string
		enableAutoFailover bool
		maxConnections     int
		charset            string
	)

	// 解析命令行参数
	flag.StringVar(&host, "host", "localhost", "服务器主机地址")
	flag.IntVar(&port, "port", 3306, "服务器端口")
	flag.StringVar(&dataDir, "db", "./data/db.data", "数据库文件路径")
	flag.BoolVar(&clusterMode, "cluster-mode", false, "是否启用集群模式")
	flag.StringVar(&nodeID, "node-id", "", "节点ID，集群模式下必须提供")
	flag.StringVar(&clusterAddr, "cluster-addr", "", "集群通信地址，格式为host:port")
	flag.StringVar(&joinAddr, "join", "", "要加入的集群节点地址")
	flag.BoolVar(&enableAutoFailover, "auto-failover", true, "是否启用自动故障转移")
	flag.IntVar(&maxConnections, "max-connections", 100, "最大连接数")
	flag.StringVar(&charset, "charset", "utf8mb4", "字符集")
	flag.Parse()

	// 验证参数
	if clusterMode {
		if nodeID == "" {
			log.Fatal("集群模式下必须提供节点ID")
		}
		if clusterAddr == "" {
			log.Fatal("集群模式下必须提供集群通信地址")
		}
	}

	// 初始化日志
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.Println("CyberDB 启动中...")
	log.Printf("主机: %s, 端口: %d, 数据目录: %s", host, port, dataDir)

	if clusterMode {
		log.Printf("集群模式启用, 节点ID: %s, 通信地址: %s", nodeID, clusterAddr)
		if joinAddr != "" {
			log.Printf("正在加入现有集群, 连接节点: %s", joinAddr)
		} else {
			log.Println("初始化新集群")
		}
	}

	// 初始化存储引擎
	// 使用filepath包处理路径，以确保跨平台兼容性
	dataDir = filepath.Clean(dataDir)
	dataPath := dataDir

	// 确保数据库目录存在
	dbDir := filepath.Dir(dataPath)
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		log.Fatalf("创建数据库目录失败: %v", err)
	}

	db := storage.NewDB(dataPath, true)
	if err := db.Open(); err != nil {
		log.Fatalf("存储引擎启动失败: %v", err)
	}
	defer func(db *storage.DB) {
		err := db.Close()
		if err != nil {
			log.Printf("存储引擎关闭出错: %v", err)
		}
	}(db)

	// 初始化故障转移管理器
	failoverConfig := failover.FailoverConfig{
		EnableAutoFailover: enableAutoFailover,
		FailoverTimeout:    60 * time.Second,
		MaxFailovers:       3,
		FailoverPeriod:     1 * time.Hour,
		WaitTime:           5 * time.Second,
	}
	fm := failover.NewFailoverManager(failoverConfig)

	// 初始化健康检查器
	healthConfig := failover.HealthConfig{
		CheckInterval:      5 * time.Second,
		FailureThreshold:   3,
		RecoveryThreshold:  2,
		CPUThreshold:       90.0,
		MemoryThreshold:    85.0,
		DiskThreshold:      90.0,
		EnableAutoFailover: enableAutoFailover,
	}
	hc := failover.NewHealthChecker(healthConfig, fm)

	// 注册当前节点
	if clusterMode {
		node := &failover.Node{
			ID:         nodeID,
			Address:    host,
			Port:       port,
			Role:       determineNodeRole(joinAddr),
			State:      failover.NodeStateUp,
			CPU:        0,
			Memory:     0,
			DiskUsage:  0,
			JoinedTime: time.Now(),
			Metadata: map[string]string{
				"version": "1.0.0",
				"os":      getOSInfo(),
			},
		}
		if err := fm.RegisterNode(node); err != nil {
			log.Fatalf("注册节点失败: %v", err)
		}
	}

	// 启动健康检查
	if clusterMode {
		hc.Start()
		defer hc.Stop()
		log.Println("健康检查器已启动")
	}

	// 初始化服务器（使用完整版服务器）
	srv := server.NewServer(host, port, db)
	srv.SetMaxConnections(maxConnections)

	// 设置连接超时和查询超时
	srv.SetConnTimeout(5 * time.Minute)
	srv.SetQueryTimeout(30 * time.Second)

	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("服务器启动失败: %v", err)
		}
	}()

	log.Printf("CyberDB 服务器已启动，监听地址: %s:%d\n", host, port)
	log.Printf("现在可以使用SQL客户端（如Navicat）连接到服务器\n")

	// 等待信号退出
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	log.Println("正在关闭服务器...")

	// 关闭服务器
	if err := srv.Stop(); err != nil {
		log.Printf("服务器关闭出错: %v", err)
	}

	log.Println("CyberDB 已安全关闭")
}

// 确定节点角色
func determineNodeRole(joinAddr string) string {
	if joinAddr == "" {
		return "primary" // 如果是新集群，第一个节点为主节点
	}
	return "replica" // 加入现有集群的节点为副本节点
}

// 获取操作系统信息
func getOSInfo() string {
	return fmt.Sprintf("%s/%s", os.Getenv("GOOS"), os.Getenv("GOARCH"))
}
