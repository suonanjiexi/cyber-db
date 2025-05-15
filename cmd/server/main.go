package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/cyberdb/cyberdb/pkg/cluster"
	"github.com/cyberdb/cyberdb/pkg/common/config"
	"github.com/cyberdb/cyberdb/pkg/common/logger"
	"github.com/cyberdb/cyberdb/pkg/compute"
	"github.com/cyberdb/cyberdb/pkg/protocol"
	"github.com/cyberdb/cyberdb/pkg/replication"
	"github.com/cyberdb/cyberdb/pkg/storage"
)

var (
	configFile string
)

func init() {
	flag.StringVar(&configFile, "config", "configs/single-node.yaml", "配置文件路径")
}

func main() {
	flag.Parse()

	// 加载配置
	cfg, err := config.LoadConfig(configFile)
	if err != nil {
		fmt.Printf("加载配置错误: %v\n", err)
		os.Exit(1)
	}

	// 设置全局配置
	config.SetCurrentConfig(cfg)

	// 初始化日志
	if err := logger.Init(logger.Config{
		Level:    cfg.Log.Level,
		Output:   cfg.Log.Output,
		FilePath: cfg.Log.FilePath,
	}); err != nil {
		fmt.Printf("初始化日志系统错误: %v\n", err)
		os.Exit(1)
	}

	logger.Info("CyberDB 初始化中...")

	// 创建集群管理器
	clusterManager := cluster.NewManager(cfg)
	if err := clusterManager.Start(); err != nil {
		logger.Error("启动集群管理器错误: " + err.Error())
		os.Exit(1)
	}
	defer clusterManager.Stop()

	// 创建存储引擎
	var storageEngine storage.Engine
	switch cfg.Storage.Engine {
	case "memory":
		logger.Info("使用内存存储引擎")
		storageEngine = storage.NewMemoryEngine()
	default:
		logger.Error("不支持的存储引擎: " + cfg.Storage.Engine)
		os.Exit(1)
	}

	// 打开存储引擎
	if err := storageEngine.Open(); err != nil {
		logger.Error("打开存储引擎错误: " + err.Error())
		os.Exit(1)
	}
	defer storageEngine.Close()

	// 创建复制管理器
	replicationManager := replication.NewManager(cfg)
	// 设置存储引擎
	replicationManager.SetStorageEngine(storageEngine)
	// 启动复制管理器
	if err := replicationManager.Start(); err != nil {
		logger.Error("启动复制管理器错误: " + err.Error())
		os.Exit(1)
	}
	defer replicationManager.Stop()

	// 设置存储引擎的复制处理
	if cfg.Replication.Enabled {
		if err := replication.SetupStorageReplication(storageEngine, replicationManager); err != nil {
			logger.Error("设置存储引擎复制失败: " + err.Error())
			os.Exit(1)
		}
	}

	// 创建计算引擎
	computeEngine := compute.NewLocalEngine(storageEngine)
	defer computeEngine.Close()

	// 创建并配置MySQL协议服务器
	mysqlAddr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.MySQLPort)
	mysqlServer := protocol.NewMySQLServer(mysqlAddr)

	// 将计算引擎设置到MySQL协议服务器中
	protocol.SetComputeEngine(computeEngine)

	// 只有在是主节点或者单节点模式下才启动MySQL服务器
	if !cfg.Cluster.Enabled || clusterManager.IsLeader() || replicationManager.IsLeader() {
		// 启动MySQL协议服务器
		if err := mysqlServer.Start(); err != nil {
			logger.Error("启动MySQL服务器错误: " + err.Error())
			os.Exit(1)
		}
		logger.Info(fmt.Sprintf("CyberDB 启动成功，监听地址: %s", mysqlAddr))
	} else {
		logger.Info("以从节点模式启动，不提供MySQL服务")
	}

	// 打印系统状态
	printSystemStatus(cfg, clusterManager, replicationManager)

	// 监听退出信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("正在关闭服务器...")

	// 关闭MySQL协议服务器
	if !cfg.Cluster.Enabled || clusterManager.IsLeader() || replicationManager.IsLeader() {
		if err := mysqlServer.Stop(); err != nil {
			logger.Error("关闭MySQL服务器错误: " + err.Error())
		}
	}

	logger.Info("服务器已关闭")
}

// printSystemStatus 打印系统状态
func printSystemStatus(cfg *config.Config, clusterMgr *cluster.Manager, replicationMgr *replication.Manager) {
	// 确定节点角色
	role := "单节点"
	if cfg.Cluster.Enabled {
		// 首先检查集群管理器状态
		if clusterMgr.IsLeader() {
			role = "主节点(集群管理)"
		} else {
			// 然后检查复制管理器状态
			if replicationMgr.IsLeader() {
				role = "主节点(Raft复制)"
			} else {
				role = "从节点"
			}
		}
	}

	// 获取复制状态
	replicationStatus := "未启用"
	if cfg.Replication.Enabled {
		switch replicationMgr.GetStatus() {
		case replication.ReplicationStatusRunning:
			replicationStatus = "运行中"
		case replication.ReplicationStatusPaused:
			replicationStatus = "已暂停"
		case replication.ReplicationStatusError:
			replicationStatus = "错误"
		case replication.ReplicationStatusInitializing:
			replicationStatus = "初始化中"
		}

		// 添加Raft主节点信息
		if leader := replicationMgr.GetLeader(); leader != "" {
			replicationStatus += fmt.Sprintf(" (Raft主节点: %s)", leader)
		}
	}

	logger.Info(fmt.Sprintf("系统状态: 节点ID=%s, 角色=%s, 存储引擎=%s, 复制状态=%s",
		cfg.Server.ID, role, cfg.Storage.Engine, replicationStatus))
}
