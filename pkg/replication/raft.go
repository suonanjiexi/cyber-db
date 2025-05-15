package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cyberdb/cyberdb/pkg/common/config"
	"github.com/cyberdb/cyberdb/pkg/common/logger"
	"github.com/cyberdb/cyberdb/pkg/storage"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// RaftReplicator 实现基于Raft的数据复制
type RaftReplicator struct {
	// nodeID 节点ID
	nodeID string

	// addr 节点地址
	addr string

	// dataDir 数据目录
	dataDir string

	// raft Raft实例
	raft *raft.Raft

	// storage 存储引擎
	storage storage.Engine

	// ctx 上下文
	ctx context.Context

	// cancel 取消函数
	cancel context.CancelFunc

	// fsm Raft状态机
	fsm *RaftFSM

	// mu 互斥锁
	mu sync.RWMutex

	// config 配置
	config *config.Config
}

// NewRaftReplicator 创建新的Raft复制器
func NewRaftReplicator(cfg *config.Config, storageEngine storage.Engine) (*RaftReplicator, error) {
	ctx, cancel := context.WithCancel(context.Background())

	return &RaftReplicator{
		nodeID:  cfg.Server.ID,
		addr:    fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Replication.Port),
		dataDir: cfg.Storage.DataDir,
		ctx:     ctx,
		cancel:  cancel,
		storage: storageEngine,
		config:  cfg,
		fsm:     NewRaftFSM(storageEngine),
	}, nil
}

// Start 启动Raft复制
func (r *RaftReplicator) Start() error {
	// Raft配置
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(r.nodeID)

	// 创建传输层
	addr, err := net.ResolveTCPAddr("tcp", r.addr)
	if err != nil {
		return fmt.Errorf("解析地址失败: %w", err)
	}

	transport, err := raft.NewTCPTransport(r.addr, addr, 3, 10*time.Second, nil)
	if err != nil {
		return fmt.Errorf("创建传输层失败: %w", err)
	}

	// 创建快照存储
	snapshotStore, err := raft.NewFileSnapshotStore(r.dataDir, 3, nil)
	if err != nil {
		return fmt.Errorf("创建快照存储失败: %w", err)
	}

	// 创建日志存储
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(r.dataDir, "raft-log.bolt"))
	if err != nil {
		return fmt.Errorf("创建日志存储失败: %w", err)
	}

	// 创建稳定存储
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(r.dataDir, "raft-stable.bolt"))
	if err != nil {
		return fmt.Errorf("创建稳定存储失败: %w", err)
	}

	// 创建Raft实例
	r.raft, err = raft.NewRaft(raftConfig, r.fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return fmt.Errorf("创建Raft实例失败: %w", err)
	}

	// 如果是集群模式，需要配置集群节点
	if r.config.Cluster.Enabled {
		// 创建集群配置
		servers := make([]raft.Server, 0, len(r.config.Cluster.Members))
		for _, member := range r.config.Cluster.Members {
			addr := member.Address
			// 如果地址不包含端口，则添加复制端口
			if _, _, err := net.SplitHostPort(addr); err != nil {
				addr = fmt.Sprintf("%s:%d", addr, r.config.Replication.Port)
			}

			server := raft.Server{
				ID:      raft.ServerID(member.ID),
				Address: raft.ServerAddress(addr),
			}
			servers = append(servers, server)
		}

		// 创建集群配置
		configuration := raft.Configuration{
			Servers: servers,
		}

		// 引导集群（仅在初始化时）
		if isLeader := r.config.Cluster.Members[0].ID == r.nodeID; isLeader {
			future := r.raft.BootstrapCluster(configuration)
			if err := future.Error(); err != nil && err != raft.ErrCantBootstrap {
				return fmt.Errorf("引导集群失败: %w", err)
			}
		}
	} else {
		// 单节点模式
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(r.nodeID),
					Address: raft.ServerAddress(r.addr),
				},
			},
		}
		future := r.raft.BootstrapCluster(configuration)
		if err := future.Error(); err != nil && err != raft.ErrCantBootstrap {
			return fmt.Errorf("引导单节点集群失败: %w", err)
		}
	}

	// 启动状态监控
	go r.monitorLeaderState()

	return nil
}

// Stop 停止Raft复制
func (r *RaftReplicator) Stop() error {
	r.cancel()

	// 关闭Raft实例
	if r.raft != nil {
		future := r.raft.Shutdown()
		return future.Error()
	}

	return nil
}

// IsLeader 检查是否为主节点
func (r *RaftReplicator) IsLeader() bool {
	return r.raft.State() == raft.Leader
}

// GetLeader 获取主节点地址
func (r *RaftReplicator) GetLeader() string {
	return string(r.raft.Leader())
}

// ApplyCommand 应用命令
func (r *RaftReplicator) ApplyCommand(cmd []byte) error {
	if !r.IsLeader() {
		return fmt.Errorf("当前节点不是主节点，无法应用命令")
	}

	// 应用命令
	future := r.raft.Apply(cmd, 30*time.Second)
	return future.Error()
}

// monitorLeaderState 监控主节点状态
func (r *RaftReplicator) monitorLeaderState() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			state := r.raft.State()
			leader := r.raft.Leader()
			logger.Info(fmt.Sprintf("Raft状态: %s, 主节点: %s", state, leader))
		}
	}
}

// RaftCommand 表示Raft命令
type RaftCommand struct {
	Operation string               // 操作类型
	DB        string               // 数据库名
	Table     string               // 表名
	Key       []byte               // 键
	Value     []byte               // 值
	Fields    map[string][]byte    // 字段
	Schema    *storage.TableSchema // 表结构
}

// RaftFSM 实现Raft有限状态机
type RaftFSM struct {
	storage storage.Engine
	mu      sync.Mutex
}

// NewRaftFSM 创建新的Raft状态机
func NewRaftFSM(storageEngine storage.Engine) *RaftFSM {
	return &RaftFSM{
		storage: storageEngine,
	}
}

// Apply 应用日志
func (f *RaftFSM) Apply(log *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	// 解析命令
	var cmd RaftCommand
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		logger.Error("解析Raft命令失败: " + err.Error())
		return err
	}

	ctx := context.Background()

	// 根据操作类型执行不同的操作
	switch cmd.Operation {
	case "CreateDB":
		err := f.storage.CreateDB(ctx, cmd.DB)
		return err
	case "DropDB":
		err := f.storage.DropDB(ctx, cmd.DB)
		return err
	case "CreateTable":
		err := f.storage.CreateTable(ctx, cmd.DB, cmd.Table, cmd.Schema)
		return err
	case "DropTable":
		err := f.storage.DropTable(ctx, cmd.DB, cmd.Table)
		return err
	case "Put":
		err := f.storage.Put(ctx, cmd.DB, cmd.Table, cmd.Key, cmd.Value)
		return err
	case "Delete":
		err := f.storage.Delete(ctx, cmd.DB, cmd.Table, cmd.Key)
		return err
	default:
		err := fmt.Errorf("未知操作类型: %s", cmd.Operation)
		logger.Error(err.Error())
		return err
	}
}

// Snapshot 创建快照
func (f *RaftFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	logger.Info("开始创建Raft状态快照")

	// 从存储引擎获取状态快照
	// 这里我们假设存储引擎有一个导出状态的方法，实际情况下需要根据存储引擎的API调整
	// 简化版实现，后续需要根据实际存储引擎扩展
	snapshot := &RaftSnapshot{
		databases: make(map[string]map[string]map[string][]byte),
	}

	// 这里仅作为演示，实际实现应该遍历存储引擎中的所有数据库和表
	// 为了简化，我们只创建一个空的快照结构，后续可以扩展为实际数据

	logger.Info("Raft状态快照创建完成")
	return snapshot, nil
}

// Restore 从快照恢复
func (f *RaftFSM) Restore(rdr io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer rdr.Close()

	logger.Info("开始从快照恢复Raft状态")

	// 从快照读取器中读取数据并恢复到存储引擎
	// 实际实现应该解析快照数据并应用到存储引擎
	// 简化版实现，仅读取快照但不做实际操作

	// 读取快照数据
	var snapshot RaftSnapshotData
	if err := json.NewDecoder(rdr).Decode(&snapshot); err != nil {
		logger.Error("解析快照数据失败: " + err.Error())
		return err
	}

	// 恢复数据到存储引擎
	// 这里仅作为演示，实际实现应该遍历快照中的所有数据并应用到存储引擎
	ctx := context.Background()

	// 清空当前存储引擎数据（如果需要）
	// 注意：实际实现可能需要更复杂的策略，比如增量更新而不是全部清空

	// 遍历快照中的数据库
	for dbName, tables := range snapshot.Databases {
		// 创建数据库（如果不存在）
		if err := f.storage.CreateDB(ctx, dbName); err != nil {
			// 如果数据库已存在，忽略错误
			if !strings.Contains(err.Error(), "已存在") {
				logger.Error("恢复数据库失败: " + err.Error())
				continue
			}
		}

		// 遍历数据库中的表
		for tableName, data := range tables {
			// 这里假设我们有表结构信息，实际实现需要存储表结构
			// 创建表（如果不存在）
			// 注意：这里简化处理，实际实现需要先获取或恢复表结构

			// 遍历表中的数据
			for key, value := range data {
				if err := f.storage.Put(ctx, dbName, tableName, []byte(key), value); err != nil {
					logger.Error(fmt.Sprintf("恢复数据失败 [%s.%s.%s]: %s",
						dbName, tableName, key, err.Error()))
					continue
				}
			}
		}
	}

	logger.Info("Raft状态从快照恢复完成")
	return nil
}

// RaftSnapshot 实现Raft快照
type RaftSnapshot struct {
	// 存储数据库 -> 表 -> 键 -> 值的映射
	databases map[string]map[string]map[string][]byte
}

// RaftSnapshotData 表示快照数据格式
type RaftSnapshotData struct {
	// 数据库映射: dbName -> tableName -> key -> value
	Databases map[string]map[string]map[string][]byte `json:"databases"`
	// 表结构映射: dbName -> tableName -> schema
	Schemas map[string]map[string]*storage.TableSchema `json:"schemas"`
	// 版本信息
	Version string `json:"version"`
	// 创建时间
	CreatedAt time.Time `json:"created_at"`
}

// Persist 持久化快照
func (s *RaftSnapshot) Persist(sink raft.SnapshotSink) error {
	logger.Info("开始持久化Raft快照")

	// 创建快照数据
	snapshotData := RaftSnapshotData{
		Databases: s.databases,
		Schemas:   make(map[string]map[string]*storage.TableSchema),
		Version:   "1.0",
		CreatedAt: time.Now(),
	}

	// 序列化快照数据
	data, err := json.Marshal(snapshotData)
	if err != nil {
		logger.Error("序列化快照数据失败: " + err.Error())
		sink.Cancel()
		return err
	}

	// 写入快照数据
	if _, err := sink.Write(data); err != nil {
		logger.Error("写入快照数据失败: " + err.Error())
		sink.Cancel()
		return err
	}

	// 关闭快照
	if err := sink.Close(); err != nil {
		logger.Error("关闭快照失败: " + err.Error())
		return err
	}

	logger.Info("Raft快照持久化完成")
	return nil
}

// Release 释放快照资源
func (s *RaftSnapshot) Release() {
	// 清理资源
	s.databases = nil
}
