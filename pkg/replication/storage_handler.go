package replication

import (
	"fmt"

	"github.com/cyberdb/cyberdb/pkg/common/logger"
	"github.com/cyberdb/cyberdb/pkg/storage"
)

// StorageReplicationHandler 实现存储引擎复制处理器接口
type StorageReplicationHandler struct {
	manager *Manager
}

// NewStorageReplicationHandler 创建存储复制处理器
func NewStorageReplicationHandler(manager *Manager) *StorageReplicationHandler {
	return &StorageReplicationHandler{
		manager: manager,
	}
}

// ReplicateOperation 实现复制操作接口
func (h *StorageReplicationHandler) ReplicateOperation(
	operation string,
	dbName,
	tableName string,
	key,
	value []byte,
	schema *storage.TableSchema) error {

	// 创建Raft命令
	cmd := &RaftCommand{
		Operation: operation,
		DB:        dbName,
		Table:     tableName,
		Key:       key,
		Value:     value,
		Schema:    schema,
	}

	// 应用命令到Raft
	err := h.manager.ApplyCommand(cmd)
	if err != nil {
		logger.Error(fmt.Sprintf("复制操作 %s 失败: %s", operation, err.Error()))
		return err
	}

	return nil
}

// SetupStorageReplication 为存储引擎设置复制
func SetupStorageReplication(storageEngine storage.Engine, replicationManager *Manager) error {
	// 创建复制处理器
	handler := NewStorageReplicationHandler(replicationManager)

	// 将处理器设置到存储引擎
	storageEngine.SetReplicationHandler(handler)

	logger.Info("存储引擎复制集成设置完成")
	return nil
}
