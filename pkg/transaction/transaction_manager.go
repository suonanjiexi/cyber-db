package transaction

import (
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/suonanjiexi/cyber-db/pkg/storage"
)

// NewManager 创建一个新的事务管理器
func NewManager(db *storage.DB, defaultTimeout time.Duration) *Manager {
	// 创建结果缓存
	resultCache, _ := lru.New[string, []byte](10000)

	// 创建分片锁，提高并发性能
	shardCount := 32 // 使用32个分片锁
	shardMutexes := make([]sync.RWMutex, shardCount)

	m := &Manager{
		db:                 db,
		activeTransactions: make(map[string]*Transaction),
		defaultTimeout:     defaultTimeout,
		defaultIsolation:   RepeatableRead, // 默认使用可重复读隔离级别
		lockManager:        NewLockManager(defaultTimeout),
		cleanupTicker:      time.NewTicker(defaultTimeout / 2), // 定期清理的间隔为超时时间的一半
		statistics:         &ManagerStats{},
		shardCount:         shardCount,
		shardMutexes:       shardMutexes,
		deadlockDetector:   NewDeadlockDetector(),
		resultCache:        resultCache,
		monitorTicker:      time.NewTicker(10 * time.Second), // 每10秒监控一次
		slowTxThreshold:    5 * time.Second,                  // 慢事务阈值
		activeAlerts:       make(map[string]time.Time),
	}

	// 启动后台清理goroutine
	go m.cleanupRoutine()

	// 启动事务监控
	go m.monitorRoutine()

	return m
}

// monitorRoutine 监控活跃事务
func (m *Manager) monitorRoutine() {
	for range m.monitorTicker.C {
		m.mutex.RLock()
		activeTxs := make([]*Transaction, 0, len(m.activeTransactions))
		for _, tx := range m.activeTransactions {
			activeTxs = append(activeTxs, tx)
		}
		m.mutex.RUnlock()

		now := time.Now()

		// 检查长时间运行的事务
		for _, tx := range activeTxs {
			tx.mutex.RLock()
			txID := tx.id
			duration := now.Sub(tx.startTime)
			isActive := tx.status == TxStatusActive
			tx.mutex.RUnlock()

			if isActive && duration > m.slowTxThreshold {
				m.alertMutex.Lock()
				_, alreadyAlerted := m.activeAlerts[txID]
				if !alreadyAlerted {
					m.activeAlerts[txID] = now
					// 这里可以添加告警逻辑，如日志记录或发送通知
				}
				m.alertMutex.Unlock()
			}
		}

		// 清理已完成的告警
		m.alertMutex.Lock()
		for txID := range m.activeAlerts {
			exists := false
			for _, tx := range activeTxs {
				if tx.id == txID {
					exists = true
					break
				}
			}
			if !exists {
				delete(m.activeAlerts, txID)
			}
		}
		m.alertMutex.Unlock()
	}
}

// Begin 开始一个新事务
func (m *Manager) Begin() (*Transaction, error) {
	return m.BeginWithIsolation(m.defaultIsolation)
}

// BeginWithIsolation 以指定隔离级别开始一个新事务
func (m *Manager) BeginWithIsolation(isolationLevel int) (*Transaction, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 生成唯一事务ID
	id := generateTxID()

	tx := &Transaction{
		id:             id,
		status:         TxStatusActive,
		startTime:      time.Now(),
		timeout:        m.defaultTimeout,
		db:             m.db,
		writeSet:       make(map[string][]byte),
		readSet:        make(map[string]struct{}),
		isolationLevel: isolationLevel,
		readCache:      make(map[string][]byte),
		statistics:     &TxStats{},
		priority:       int(time.Now().UnixNano() % 100), // 简单的优先级分配
		dependencies:   make(map[string]struct{}),
	}

	m.activeTransactions[id] = tx

	// 更新统计信息
	m.statistics.mutex.Lock()
	m.statistics.TxStarted++
	m.statistics.mutex.Unlock()

	return tx, nil
}

// cleanupRoutine 定期清理超时的事务
func (m *Manager) cleanupRoutine() {
	for range m.cleanupTicker.C {
		// 使用读锁获取活跃事务列表，减少锁竞争
		m.mutex.RLock()
		activeIDs := make([]string, 0, len(m.activeTransactions))
		activeTxs := make([]*Transaction, 0, len(m.activeTransactions))

		for id, tx := range m.activeTransactions {
			activeIDs = append(activeIDs, id)
			activeTxs = append(activeTxs, tx)
		}
		m.mutex.RUnlock()

		// 检查每个事务，不需要持有全局锁
		toRemove := make([]string, 0)
		abortedCount := 0

		for i, id := range activeIDs {
			tx := activeTxs[i]
			tx.mutex.RLock()
			isTimeout := tx.status == TxStatusActive && time.Since(tx.startTime) > tx.timeout
			isCompleted := tx.status == TxStatusCommitted || tx.status == TxStatusAborted
			tx.mutex.RUnlock()

			if isTimeout {
				// 获取写锁来修改事务状态
				tx.mutex.Lock()
				// 再次检查状态，避免竞态条件
				if tx.status == TxStatusActive && time.Since(tx.startTime) > tx.timeout {
					tx.status = TxStatusAborted
					abortedCount++

					// 更新统计信息
					m.statistics.mutex.Lock()
					m.statistics.TxTimeout++
					m.statistics.TxAborted++
					m.statistics.mutex.Unlock()
				}
				tx.mutex.Unlock()
			}

			if isTimeout || isCompleted {
				toRemove = append(toRemove, id)
			}
		}

		// 删除需要清理的事务
		if len(toRemove) > 0 {
			m.mutex.Lock()
			for _, id := range toRemove {
				delete(m.activeTransactions, id)
			}
			m.mutex.Unlock()
		}

		if abortedCount > 0 {
			// 记录超时事务数量
			// 这里可以添加日志记录或告警
		}
	}
}

// GetTransaction 根据ID获取事务
func (m *Manager) GetTransaction(id string) (*Transaction, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	tx, exists := m.activeTransactions[id]
	if !exists {
		return nil, ErrTxNotActive
	}

	return tx, nil
}

// GetStats 获取事务管理器统计信息
func (m *Manager) GetStats() ManagerStats {
	m.statistics.mutex.Lock()
	defer m.statistics.mutex.Unlock()

	return ManagerStats{
		TxStarted:   m.statistics.TxStarted,
		TxCommitted: m.statistics.TxCommitted,
		TxAborted:   m.statistics.TxAborted,
		TxTimeout:   m.statistics.TxTimeout,
	}
}
