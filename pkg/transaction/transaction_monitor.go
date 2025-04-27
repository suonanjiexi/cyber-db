package transaction

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// TransactionMonitor 事务监控器
type TransactionMonitor struct {
	manager         *Manager
	tccCoordinator  *TCCCoordinator
	distCoordinator *DistributedCoordinator
	monitorInterval time.Duration
	slowTxThreshold time.Duration
	longTxThreshold time.Duration
	activeAlerts    map[string]time.Time
	alertMutex      sync.Mutex
	metrics         *TransactionMetrics
	stopCh          chan struct{}
}

// TransactionMetrics 事务指标
type TransactionMetrics struct {
	ActiveTransactions    int64
	CompletedTransactions int64
	FailedTransactions    int64
	AvgTxDuration         time.Duration
	SlowTransactions      int64
	LongRunningTxs        int64
	mutex                 sync.Mutex
}

// NewTransactionMonitor 创建事务监控器
func NewTransactionMonitor(
	manager *Manager,
	tccCoordinator *TCCCoordinator,
	distCoordinator *DistributedCoordinator,
) *TransactionMonitor {
	return &TransactionMonitor{
		manager:         manager,
		tccCoordinator:  tccCoordinator,
		distCoordinator: distCoordinator,
		monitorInterval: 10 * time.Second,
		slowTxThreshold: 5 * time.Second,
		longTxThreshold: 30 * time.Second,
		activeAlerts:    make(map[string]time.Time),
		metrics:         &TransactionMetrics{},
		stopCh:          make(chan struct{}),
	}
}

// Start 启动事务监控
func (tm *TransactionMonitor) Start() {
	go tm.monitorRoutine()
	log.Println("事务监控器已启动")
}

// Stop 停止事务监控
func (tm *TransactionMonitor) Stop() {
	close(tm.stopCh)
	log.Println("事务监控器已停止")
}

// monitorRoutine 监控例程
func (tm *TransactionMonitor) monitorRoutine() {
	ticker := time.NewTicker(tm.monitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tm.collectMetrics()
			tm.checkSlowTransactions()
			tm.checkLongRunningTransactions()
		case <-tm.stopCh:
			return
		}
	}
}

// collectMetrics 收集事务指标
func (tm *TransactionMonitor) collectMetrics() {
	// 收集本地事务指标
	localTxCount := 0
	completedCount := 0
	failedCount := 0

	tm.manager.mutex.RLock()
	localTxCount = len(tm.manager.activeTransactions)
	tm.manager.mutex.RUnlock()

	// 收集TCC事务指标
	tccTxs := tm.tccCoordinator.GetAllTransactions()
	for _, tx := range tccTxs {
		if tx.State == TCCStateConfirmed {
			completedCount++
		} else if tx.State == TCCStateCancelled || tx.State == TCCStateFailed {
			failedCount++
		}
	}

	// 收集分布式事务指标
	distTxs := tm.distCoordinator.GetAllTransactions()
	for _, tx := range distTxs {
		if tx.State == TxCommitted {
			completedCount++
		} else if tx.State == TxAborted {
			failedCount++
		}
	}

	// 更新指标
	tm.metrics.mutex.Lock()
	tm.metrics.ActiveTransactions = int64(localTxCount + len(tccTxs) + len(distTxs) - completedCount - failedCount)
	tm.metrics.CompletedTransactions += int64(completedCount)
	tm.metrics.FailedTransactions += int64(failedCount)
	tm.metrics.mutex.Unlock()

	log.Printf("事务指标 - 活跃: %d, 已完成: %d, 失败: %d, 慢事务: %d, 长时间运行: %d",
		tm.metrics.ActiveTransactions,
		tm.metrics.CompletedTransactions,
		tm.metrics.FailedTransactions,
		tm.metrics.SlowTransactions,
		tm.metrics.LongRunningTxs)
}

// checkSlowTransactions 检查慢事务
func (tm *TransactionMonitor) checkSlowTransactions() {
	slowCount := 0

	// 检查本地事务
	tm.manager.mutex.RLock()
	for txID, tx := range tm.manager.activeTransactions {
		tx.mutex.RLock()
		if tx.status == TxStatusCommitted {
			duration := tx.statistics.TotalWriteTime + tx.statistics.TotalReadTime
			if duration > tm.slowTxThreshold {
				slowCount++
				tm.alertSlowTransaction(txID, "本地事务", duration)
			}
		}
		tx.mutex.RUnlock()
	}
	tm.manager.mutex.RUnlock()

	// 检查TCC事务
	tccTxs := tm.tccCoordinator.GetAllTransactions()
	for _, tx := range tccTxs {
		if tx.State == TCCStateConfirmed || tx.State == TCCStateCancelled {
			duration := tx.EndTime.Sub(tx.StartTime)
			if duration > tm.slowTxThreshold {
				slowCount++
				tm.alertSlowTransaction(tx.ID, "TCC事务", duration)
			}
		}
	}

	// 检查分布式事务
	distTxs := tm.distCoordinator.GetAllTransactions()
	for _, tx := range distTxs {
		if tx.State == TxCommitted || tx.State == TxAborted {
			duration := tx.EndTime.Sub(tx.StartTime)
			if duration > tm.slowTxThreshold {
				slowCount++
				tm.alertSlowTransaction(tx.ID, "分布式事务", duration)
			}
		}
	}

	// 更新慢事务计数
	tm.metrics.mutex.Lock()
	tm.metrics.SlowTransactions = int64(slowCount)
	tm.metrics.mutex.Unlock()
}

// checkLongRunningTransactions 检查长时间运行的事务
func (tm *TransactionMonitor) checkLongRunningTransactions() {
	now := time.Now()
	longCount := 0

	// 检查本地事务
	tm.manager.mutex.RLock()
	for txID, tx := range tm.manager.activeTransactions {
		tx.mutex.RLock()
		if tx.status == TxStatusActive {
			duration := now.Sub(tx.startTime)
			if duration > tm.longTxThreshold {
				longCount++
				tm.alertLongRunningTransaction(txID, "本地事务", duration)
			}
		}
		tx.mutex.RUnlock()
	}
	tm.manager.mutex.RUnlock()

	// 检查TCC事务
	tccTxs := tm.tccCoordinator.GetAllTransactions()
	for _, tx := range tccTxs {
		if tx.State != TCCStateConfirmed && tx.State != TCCStateCancelled && tx.State != TCCStateFailed {
			duration := now.Sub(tx.StartTime)
			if duration > tm.longTxThreshold {
				longCount++
				tm.alertLongRunningTransaction(tx.ID, "TCC事务", duration)
			}
		}
	}

	// 检查分布式事务
	distTxs := tm.distCoordinator.GetAllTransactions()
	for _, tx := range distTxs {
		if tx.State != TxCommitted && tx.State != TxAborted {
			duration := now.Sub(tx.StartTime)
			if duration > tm.longTxThreshold {
				longCount++
				tm.alertLongRunningTransaction(tx.ID, "分布式事务", duration)
			}
		}
	}

	// 更新长时间运行事务计数
	tm.metrics.mutex.Lock()
	tm.metrics.LongRunningTxs = int64(longCount)
	tm.metrics.mutex.Unlock()
}

// alertSlowTransaction 发出慢事务警告
func (tm *TransactionMonitor) alertSlowTransaction(txID string, txType string, duration time.Duration) {
	tm.alertMutex.Lock()
	defer tm.alertMutex.Unlock()

	// 检查是否已经发出过警告
	alertKey := fmt.Sprintf("slow:%s", txID)
	if _, exists := tm.activeAlerts[alertKey]; exists {
		return
	}

	// 记录警告时间
	tm.activeAlerts[alertKey] = time.Now()

	// 输出警告日志
	log.Printf("警告: 慢事务检测 - %s %s 执行时间 %.2f 秒", txType, txID, duration.Seconds())
}

// alertLongRunningTransaction 发出长时间运行事务警告
func (tm *TransactionMonitor) alertLongRunningTransaction(txID string, txType string, duration time.Duration) {
	tm.alertMutex.Lock()
	defer tm.alertMutex.Unlock()

	// 检查是否已经发出过警告
	alertKey := fmt.Sprintf("long:%s", txID)
	lastAlert, exists := tm.activeAlerts[alertKey]

	// 如果已经发出过警告，检查是否需要再次发出（每分钟最多一次）
	if exists && time.Since(lastAlert) < time.Minute {
		return
	}

	// 记录警告时间
	tm.activeAlerts[alertKey] = time.Now()

	// 输出警告日志
	log.Printf("警告: 长时间运行事务 - %s %s 已运行 %.2f 秒", txType, txID, duration.Seconds())
}

// cleanupAlerts 清理过期警告
func (tm *TransactionMonitor) cleanupAlerts() {
	tm.alertMutex.Lock()
	defer tm.alertMutex.Unlock()

	now := time.Now()
	for key, alertTime := range tm.activeAlerts {
		// 清理超过1小时的警告
		if now.Sub(alertTime) > time.Hour {
			delete(tm.activeAlerts, key)
		}
	}
}

// GetMetrics 获取事务指标
func (tm *TransactionMonitor) GetMetrics() TransactionMetrics {
	tm.metrics.mutex.Lock()
	defer tm.metrics.mutex.Unlock()

	// 返回指标副本
	return *tm.metrics
}

// SetSlowTxThreshold 设置慢事务阈值
func (tm *TransactionMonitor) SetSlowTxThreshold(threshold time.Duration) {
	tm.slowTxThreshold = threshold
}

// SetLongTxThreshold 设置长时间运行事务阈值
func (tm *TransactionMonitor) SetLongTxThreshold(threshold time.Duration) {
	tm.longTxThreshold = threshold
}

// SetMonitorInterval 设置监控间隔
func (tm *TransactionMonitor) SetMonitorInterval(interval time.Duration) {
	tm.monitorInterval = interval
}
