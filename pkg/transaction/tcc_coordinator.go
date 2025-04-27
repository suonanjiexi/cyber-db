package transaction

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/suonanjiexi/cyber-db/pkg/cluster"
)

// TCCPhase TCC事务阶段
type TCCPhase string

const (
	TCCPhaseTry     TCCPhase = "try"
	TCCPhaseConfirm TCCPhase = "confirm"
	TCCPhaseCancel  TCCPhase = "cancel"
)

// TCCState TCC事务状态
type TCCState string

const (
	TCCStateInitial    TCCState = "initial"
	TCCStateTrying     TCCState = "trying"
	TCCStateTried      TCCState = "tried"
	TCCStateConfirming TCCState = "confirming"
	TCCStateConfirmed  TCCState = "confirmed"
	TCCStateCancelling TCCState = "cancelling"
	TCCStateCancelled  TCCState = "cancelled"
	TCCStateFailed     TCCState = "failed"
)

// TCCCoordinator TCC事务协调器
type TCCCoordinator struct {
	manager       *TransactionManager
	clusterClient *cluster.Client
	transactions  map[string]*TCCTransaction
	mutex         sync.RWMutex
	timeout       time.Duration
}

// TCCTransaction TCC事务
type TCCTransaction struct {
	ID           string
	State        TCCState
	Participants []TCCParticipant
	StartTime    time.Time
	EndTime      time.Time
	Timeout      time.Duration
	Error        error
	mutex        sync.RWMutex
}

// TCCParticipant TCC参与者
type TCCParticipant struct {
	NodeID        string
	ResourceID    string
	TryResult     bool
	TryError      error
	ConfirmResult bool
	ConfirmError  error
	CancelResult  bool
	CancelError   error
}

// NewTCCCoordinator 创建TCC事务协调器
func NewTCCCoordinator(manager *TransactionManager, clusterClient *cluster.Client) *TCCCoordinator {
	return &TCCCoordinator{
		manager:       manager,
		clusterClient: clusterClient,
		transactions:  make(map[string]*TCCTransaction),
		timeout:       30 * time.Second, // 默认超时时间
	}
}

// BeginTCC 开始TCC事务
func (tc *TCCCoordinator) BeginTCC() (*TCCTransaction, error) {
	txID := generateTxID()

	tx := &TCCTransaction{
		ID:           txID,
		State:        TCCStateInitial,
		Participants: make([]TCCParticipant, 0),
		StartTime:    time.Now(),
		Timeout:      tc.timeout,
	}

	tc.mutex.Lock()
	tc.transactions[txID] = tx
	tc.mutex.Unlock()

	log.Printf("开始TCC事务 %s", txID)

	return tx, nil
}

// AddParticipant 添加TCC参与者
func (tc *TCCCoordinator) AddParticipant(txID string, nodeID string, resourceID string) error {
	tc.mutex.RLock()
	tx, exists := tc.transactions[txID]
	tc.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("事务 %s 不存在", txID)
	}

	tx.mutex.Lock()
	defer tx.mutex.Unlock()

	// 检查状态
	if tx.State != TCCStateInitial {
		return fmt.Errorf("事务 %s 已经开始执行，无法添加参与者", txID)
	}

	// 添加参与者
	participant := TCCParticipant{
		NodeID:     nodeID,
		ResourceID: resourceID,
	}

	tx.Participants = append(tx.Participants, participant)
	log.Printf("事务 %s 添加参与者: %s, 资源: %s", txID, nodeID, resourceID)

	return nil
}

// Try 执行Try阶段
func (tc *TCCCoordinator) Try(txID string) error {
	tc.mutex.RLock()
	tx, exists := tc.transactions[txID]
	tc.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("事务 %s 不存在", txID)
	}

	tx.mutex.Lock()

	// 检查状态
	if tx.State != TCCStateInitial {
		tx.mutex.Unlock()
		return fmt.Errorf("事务 %s 状态错误: %s，无法执行Try阶段", txID, tx.State)
	}

	// 更新状态
	tx.State = TCCStateTrying
	participants := make([]TCCParticipant, len(tx.Participants))
	copy(participants, tx.Participants)

	tx.mutex.Unlock()

	log.Printf("事务 %s 开始Try阶段", txID)

	// 如果没有参与者，直接返回成功
	if len(participants) == 0 {
		tx.mutex.Lock()
		tx.State = TCCStateTried
		tx.mutex.Unlock()
		return nil
	}

	// 创建上下文，带超时
	ctx, cancel := context.WithTimeout(context.Background(), tx.Timeout)
	defer cancel()

	// 向所有参与者发送Try请求
	var wg sync.WaitGroup
	errCh := make(chan error, len(participants))

	for i, p := range participants {
		wg.Add(1)
		go func(idx int, participant TCCParticipant) {
			defer wg.Done()

			// 发送Try请求
			success, err := tc.sendTryRequest(ctx, txID, participant.NodeID, participant.ResourceID)

			// 记录结果
			tx.mutex.Lock()
			if idx < len(tx.Participants) {
				tx.Participants[idx].TryResult = success
				tx.Participants[idx].TryError = err
			}
			tx.mutex.Unlock()

			if err != nil {
				errCh <- fmt.Errorf("参与者 %s 资源 %s Try失败: %v",
					participant.NodeID, participant.ResourceID, err)
			} else if !success {
				errCh <- fmt.Errorf("参与者 %s 资源 %s Try返回失败",
					participant.NodeID, participant.ResourceID)
			}
		}(i, p)
	}

	// 等待所有Try请求完成
	wg.Wait()
	close(errCh)

	// 检查是否有错误
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	tx.mutex.Lock()
	defer tx.mutex.Unlock()

	if len(errs) > 0 {
		// Try阶段失败
		tx.State = TCCStateFailed
		tx.Error = fmt.Errorf("Try阶段失败: %v", errs)

		// 异步执行Cancel
		go tc.cancel(txID)

		return tx.Error
	}

	// Try阶段成功
	tx.State = TCCStateTried

	return nil
}

// Confirm 执行Confirm阶段
func (tc *TCCCoordinator) Confirm(txID string) error {
	tc.mutex.RLock()
	tx, exists := tc.transactions[txID]
	tc.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("事务 %s 不存在", txID)
	}

	tx.mutex.Lock()

	// 检查状态
	if tx.State != TCCStateTried {
		tx.mutex.Unlock()
		return fmt.Errorf("事务 %s 状态错误: %s，无法执行Confirm阶段", txID, tx.State)
	}

	// 更新状态
	tx.State = TCCStateConfirming
	participants := make([]TCCParticipant, len(tx.Participants))
	copy(participants, tx.Participants)

	tx.mutex.Unlock()

	log.Printf("事务 %s 开始Confirm阶段", txID)

	// 如果没有参与者，直接标记为已确认
	if len(participants) == 0 {
		tx.mutex.Lock()
		tx.State = TCCStateConfirmed
		tx.EndTime = time.Now()
		tx.mutex.Unlock()
		return nil
	}

	// 创建上下文，带超时
	ctx, cancel := context.WithTimeout(context.Background(), tx.Timeout)
	defer cancel()

	// 向所有参与者发送Confirm请求
	var wg sync.WaitGroup
	errCh := make(chan error, len(participants))

	for i, p := range participants {
		// 只对Try成功的参与者执行Confirm
		if !p.TryResult {
			continue
		}

		wg.Add(1)
		go func(idx int, participant TCCParticipant) {
			defer wg.Done()

			// 发送Confirm请求
			success, err := tc.sendConfirmRequest(ctx, txID, participant.NodeID, participant.ResourceID)

			// 记录结果
			tx.mutex.Lock()
			if idx < len(tx.Participants) {
				tx.Participants[idx].ConfirmResult = success
				tx.Participants[idx].ConfirmError = err
			}
			tx.mutex.Unlock()

			if err != nil {
				errCh <- fmt.Errorf("参与者 %s 资源 %s Confirm失败: %v",
					participant.NodeID, participant.ResourceID, err)
			} else if !success {
				errCh <- fmt.Errorf("参与者 %s 资源 %s Confirm返回失败",
					participant.NodeID, participant.ResourceID)
			}
		}(i, p)
	}

	// 等待所有Confirm请求完成
	wg.Wait()
	close(errCh)

	// 检查是否有错误
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	tx.mutex.Lock()
	defer tx.mutex.Unlock()

	if len(errs) > 0 {
		// Confirm阶段有错误，但仍然标记为已确认
		// 因为在TCC中，一旦决定确认，就必须完成确认（可能需要重试机制）
		tx.Error = fmt.Errorf("部分参与者Confirm失败: %v", errs)
		log.Printf("警告: 事务 %s 部分Confirm失败: %v", txID, errs)
	}

	// 标记为已确认
	tx.State = TCCStateConfirmed
	tx.EndTime = time.Now()

	log.Printf("事务 %s 已成功确认", txID)

	return nil
}

// Cancel 执行Cancel阶段
func (tc *TCCCoordinator) Cancel(txID string) error {
	tc.mutex.RLock()
	tx, exists := tc.transactions[txID]
	tc.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("事务 %s 不存在", txID)
	}

	tx.mutex.Lock()

	// 检查状态
	if tx.State == TCCStateConfirmed {
		tx.mutex.Unlock()
		return fmt.Errorf("事务 %s 已确认，无法取消", txID)
	}

	if tx.State == TCCStateCancelled {
		tx.mutex.Unlock()
		return nil // 已经是取消状态
	}

	// 更新状态
	tx.State = TCCStateCancelling

	tx.mutex.Unlock()

	// 执行取消
	return tc.cancel(txID)
}

// cancel 内部取消实现
func (tc *TCCCoordinator) cancel(txID string) error {
	tc.mutex.RLock()
	tx, exists := tc.transactions[txID]
	tc.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("事务 %s 不存在", txID)
	}

	tx.mutex.Lock()
	participants := make([]TCCParticipant, len(tx.Participants))
	copy(participants, tx.Participants)
	tx.mutex.Unlock()

	log.Printf("事务 %s 开始取消", txID)

	// 如果没有参与者，直接标记为已取消
	if len(participants) == 0 {
		tx.mutex.Lock()
		tx.State = TCCStateCancelled
		tx.EndTime = time.Now()
		tx.mutex.Unlock()
		return nil
	}

	// 创建上下文，带超时
	ctx, cancel := context.WithTimeout(context.Background(), tx.Timeout)
	defer cancel()

	// 向所有参与者发送Cancel请求
	var wg sync.WaitGroup
	cancelErrors := make([]error, 0)
	var errMutex sync.Mutex

	for i, p := range participants {
		// 只对Try成功的参与者执行Cancel
		if !p.TryResult {
			continue
		}

		wg.Add(1)
		go func(idx int, participant TCCParticipant) {
			defer wg.Done()

			// 发送Cancel请求，带重试机制
			success, err := tc.sendCancelRequestWithRetry(ctx, txID, participant.NodeID, participant.ResourceID)

			// 记录结果
			tx.mutex.Lock()
			if idx < len(tx.Participants) {
				tx.Participants[idx].CancelResult = success
				tx.Participants[idx].CancelError = err
			}
			tx.mutex.Unlock()

			if err != nil {
				errMsg := fmt.Sprintf("参与者 %s 资源 %s Cancel失败: %v",
					participant.NodeID, participant.ResourceID, err)
				log.Printf("警告: %s", errMsg)

				errMutex.Lock()
				cancelErrors = append(cancelErrors, errors.New(errMsg))
				errMutex.Unlock()
			} else if !success {
				errMsg := fmt.Sprintf("参与者 %s 资源 %s Cancel返回失败",
					participant.NodeID, participant.ResourceID)
				log.Printf("警告: %s", errMsg)

				errMutex.Lock()
				cancelErrors = append(cancelErrors, errors.New(errMsg))
				errMutex.Unlock()
			}
		}(i, p)
	}

	// 等待所有Cancel请求完成
	wg.Wait()

	// 标记为已取消
	tx.mutex.Lock()
	tx.State = TCCStateCancelled
	tx.EndTime = time.Now()

	// 记录错误信息
	if len(cancelErrors) > 0 {
		tx.Error = fmt.Errorf("部分参与者取消失败: %v", cancelErrors)
	}

	tx.mutex.Unlock()

	log.Printf("事务 %s 已取消", txID)

	return nil
}

// sendCancelRequestWithRetry 发送Cancel请求，带重试机制
func (tc *TCCCoordinator) sendCancelRequestWithRetry(ctx context.Context, txID string, nodeID string, resourceID string) (bool, error) {
	maxRetries := 3
	retryDelay := 500 * time.Millisecond

	for retry := 0; retry < maxRetries; retry++ {
		success, err := tc.sendCancelRequest(ctx, txID, nodeID, resourceID)
		if err == nil {
			return success, nil
		}

		// 检查是否应该重试
		if retry < maxRetries-1 && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			log.Printf("发送Cancel请求失败，将在 %v 后重试: %v", retryDelay, err)
			select {
			case <-time.After(retryDelay):
				retryDelay *= 2 // 指数退避
				continue
			case <-ctx.Done():
				return false, ctx.Err()
			}
		}

		return false, err
	}

	return false, fmt.Errorf("发送Cancel请求失败，已达到最大重试次数")
}

// 发送Try请求
func (tc *TCCCoordinator) sendTryRequest(ctx context.Context, txID string, nodeID string, resourceID string) (bool, error) {
	// 获取节点连接
	conn, err := tc.clusterClient.GetNodeConnection(nodeID)
	if err != nil {
		return false, fmt.Errorf("获取节点连接失败: %v", err)
	}

	// 发送Try请求
	req := &TryRequest{
		TransactionID: txID,
		ResourceID:    resourceID,
	}

	resp, err := conn.Try(ctx, req)
	if err != nil {
		return false, fmt.Errorf("发送Try请求失败: %v", err)
	}

	// 类型断言获取Success字段
	if respStruct, ok := resp.(*struct{ Success bool }); ok {
		return respStruct.Success, nil
	}

	return false, nil
}

// 发送Confirm请求
func (tc *TCCCoordinator) sendConfirmRequest(ctx context.Context, txID string, nodeID string, resourceID string) (bool, error) {
	// 获取节点连接
	conn, err := tc.clusterClient.GetNodeConnection(nodeID)
	if err != nil {
		return false, fmt.Errorf("获取节点连接失败: %v", err)
	}

	// 发送Confirm请求
	req := &ConfirmRequest{
		TransactionID: txID,
		ResourceID:    resourceID,
	}

	resp, err := conn.Confirm(ctx, req)
	if err != nil {
		return false, fmt.Errorf("发送Confirm请求失败: %v", err)
	}

	// 类型断言获取Success字段
	if respStruct, ok := resp.(*struct{ Success bool }); ok {
		return respStruct.Success, nil
	}

	return false, nil
}

// 发送Cancel请求
func (tc *TCCCoordinator) sendCancelRequest(ctx context.Context, txID string, nodeID string, resourceID string) (bool, error) {
	// 获取节点连接
	conn, err := tc.clusterClient.GetNodeConnection(nodeID)
	if err != nil {
		return false, fmt.Errorf("获取节点连接失败: %v", err)
	}

	// 发送Cancel请求
	req := &CancelRequest{
		TransactionID: txID,
		ResourceID:    resourceID,
	}

	resp, err := conn.Cancel(ctx, req)
	if err != nil {
		return false, fmt.Errorf("发送Cancel请求失败: %v", err)
	}

	// 类型断言获取Success字段
	if respStruct, ok := resp.(*struct{ Success bool }); ok {
		return respStruct.Success, nil
	}

	return false, nil
}

// GetTransaction 获取事务信息
func (tc *TCCCoordinator) GetTransaction(txID string) (*TCCTransaction, error) {
	tc.mutex.RLock()
	tx, exists := tc.transactions[txID]
	tc.mutex.RUnlock()

	if !exists {
		return nil, fmt.Errorf("事务 %s 不存在", txID)
	}

	// 创建副本以避免并发修改
	tx.mutex.RLock()
	txCopy := *tx
	tx.mutex.RUnlock()

	return &txCopy, nil
}

// CleanupTransaction 清理已完成的事务
func (tc *TCCCoordinator) CleanupTransaction(txID string) error {
	tc.mutex.RLock()
	tx, exists := tc.transactions[txID]
	tc.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("事务 %s 不存在", txID)
	}

	tx.mutex.RLock()
	completed := tx.State == TCCStateConfirmed || tx.State == TCCStateCancelled
	tx.mutex.RUnlock()

	if !completed {
		return fmt.Errorf("事务 %s 尚未完成，无法清理", txID)
	}

	tc.mutex.Lock()
	delete(tc.transactions, txID)
	tc.mutex.Unlock()

	log.Printf("已清理事务 %s", txID)

	return nil
}

// StartTimeoutMonitor 启动超时监控
func (tc *TCCCoordinator) StartTimeoutMonitor() {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			tc.checkTransactionTimeouts()
		}
	}()

	log.Println("TCC事务超时监控已启动")
}

// checkTransactionTimeouts 检查事务超时
func (tc *TCCCoordinator) checkTransactionTimeouts() {
	now := time.Now()

	tc.mutex.RLock()
	txIDs := make([]string, 0, len(tc.transactions))
	for txID, tx := range tc.transactions {
		tx.mutex.RLock()

		// 检查是否是活跃事务
		active := tx.State != TCCStateConfirmed && tx.State != TCCStateCancelled

		// 检查是否超时
		timeout := active && now.Sub(tx.StartTime) > tx.Timeout

		tx.mutex.RUnlock()

		if timeout {
			txIDs = append(txIDs, txID)
		}
	}
	tc.mutex.RUnlock()

	// 处理超时事务
	for _, txID := range txIDs {
		log.Printf("事务 %s 已超时，执行取消", txID)

		// 异步取消事务
		go func(id string) {
			if err := tc.Cancel(id); err != nil {
				log.Printf("取消超时事务 %s 失败: %v", id, err)
			}
		}(txID)
	}
}

// 请求和响应结构体定义
type TryRequest struct {
	TransactionID string
	ResourceID    string
}

type TryResponse struct {
	Success bool
}

type ConfirmRequest struct {
	TransactionID string
	ResourceID    string
}

type ConfirmResponse struct {
	Success bool
}

type CancelRequest struct {
	TransactionID string
	ResourceID    string
}

type CancelResponse struct {
	Success bool
}

// SetTimeout 设置事务超时时间
func (tc *TCCCoordinator) SetTimeout(timeout time.Duration) {
	tc.timeout = timeout
}

// GetAllTransactions 获取所有事务
func (tc *TCCCoordinator) GetAllTransactions() []*TCCTransaction {
	tc.mutex.RLock()
	defer tc.mutex.RUnlock()

	transactions := make([]*TCCTransaction, 0, len(tc.transactions))
	for _, tx := range tc.transactions {
		tx.mutex.RLock()
		txCopy := *tx
		tx.mutex.RUnlock()

		transactions = append(transactions, &txCopy)
	}

	return transactions
}

// GetActiveTransactions 获取活跃事务
func (tc *TCCCoordinator) GetActiveTransactions() []*TCCTransaction {
	tc.mutex.RLock()
	defer tc.mutex.RUnlock()

	transactions := make([]*TCCTransaction, 0)
	for _, tx := range tc.transactions {
		tx.mutex.RLock()
		active := tx.State != TCCStateConfirmed && tx.State != TCCStateCancelled
		txCopy := *tx
		tx.mutex.RUnlock()

		if active {
			transactions = append(transactions, &txCopy)
		}
	}

	return transactions
}

// RecoverTransaction 恢复事务
func (tc *TCCCoordinator) RecoverTransaction(txID string) error {
	tc.mutex.RLock()
	tx, exists := tc.transactions[txID]
	tc.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("事务 %s 不存在", txID)
	}

	tx.mutex.Lock()
	state := tx.State
	tx.mutex.Unlock()

	log.Printf("恢复事务 %s，当前状态: %s", txID, state)

	switch state {
	case TCCStateInitial, TCCStateTrying:
		// 初始状态或正在Try，执行取消
		return tc.Cancel(txID)
	case TCCStateTried:
		// Try成功，执行确认
		return tc.Confirm(txID)
	case TCCStateConfirming:
		// 正在确认，继续确认
		return tc.Confirm(txID)
	case TCCStateCancelling:
		// 正在取消，继续取消
		return tc.Cancel(txID)
	case TCCStateConfirmed, TCCStateCancelled, TCCStateFailed:
		// 已完成状态，无需恢复
		return nil
	default:
		return fmt.Errorf("未知的事务状态: %s", state)
	}
}
