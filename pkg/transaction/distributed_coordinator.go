package transaction

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/suonanjiexi/cyber-db/pkg/cluster"
)

// DistributedTransaction 分布式事务
type DistributedTransaction struct {
	ID           string
	Protocol     TransactionProtocol
	State        TransactionState
	Participants []string
	StartTime    time.Time
	EndTime      time.Time
	Timeout      time.Duration
	Votes        map[string]bool
	Error        error
	mutex        sync.RWMutex
}

// DistributedCoordinator 分布式事务协调器
type DistributedCoordinator struct {
	manager       *TransactionManager
	clusterClient *cluster.Client
	transactions  map[string]*DistributedTransaction
	mutex         sync.RWMutex
	timeout       time.Duration
}

// NewDistributedCoordinator 创建分布式事务协调器
func NewDistributedCoordinator(manager *TransactionManager, client *cluster.Client) *DistributedCoordinator {
	return &DistributedCoordinator{
		manager:       manager,
		clusterClient: client,
		transactions:  make(map[string]*DistributedTransaction),
		timeout:       30 * time.Second, // 默认超时时间
	}
}

// BeginDistributed 开始分布式事务
func (dc *DistributedCoordinator) BeginDistributed(protocol TransactionProtocol) (*DistributedTransaction, error) {
	txID := generateTxID()

	tx := &DistributedTransaction{
		ID:           txID,
		Protocol:     protocol,
		State:        TxInitial,
		Participants: make([]string, 0),
		StartTime:    time.Now(),
		Timeout:      dc.timeout,
		Votes:        make(map[string]bool),
	}

	dc.mutex.Lock()
	dc.transactions[txID] = tx
	dc.mutex.Unlock()

	log.Printf("开始分布式事务 %s，使用协议: %v", txID, protocol)

	return tx, nil
}

// AddParticipant 添加事务参与者
func (dc *DistributedCoordinator) AddParticipant(txID string, nodeID string) error {
	dc.mutex.RLock()
	tx, exists := dc.transactions[txID]
	dc.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("事务 %s 不存在", txID)
	}

	tx.mutex.Lock()
	defer tx.mutex.Unlock()

	// 检查状态
	if tx.State != TxInitial {
		return fmt.Errorf("事务 %s 已经开始执行，无法添加参与者", txID)
	}

	// 检查参与者是否已存在
	for _, p := range tx.Participants {
		if p == nodeID {
			return nil // 已存在，直接返回
		}
	}

	// 添加参与者
	tx.Participants = append(tx.Participants, nodeID)
	log.Printf("事务 %s 添加参与者: %s", txID, nodeID)

	return nil
}

// Prepare 准备阶段
func (dc *DistributedCoordinator) Prepare(txID string) error {
	dc.mutex.RLock()
	tx, exists := dc.transactions[txID]
	dc.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("事务 %s 不存在", txID)
	}

	tx.mutex.Lock()

	// 检查状态
	if tx.State != TxInitial {
		tx.mutex.Unlock()
		return fmt.Errorf("事务 %s 状态错误: %s，无法执行准备阶段", txID, tx.State)
	}

	// 更新状态
	tx.State = TxPreparing
	participants := make([]string, len(tx.Participants))
	copy(participants, tx.Participants)

	tx.mutex.Unlock()

	log.Printf("事务 %s 开始准备阶段，参与者数量: %d", txID, len(participants))

	// 如果没有参与者，直接返回成功
	if len(participants) == 0 {
		tx.mutex.Lock()
		tx.State = TxPrepared
		tx.mutex.Unlock()
		return nil
	}

	// 创建上下文，带超时
	ctx, cancel := context.WithTimeout(context.Background(), tx.Timeout)
	defer cancel()

	// 向所有参与者发送准备请求
	var wg sync.WaitGroup
	errCh := make(chan error, len(participants))

	for _, nodeID := range participants {
		wg.Add(1)
		go func(nid string) {
			defer wg.Done()

			// 发送准备请求
			vote, err := dc.sendPrepareRequest(ctx, txID, nid)

			if err != nil {
				errCh <- fmt.Errorf("参与者 %s 准备失败: %v", nid, err)
				return
			}

			// 记录投票结果
			tx.mutex.Lock()
			tx.Votes[nid] = vote
			tx.mutex.Unlock()

			if !vote {
				errCh <- fmt.Errorf("参与者 %s 投票拒绝提交", nid)
			}
		}(nodeID)
	}

	// 等待所有准备请求完成
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
		// 准备阶段失败
		tx.State = TxAborting
		tx.Error = fmt.Errorf("准备阶段失败: %v", errs)

		// 异步执行中止
		go dc.abort(txID)

		return tx.Error
	}

	// 检查所有投票
	for _, vote := range tx.Votes {
		if !vote {
			// 有参与者投票拒绝
			tx.State = TxAborting
			tx.Error = fmt.Errorf("有参与者投票拒绝提交")

			// 异步执行中止
			go dc.abort(txID)

			return tx.Error
		}
	}

	// 所有参与者都投票同意
	tx.State = TxPrepared

	// 如果是三阶段提交，还需要执行额外的阶段
	if tx.Protocol == ThreePhaseCommit {
		go dc.preCommit(txID)
	}

	return nil
}

// Commit 提交事务
func (dc *DistributedCoordinator) Commit(txID string) error {
	dc.mutex.RLock()
	tx, exists := dc.transactions[txID]
	dc.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("事务 %s 不存在", txID)
	}

	tx.mutex.Lock()

	// 检查状态
	validStates := map[TransactionState]bool{
		TxPrepared:  true,
		TxPreCommit: true, // 三阶段提交
	}

	if !validStates[tx.State] {
		tx.mutex.Unlock()
		return fmt.Errorf("事务 %s 状态错误: %s，无法执行提交", txID, tx.State)
	}

	// 更新状态
	tx.State = TxCommitting
	participants := make([]string, len(tx.Participants))
	copy(participants, tx.Participants)

	tx.mutex.Unlock()

	return dc.commit(txID)
}

// commit 提交事务的内部实现
func (dc *DistributedCoordinator) commit(txID string) error {
	dc.mutex.RLock()
	tx, exists := dc.transactions[txID]
	dc.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("事务 %s 不存在", txID)
	}

	log.Printf("事务 %s 开始提交阶段", txID)

	tx.mutex.RLock()
	participants := make([]string, len(tx.Participants))
	copy(participants, tx.Participants)
	tx.mutex.RUnlock()

	// 如果没有参与者，直接标记为已提交
	if len(participants) == 0 {
		tx.mutex.Lock()
		tx.State = TxCommitted
		tx.EndTime = time.Now()
		tx.mutex.Unlock()
		return nil
	}

	// 创建上下文，带超时
	ctx, cancel := context.WithTimeout(context.Background(), tx.Timeout)
	defer cancel()

	// 向所有参与者发送提交请求
	var wg sync.WaitGroup
	errCh := make(chan error, len(participants))

	for _, nodeID := range participants {
		wg.Add(1)
		go func(nid string) {
			defer wg.Done()

			// 发送提交请求
			if err := dc.sendCommitRequest(ctx, txID, nid); err != nil {
				errCh <- fmt.Errorf("参与者 %s 提交失败: %v", nid, err)
			}
		}(nodeID)
	}

	// 等待所有提交请求完成
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
		// 提交阶段有错误，但仍然标记为已提交
		// 因为在2PC/3PC中，一旦协调者决定提交，就必须完成提交
		tx.Error = fmt.Errorf("部分参与者提交失败: %v", errs)
		log.Printf("警告: 事务 %s 部分提交失败: %v", txID, errs)
	}

	// 标记为已提交
	tx.State = TxCommitted
	tx.EndTime = time.Now()

	log.Printf("事务 %s 已成功提交", txID)

	return nil
}

// 发送准备请求
func (dc *DistributedCoordinator) sendPrepareRequest(ctx context.Context, txID string, nodeID string) (bool, error) {
	// 获取节点连接
	conn, err := dc.clusterClient.GetNodeConnection(nodeID)
	if err != nil {
		return false, fmt.Errorf("获取节点连接失败: %v", err)
	}

	// 发送准备请求
	req := &PrepareRequest{
		TransactionID: txID,
	}

	resp := &PrepareResponse{}
	err = conn.Call("Transaction.Prepare", req, resp)
	if err != nil {
		return false, fmt.Errorf("发送准备请求失败: %v", err)
	}

	return resp.Vote, nil
}

// 发送提交请求
func (dc *DistributedCoordinator) sendCommitRequest(ctx context.Context, txID string, nodeID string) error {
	// 获取节点连接
	conn, err := dc.clusterClient.GetNodeConnection(nodeID)
	if err != nil {
		return fmt.Errorf("获取节点连接失败: %v", err)
	}

	// 发送提交请求
	req := &CommitRequest{
		TransactionID: txID,
	}

	resp := &CommitResponse{}
	err = conn.Call("Transaction.Commit", req, resp)
	if err != nil {
		return fmt.Errorf("发送提交请求失败: %v", err)
	}

	return nil
}

// 发送中止请求
func (dc *DistributedCoordinator) sendAbortRequest(ctx context.Context, txID string, nodeID string) error {
	// 获取节点连接
	conn, err := dc.clusterClient.GetNodeConnection(nodeID)
	if err != nil {
		return fmt.Errorf("获取节点连接失败: %v", err)
	}

	// 发送中止请求
	req := &AbortRequest{
		TransactionID: txID,
	}

	resp := &AbortResponse{}
	err = conn.Call("Transaction.Abort", req, resp)
	if err != nil {
		return fmt.Errorf("发送中止请求失败: %v", err)
	}

	return nil
}

// 发送预提交请求 (3PC)
func (dc *DistributedCoordinator) sendPreCommitRequest(ctx context.Context, txID string, nodeID string) error {
	// 获取节点连接
	conn, err := dc.clusterClient.GetNodeConnection(nodeID)
	if err != nil {
		return fmt.Errorf("获取节点连接失败: %v", err)
	}

	// 发送预提交请求
	req := &PreCommitRequest{
		TransactionID: txID,
	}

	resp := &PreCommitResponse{}
	err = conn.Call("Transaction.PreCommit", req, resp)
	if err != nil {
		return fmt.Errorf("发送预提交请求失败: %v", err)
	}

	return nil
}

// GetTransaction 获取事务信息
func (dc *DistributedCoordinator) GetTransaction(txID string) (*DistributedTransaction, error) {
	dc.mutex.RLock()
	tx, exists := dc.transactions[txID]
	dc.mutex.RUnlock()

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
func (dc *DistributedCoordinator) CleanupTransaction(txID string) error {
	dc.mutex.RLock()
	tx, exists := dc.transactions[txID]
	dc.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("事务 %s 不存在", txID)
	}

	tx.mutex.RLock()
	completed := tx.State == TxCommitted || tx.State == TxAborted
	tx.mutex.RUnlock()

	if !completed {
		return fmt.Errorf("事务 %s 尚未完成，无法清理", txID)
	}

	dc.mutex.Lock()
	delete(dc.transactions, txID)
	dc.mutex.Unlock()

	log.Printf("已清理事务 %s", txID)

	return nil
}

// StartTimeoutMonitor 启动超时监控
func (dc *DistributedCoordinator) StartTimeoutMonitor() {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			dc.checkTransactionTimeouts()
		}
	}()

	log.Println("事务超时监控已启动")
}

// checkTransactionTimeouts 检查事务超时
func (dc *DistributedCoordinator) checkTransactionTimeouts() {
	now := time.Now()

	// 使用读锁获取需要检查的事务ID列表
	dc.mutex.RLock()
	txIDs := make([]string, 0, len(dc.transactions))
	txStates := make(map[string]TransactionState)

	for txID, tx := range dc.transactions {
		tx.mutex.RLock()

		// 检查是否是活跃事务
		active := tx.State != TxCommitted && tx.State != TxAborted

		// 检查是否超时
		timeout := active && now.Sub(tx.StartTime) > tx.Timeout

		if timeout {
			txIDs = append(txIDs, txID)
			txStates[txID] = tx.State
		}

		tx.mutex.RUnlock()
	}
	dc.mutex.RUnlock()

	// 处理超时事务
	for _, txID := range txIDs {
		log.Printf("事务 %s (状态: %s) 已超时，执行中止", txID, txStates[txID])

		// 异步中止事务
		go func(id string) {
			if err := dc.Abort(id); err != nil {
				log.Printf("中止超时事务 %s 失败: %v", id, err)
			}
		}(txID)
	}
}

// 请求和响应结构体定义
type PrepareRequest struct {
	TransactionID string
}

type PrepareResponse struct {
	Vote bool
}

type CommitRequest struct {
	TransactionID string
}

type CommitResponse struct {
	Success bool
}

type AbortRequest struct {
	TransactionID string
}

type AbortResponse struct {
	Success bool
}

type PreCommitRequest struct {
	TransactionID string
}

type PreCommitResponse struct {
	Success bool
}

// Abort 中止事务
func (dc *DistributedCoordinator) Abort(txID string) error {
	dc.mutex.RLock()
	tx, exists := dc.transactions[txID]
	dc.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("事务 %s 不存在", txID)
	}

	tx.mutex.Lock()

	// 检查状态
	if tx.State == TxCommitted {
		tx.mutex.Unlock()
		return fmt.Errorf("事务 %s 已提交，无法中止", txID)
	}

	if tx.State == TxAborted {
		tx.mutex.Unlock()
		return nil // 已经是中止状态
	}

	// 更新状态
	tx.State = TxAborting

	tx.mutex.Unlock()

	// 执行中止
	return dc.abort(txID)
}

// abort 内部中止实现
func (dc *DistributedCoordinator) abort(txID string) error {
	dc.mutex.RLock()
	tx, exists := dc.transactions[txID]
	dc.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("事务 %s 不存在", txID)
	}

	tx.mutex.Lock()
	participants := make([]string, len(tx.Participants))
	copy(participants, tx.Participants)
	tx.mutex.Unlock()

	log.Printf("事务 %s 开始中止", txID)

	// 如果没有参与者，直接标记为已中止
	if len(participants) == 0 {
		tx.mutex.Lock()
		tx.State = TxAborted
		tx.EndTime = time.Now()
		tx.mutex.Unlock()
		return nil
	}

	// 创建上下文，带超时
	ctx, cancel := context.WithTimeout(context.Background(), tx.Timeout)
	defer cancel()

	// 向所有参与者发送中止请求
	var wg sync.WaitGroup
	for _, nodeID := range participants {
		wg.Add(1)
		go func(nid string) {
			defer wg.Done()

			// 发送中止请求
			if err := dc.sendAbortRequest(ctx, txID, nid); err != nil {
				log.Printf("警告: 向参与者 %s 发送中止请求失败: %v", nid, err)
			}
		}(nodeID)
	}

	// 等待所有中止请求完成
	wg.Wait()

	// 标记为已中止
	tx.mutex.Lock()
	tx.State = TxAborted
	tx.EndTime = time.Now()
	tx.mutex.Unlock()

	log.Printf("事务 %s 已中止", txID)

	return nil
}

// preCommit 三阶段提交的预提交阶段
func (dc *DistributedCoordinator) preCommit(txID string) error {
	dc.mutex.RLock()
	tx, exists := dc.transactions[txID]
	dc.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("事务 %s 不存在", txID)
	}

	tx.mutex.Lock()

	// 检查状态
	if tx.State != TxPrepared {
		tx.mutex.Unlock()
		return fmt.Errorf("事务 %s 状态错误: %s，无法执行预提交", txID, tx.State)
	}

	// 更新状态
	tx.State = TxPreCommit
	participants := make([]string, len(tx.Participants))
	copy(participants, tx.Participants)

	tx.mutex.Unlock()

	log.Printf("事务 %s 开始预提交阶段", txID)

	// 如果没有参与者，直接返回
	if len(participants) == 0 {
		return nil
	}

	// 创建上下文，带超时
	ctx, cancel := context.WithTimeout(context.Background(), tx.Timeout)
	defer cancel()

	// 向所有参与者发送预提交请求
	var wg sync.WaitGroup
	errCh := make(chan error, len(participants))

	for _, nodeID := range participants {
		wg.Add(1)
		go func(nid string) {
			defer wg.Done()

			// 发送预提交请求
			if err := dc.sendPreCommitRequest(ctx, txID, nid); err != nil {
				errCh <- fmt.Errorf("参与者 %s 预提交失败: %v", nid, err)
			}
		}(nodeID)
	}

	// 等待所有预提交请求完成
	wg.Wait()
	close(errCh)

	// 检查是否有错误
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		// 预提交阶段失败
		tx.mutex.Lock()
		tx.State = TxAborting
		tx.Error = fmt.Errorf("预提交阶段失败: %v", errs)
		tx.mutex.Unlock()

		// 执行中止
		go dc.abort(txID)

		return fmt.Errorf("预提交阶段失败: %v", errs)
	}

	return nil
}

// RecoverTransaction 恢复分布式事务
func (dc *DistributedCoordinator) RecoverTransaction(txID string) error {
	dc.mutex.RLock()
	tx, exists := dc.transactions[txID]
	dc.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("事务 %s 不存在", txID)
	}

	tx.mutex.Lock()
	state := tx.State
	tx.mutex.Unlock()

	log.Printf("恢复分布式事务 %s，当前状态: %s", txID, state)

	switch state {
	case TxInitial, TxPreparing:
		// 初始状态或正在准备，执行中止
		return dc.Abort(txID)
	case TxPrepared:
		if tx.Protocol == ThreePhaseCommit {
			// 三阶段提交，执行预提交
			return dc.preCommit(txID)
		}
		// 两阶段提交，执行提交
		return dc.Commit(txID)
	case TxPreCommit:
		// 已预提交，执行提交
		return dc.Commit(txID)
	case TxCommitting:
		// 正在提交，继续提交
		return dc.commit(txID)
	case TxAborting:
		// 正在中止，继续中止
		return dc.abort(txID)
	case TxCommitted, TxAborted:
		// 已完成状态，无需恢复
		return nil
	default:
		return fmt.Errorf("未知的事务状态: %s", state)
	}
}
