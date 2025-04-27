package transaction

import (
	"errors"
	"time"
)

var (
	ErrLockTimeout     = errors.New("lock acquisition timed out")
	ErrLockDeadlock    = errors.New("deadlock detected during lock acquisition")
	ErrInvalidLockType = errors.New("invalid lock type")
)

// NewLockManager 创建一个新的锁管理器
func NewLockManager(timeout time.Duration) *LockManager {
	return &LockManager{
		locks:        make(map[string]*LockEntry),
		waitForGraph: make(map[string]map[string]struct{}),
		timeout:      timeout,
	}
}

// AcquireLock 获取锁
func (lm *LockManager) AcquireLock(txID string, key string, lockType LockType) error {
	if lockType != ReadLock && lockType != WriteLock {
		return ErrInvalidLockType
	}

	// 获取锁条目
	lm.mutex.Lock()
	le, exists := lm.locks[key]
	if !exists {
		le = &LockEntry{
			Type:        lockType,
			Holder:      txID,
			WaitQueue:   make([]string, 0),
			Timestamp:   time.Now(),
			readLocks:   make(map[string]struct{}),
			writeLock:   "",
			waitingList: make([]*LockRequest, 0),
		}
		lm.locks[key] = le
	}
	lm.mutex.Unlock()

	// 尝试获取锁
	le.mutex.Lock()

	// 检查是否可以立即授予锁
	if lm.canGrantImmediately(txID, le, lockType) {
		// 立即授予锁
		lm.grantLock(txID, le, lockType)
		le.mutex.Unlock()
		return nil
	}

	// 需要等待
	request := &LockRequest{
		TxID:        txID,
		Key:         key,
		Type:        lockType,
		grantedChan: make(chan bool, 1),
		startTime:   time.Now(),
	}

	// 将请求添加到等待列表
	le.waitingList = append(le.waitingList, request)

	// 更新等待图
	lm.updateWaitForGraph(txID, le)

	// 检查死锁
	if lm.detectDeadlock(txID) {
		// 从等待列表中移除
		for i, req := range le.waitingList {
			if req.TxID == txID {
				le.waitingList = append(le.waitingList[:i], le.waitingList[i+1:]...)
				break
			}
		}

		// 清理等待图
		lm.removeFromWaitForGraph(txID)

		le.mutex.Unlock()
		return ErrLockDeadlock
	}

	// 释放锁条目的互斥锁，等待通知
	le.mutex.Unlock()

	// 等待锁授予或超时
	select {
	case <-request.grantedChan:
		return nil
	case <-time.After(lm.timeout):
		// 超时，从等待列表中移除
		le.mutex.Lock()
		for i, req := range le.waitingList {
			if req.TxID == txID {
				le.waitingList = append(le.waitingList[:i], le.waitingList[i+1:]...)
				break
			}
		}

		// 清理等待图
		lm.removeFromWaitForGraph(txID)

		le.mutex.Unlock()
		return ErrLockTimeout
	}
}

// ReleaseLock 释放锁
func (lm *LockManager) ReleaseLock(txID string, key string) {
	lm.mutex.Lock()
	le, exists := lm.locks[key]
	if !exists {
		lm.mutex.Unlock()
		return
	}
	lm.mutex.Unlock()

	le.mutex.Lock()
	defer le.mutex.Unlock()

	// 释放读锁
	delete(le.readLocks, txID)

	// 释放写锁
	if le.writeLock == txID {
		le.writeLock = ""
	}

	// 清理等待图
	lm.removeFromWaitForGraph(txID)

	// 处理等待列表
	lm.processWaitingList(le)
}

// ReleaseAllLocks 释放事务的所有锁
func (lm *LockManager) ReleaseAllLocks(txID string) {
	// 获取所有锁条目
	lm.mutex.Lock()
	lockKeys := make([]string, 0, len(lm.locks))
	for key := range lm.locks {
		lockKeys = append(lockKeys, key)
	}
	lm.mutex.Unlock()

	// 释放每个锁
	for _, key := range lockKeys {
		lm.ReleaseLock(txID, key)
	}

	// 清理等待图
	lm.mutex.Lock()
	delete(lm.waitForGraph, txID)
	for _, waiters := range lm.waitForGraph {
		delete(waiters, txID)
	}
	lm.mutex.Unlock()
}

// 检查是否可以立即授予锁
func (lm *LockManager) canGrantImmediately(txID string, le *LockEntry, lockType LockType) bool {
	if lockType == ReadLock {
		// 检查是否有写锁
		if le.writeLock != "" && le.writeLock != txID {
			return false
		}
		// 已经持有读锁
		if _, hasReadLock := le.readLocks[txID]; hasReadLock {
			return true
		}
		// 可以获取读锁
		return true
	} else { // WriteLock
		// 检查是否有其他事务持有任何锁
		if le.writeLock != "" && le.writeLock != txID {
			return false
		}
		if len(le.readLocks) > 0 {
			// 如果只有当前事务持有读锁，可以升级为写锁
			if len(le.readLocks) == 1 && le.readLocks[txID] != struct{}{} {
				return false
			}
		}
		// 已经持有写锁
		if le.writeLock == txID {
			return true
		}
		// 可以获取写锁
		return len(le.readLocks) == 0 || (len(le.readLocks) == 1 && le.readLocks[txID] == struct{}{})
	}
}

// 授予锁
func (lm *LockManager) grantLock(txID string, le *LockEntry, lockType LockType) {
	if lockType == ReadLock {
		le.readLocks[txID] = struct{}{}
	} else { // WriteLock
		// 如果有读锁，先移除
		delete(le.readLocks, txID)
		le.writeLock = txID
	}
}

// 处理等待列表
func (lm *LockManager) processWaitingList(le *LockEntry) {
	if len(le.waitingList) == 0 {
		return
	}

	// 优先处理写锁请求，以防止写饥饿
	hasProcessedWrite := false

	// 首先尝试授予写锁
	for i := 0; i < len(le.waitingList); i++ {
		req := le.waitingList[i]
		if req.Type == WriteLock && lm.canGrantImmediately(req.TxID, le, WriteLock) {
			// 授予锁
			lm.grantLock(req.TxID, le, WriteLock)

			// 通知请求
			req.grantedChan <- true

			// 从等待列表中移除
			le.waitingList = append(le.waitingList[:i], le.waitingList[i+1:]...)

			// 清理等待图
			lm.removeFromWaitForGraph(req.TxID)

			hasProcessedWrite = true
			break
		}
	}

	// 如果没有处理写锁，尝试授予读锁
	if !hasProcessedWrite {
		i := 0
		for i < len(le.waitingList) {
			req := le.waitingList[i]
			if req.Type == ReadLock && lm.canGrantImmediately(req.TxID, le, ReadLock) {
				// 授予锁
				lm.grantLock(req.TxID, le, ReadLock)

				// 通知请求
				req.grantedChan <- true

				// 从等待列表中移除
				le.waitingList = append(le.waitingList[:i], le.waitingList[i+1:]...)

				// 清理等待图
				lm.removeFromWaitForGraph(req.TxID)

				// 不增加 i，因为列表已经缩短
			} else {
				i++
			}
		}
	}
}

// 更新等待图
func (lm *LockManager) updateWaitForGraph(txID string, le *LockEntry) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	// 确保事务在等待图中有一个条目
	if _, exists := lm.waitForGraph[txID]; !exists {
		lm.waitForGraph[txID] = make(map[string]struct{})
	}

	// 添加此事务等待的所有其他事务
	if le.writeLock != "" && le.writeLock != txID {
		lm.waitForGraph[txID][le.writeLock] = struct{}{}
	}

	for holderID := range le.readLocks {
		if holderID != txID {
			lm.waitForGraph[txID][holderID] = struct{}{}
		}
	}
}

// 从等待图中移除
func (lm *LockManager) removeFromWaitForGraph(txID string) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	// 移除事务的条目
	delete(lm.waitForGraph, txID)

	// 从其他事务的等待列表中移除
	for _, waiters := range lm.waitForGraph {
		delete(waiters, txID)
	}
}

// 死锁检测
func (lm *LockManager) detectDeadlock(txID string) bool {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	// 使用深度优先搜索检测环
	visited := make(map[string]bool)
	path := make(map[string]bool)

	var dfs func(string) bool
	dfs = func(current string) bool {
		// 标记为已访问
		visited[current] = true
		path[current] = true

		// 检查所有依赖
		for waiting := range lm.waitForGraph[current] {
			if !visited[waiting] {
				if dfs(waiting) {
					return true
				}
			} else if path[waiting] {
				// 在当前路径上发现了一个已访问过的节点，形成环
				return true
			}
		}

		// 回溯
		path[current] = false
		return false
	}

	return dfs(txID)
}
