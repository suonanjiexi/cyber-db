package transaction

import (
	"sync"
)

// DeadlockDetector 死锁检测器
type DeadlockDetector struct {
	waitForGraph map[string]map[string]struct{} // 等待图：txID -> 等待的事务ID集合
	mutex        sync.Mutex                     // 互斥锁
}

// NewDeadlockDetector 创建一个新的死锁检测器
func NewDeadlockDetector() *DeadlockDetector {
	return &DeadlockDetector{
		waitForGraph: make(map[string]map[string]struct{}),
	}
}

// AddDependency 添加事务依赖关系
func (dd *DeadlockDetector) AddDependency(waitingTxID, holdingTxID string) {
	dd.mutex.Lock()
	defer dd.mutex.Unlock()

	// 确保等待事务在图中有一个条目
	if _, exists := dd.waitForGraph[waitingTxID]; !exists {
		dd.waitForGraph[waitingTxID] = make(map[string]struct{})
	}

	// 添加依赖关系
	dd.waitForGraph[waitingTxID][holdingTxID] = struct{}{}
}

// RemoveDependency 移除特定的事务依赖关系
func (dd *DeadlockDetector) RemoveDependency(waitingTxID, holdingTxID string) {
	dd.mutex.Lock()
	defer dd.mutex.Unlock()

	if waiting, exists := dd.waitForGraph[waitingTxID]; exists {
		delete(waiting, holdingTxID)

		// 如果没有更多依赖，删除整个条目
		if len(waiting) == 0 {
			delete(dd.waitForGraph, waitingTxID)
		}
	}
}

// RemoveTransaction 从等待图中完全移除一个事务
func (dd *DeadlockDetector) RemoveTransaction(txID string) {
	dd.mutex.Lock()
	defer dd.mutex.Unlock()

	// 移除txID作为等待者的条目
	delete(dd.waitForGraph, txID)

	// 移除所有对txID的依赖
	for waiter, deps := range dd.waitForGraph {
		delete(deps, txID)

		// 如果没有更多依赖，删除整个条目
		if len(deps) == 0 {
			delete(dd.waitForGraph, waiter)
		}
	}
}

// DetectDeadlock 检测是否有死锁
func (dd *DeadlockDetector) DetectDeadlock(startTxID string) bool {
	dd.mutex.Lock()
	defer dd.mutex.Unlock()

	// 使用深度优先搜索检测环
	visited := make(map[string]bool)
	path := make(map[string]bool)

	// DFS辅助函数
	var dfs func(string) bool
	dfs = func(current string) bool {
		// 如果节点已经在当前路径上，找到环
		if path[current] {
			return true
		}

		// 如果节点已经访问过且不在环上，跳过
		if visited[current] {
			return false
		}

		// 标记节点为已访问，并添加到当前路径
		visited[current] = true
		path[current] = true

		// 访问所有依赖节点
		for next := range dd.waitForGraph[current] {
			if dfs(next) {
				return true
			}
		}

		// 回溯，从当前路径中移除节点
		path[current] = false
		return false
	}

	// 从指定事务开始检测
	return dfs(startTxID)
}

// GetDeadlockCycle 获取死锁环（用于调试）
func (dd *DeadlockDetector) GetDeadlockCycle(startTxID string) []string {
	dd.mutex.Lock()
	defer dd.mutex.Unlock()

	// 使用深度优先搜索找到环
	visited := make(map[string]bool)
	path := make(map[string]int) // 存储节点在路径中的位置
	var cycle []string

	// DFS辅助函数
	var dfs func(string, int) bool
	dfs = func(current string, depth int) bool {
		// 检查是否形成环
		if pos, exists := path[current]; exists {
			// 找到环，提取环中的节点
			cycle = make([]string, depth-pos)
			for i := pos; i < depth; i++ {
				for node, p := range path {
					if p == i {
						cycle[i-pos] = node
						break
					}
				}
			}
			return true
		}

		// 如果节点已访问但不在当前路径上，不会形成环
		if visited[current] {
			return false
		}

		// 标记节点为已访问，并记录在路径中的位置
		visited[current] = true
		path[current] = depth

		// 访问所有依赖节点
		for next := range dd.waitForGraph[current] {
			if dfs(next, depth+1) {
				return true
			}
		}

		// 回溯，从当前路径中删除节点
		delete(path, current)
		return false
	}

	// 从指定事务开始寻找环
	dfs(startTxID, 0)
	return cycle
}

// GetStatistics 获取死锁检测器统计信息
func (dd *DeadlockDetector) GetStatistics() map[string]interface{} {
	dd.mutex.Lock()
	defer dd.mutex.Unlock()

	stats := make(map[string]interface{})
	stats["TotalTransactions"] = len(dd.waitForGraph)

	// 计算总依赖数
	totalDeps := 0
	for _, deps := range dd.waitForGraph {
		totalDeps += len(deps)
	}
	stats["TotalDependencies"] = totalDeps

	// 找出最高依赖度
	maxDeps := 0
	for _, deps := range dd.waitForGraph {
		if len(deps) > maxDeps {
			maxDeps = len(deps)
		}
	}
	stats["MaxDependencies"] = maxDeps

	return stats
}
