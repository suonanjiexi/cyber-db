package storage

import (
	"fmt"
	"time"
)

// GetStats 获取B+树统计信息
func (bt *BTree) GetStats() map[string]interface{} {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	if bt.Stats == nil {
		return nil
	}

	bt.Stats.mutex.Lock()
	defer bt.Stats.mutex.Unlock()

	// 计算缓存命中率
	cacheHitRate := 0.0
	if bt.Stats.CacheHits+bt.Stats.CacheMisses > 0 {
		cacheHitRate = float64(bt.Stats.CacheHits) / float64(bt.Stats.CacheHits+bt.Stats.CacheMisses)
	}

	// 计算平均分支因子
	avgBranchingFactor := 0.0
	if bt.NodeCount > 1 {
		avgBranchingFactor = float64(bt.NodeCount-1) / float64(bt.NodeCount-bt.countLeafNodes())
	}

	return map[string]interface{}{
		"NodeCount":          bt.NodeCount,
		"Height":             bt.Height,
		"Queries":            bt.Stats.Queries,
		"Inserts":            bt.Stats.Inserts,
		"Deletes":            bt.Stats.Deletes,
		"Splits":             bt.Stats.Splits,
		"Merges":             bt.Stats.Merges,
		"CacheHits":          bt.Stats.CacheHits,
		"CacheMisses":        bt.Stats.CacheMisses,
		"CacheHitRate":       cacheHitRate,
		"MinSearchTime":      bt.Stats.MinSearchTime,
		"MaxSearchTime":      bt.Stats.MaxSearchTime,
		"AvgSearchTime":      bt.Stats.AvgSearchTime,
		"LastRebalance":      bt.Stats.LastRebalance,
		"AvgBranchingFactor": avgBranchingFactor,
	}
}

// countLeafNodes 计算叶子节点数量
func (bt *BTree) countLeafNodes() int {
	return bt.countLeafNodesRecursive(bt.Root)
}

// countLeafNodesRecursive 递归计算叶子节点数量
func (bt *BTree) countLeafNodesRecursive(node *BTreeNode) int {
	if node == nil {
		return 0
	}

	if node.IsLeaf {
		return 1
	}

	count := 0
	for _, child := range node.Children {
		count += bt.countLeafNodesRecursive(child)
	}

	return count
}

// PrintTree 打印B+树结构（用于调试）
func (bt *BTree) PrintTree() {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	fmt.Println("B+Tree Structure:")
	bt.printNode(bt.Root, 0)
}

// printNode 递归打印节点
func (bt *BTree) printNode(node *BTreeNode, level int) {
	if node == nil {
		return
	}

	// 打印缩进
	indent := ""
	for i := 0; i < level; i++ {
		indent += "  "
	}

	// 打印节点信息
	nodeType := "Internal"
	if node.IsLeaf {
		nodeType = "Leaf"
	}

	fmt.Printf("%s[%s Node] Keys: %v\n", indent, nodeType, node.Keys)

	// 递归打印子节点
	if !node.IsLeaf {
		for _, child := range node.Children {
			bt.printNode(child, level+1)
		}
	}
}

// Rebalance 重新平衡B+树
func (bt *BTree) Rebalance() {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	// 记录开始时间
	startTime := time.Now()

	// 执行重平衡操作
	bt.rebalanceNode(bt.Root)

	// 更新统计信息
	if bt.Stats != nil {
		bt.Stats.mutex.Lock()
		bt.Stats.LastRebalance = time.Now()
		bt.Stats.mutex.Unlock()
	}

	fmt.Printf("Tree rebalanced in %v\n", time.Since(startTime))
}

// rebalanceNode 重新平衡节点
func (bt *BTree) rebalanceNode(node *BTreeNode) {
	if node == nil || node.IsLeaf {
		return
	}

	// 递归重平衡子节点
	for _, child := range node.Children {
		bt.rebalanceNode(child)
	}

	// 检查子节点的键分布
	minKeys := len(node.Children[0].Keys)
	maxKeys := minKeys

	for _, child := range node.Children {
		keyCount := len(child.Keys)
		if keyCount < minKeys {
			minKeys = keyCount
		}
		if keyCount > maxKeys {
			maxKeys = keyCount
		}
	}

	// 如果键分布不均匀，进行重新分配
	if maxKeys > 2*minKeys {
		bt.redistributeKeys(node)
	}
}

// redistributeKeys 重新分配节点中的键
func (bt *BTree) redistributeKeys(node *BTreeNode) {
	if node == nil || node.IsLeaf {
		return
	}

	// 收集所有子节点的键和值
	var allKeys []string
	var allValues [][]string
	var allChildren []*BTreeNode

	for i, child := range node.Children {
		if child.IsLeaf {
			allKeys = append(allKeys, child.Keys...)
			allValues = append(allValues, child.Values...)
		} else {
			allKeys = append(allKeys, child.Keys...)
			if i < len(node.Keys) {
				allKeys = append(allKeys, node.Keys[i])
			}
			allChildren = append(allChildren, child.Children...)
		}
	}

	// 计算每个节点应该有的键数量
	keysPerNode := len(allKeys) / len(node.Children)
	extraKeys := len(allKeys) % len(node.Children)

	// 重新分配键和值
	keyIndex := 0
	childIndex := 0
	for i := range node.Children {
		child := node.Children[i]
		keysForThisNode := keysPerNode
		if i < extraKeys {
			keysForThisNode++
		}

		if child.IsLeaf {
			child.Keys = allKeys[keyIndex : keyIndex+keysForThisNode]
			child.Values = allValues[keyIndex : keyIndex+keysForThisNode]
			keyIndex += keysForThisNode
		} else {
			nodeKeys := allKeys[keyIndex : keyIndex+keysForThisNode-1]
			child.Keys = nodeKeys
			child.Children = allChildren[childIndex : childIndex+keysForThisNode]
			childIndex += keysForThisNode

			// 更新父节点的分隔键
			if i < len(node.Keys) {
				node.Keys[i] = allKeys[keyIndex+keysForThisNode-1]
			}

			keyIndex += keysForThisNode
		}
	}
}
