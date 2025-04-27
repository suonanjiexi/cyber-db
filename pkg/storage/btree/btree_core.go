package storage

import (
	"fmt"
	"sort"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

// NewBTree 创建一个新的B+树
func NewBTree(degree int) *BTree {
	if degree < 2 {
		degree = 2 // 最小度为2
	}

	// 创建根节点
	root := &BTreeNode{
		IsLeaf:     true,
		Keys:       make([]string, 0),
		Values:     make([][]string, 0),
		keyMutexes: make([]sync.RWMutex, 0), // 初始化键级别锁数组
	}

	// 创建缓存
	nodeCache, _ := lru.New[string, *BTreeNode](10000) // 增大缓存容量

	// 创建统计信息
	stats := &TreeStats{
		MinSearchTime: 0,
		MaxSearchTime: 0,
		AvgSearchTime: 0,
	}

	return &BTree{
		Root:      root,
		Degree:    degree,
		NodeCount: 1,
		Height:    1,
		NodeCache: nodeCache,
		Stats:     stats,
	}
}

// findLeaf 查找键所在的叶子节点
func (bt *BTree) findLeaf(key string) *BTreeNode {
	// 记录开始时间，用于性能统计
	start := time.Now()

	// 检查节点缓存
	if bt.NodeCache != nil {
		cacheKey := "node:" + key
		if cachedNode, ok := bt.NodeCache.Get(cacheKey); ok {
			// 缓存命中
			if bt.Stats != nil {
				bt.Stats.mutex.Lock()
				bt.Stats.CacheHits++
				bt.Stats.mutex.Unlock()
			}
			return cachedNode
		}
	}

	node := bt.Root
	var path []*BTreeNode // 记录查找路径，用于预取和缓存

	for !node.IsLeaf {
		// 获取节点级读锁
		node.mutex.RLock()

		// 记录路径
		path = append(path, node)

		// 优化的二分查找
		keysLen := len(node.Keys)
		pos := 0

		if keysLen > 8 { // 只有当键数量足够多时才使用二分查找
			// 使用更高效的二分查找
			l, r := 0, keysLen-1
			for l <= r {
				mid := l + (r-l)/2 // 避免整数溢出
				if node.Keys[mid] <= key {
					pos = mid + 1
					l = mid + 1
				} else {
					r = mid - 1
				}
			}
		} else {
			// 对于小节点，线性查找更快
			for pos < keysLen && key >= node.Keys[pos] {
				pos++
			}
		}

		// 获取下一个节点
		nextNode := node.Children[pos]

		// 预取相邻节点（如果可能是下一个查询目标）
		if bt.NodeCache != nil && pos+1 < len(node.Children) {
			go func(prefetchNode *BTreeNode) {
				prefetchCacheKey := fmt.Sprintf("node:%p", prefetchNode)
				bt.NodeCache.Add(prefetchCacheKey, prefetchNode)
			}(node.Children[pos+1])
		}

		node.mutex.RUnlock()
		node = nextNode
	}

	// 缓存叶子节点和路径上的节点
	if bt.NodeCache != nil {
		// 缓存叶子节点
		cacheKey := "node:" + key
		bt.NodeCache.Add(cacheKey, node)

		// 缓存路径上的节点，使用节点指针作为部分键
		for i, pathNode := range path {
			pathCacheKey := fmt.Sprintf("path:%s:%d", key, i)
			bt.NodeCache.Add(pathCacheKey, pathNode)
		}
	}

	// 更新统计信息
	if bt.Stats != nil {
		elapsed := time.Since(start)
		bt.Stats.mutex.Lock()
		// 更新查询性能统计
		bt.Stats.AvgSearchTime = (bt.Stats.AvgSearchTime*float64(bt.Stats.Queries) + elapsed.Seconds()) / float64(bt.Stats.Queries+1)
		// 记录最大查找时间
		if elapsed.Seconds() > bt.Stats.MaxSearchTime {
			bt.Stats.MaxSearchTime = elapsed.Seconds()
		}
		// 记录最小查找时间
		if bt.Stats.MinSearchTime == 0 || elapsed.Seconds() < bt.Stats.MinSearchTime {
			bt.Stats.MinSearchTime = elapsed.Seconds()
		}
		bt.Stats.Queries++
		bt.Stats.mutex.Unlock()
	}

	return node
}

// Search 在B+树中查找键
func (bt *BTree) Search(key string) *SearchResult {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	// 查找叶子节点
	leaf := bt.findLeaf(key)

	// 获取节点读锁
	leaf.mutex.RLock()
	defer leaf.mutex.RUnlock()

	// 在叶子节点中查找键
	idx := sort.SearchStrings(leaf.Keys, key)
	if idx < len(leaf.Keys) && leaf.Keys[idx] == key {
		// 找到键
		return &SearchResult{
			Found:  true,
			Node:   leaf,
			Index:  idx,
			Values: leaf.Values[idx],
		}
	}

	// 未找到键
	return &SearchResult{
		Found: false,
		Node:  leaf,
		Index: idx,
	}
}

// RangeSearch 范围查询
func (bt *BTree) RangeSearch(startKey, endKey string) [][]string {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	// 查找起始叶子节点
	leaf := bt.findLeaf(startKey)

	// 结果集
	var results [][]string

	// 遍历叶子节点链表
	for leaf != nil {
		leaf.mutex.RLock()

		// 在当前叶子节点中查找符合范围的键
		for i, key := range leaf.Keys {
			if key >= startKey && (endKey == "" || key <= endKey) {
				results = append(results, leaf.Values[i])
			}

			// 如果已经超过结束键，提前结束
			if endKey != "" && key > endKey {
				leaf.mutex.RUnlock()
				return results
			}
		}

		// 获取下一个叶子节点
		nextLeaf := leaf.Next
		leaf.mutex.RUnlock()

		// 移动到下一个叶子节点
		leaf = nextLeaf
	}

	return results
}
