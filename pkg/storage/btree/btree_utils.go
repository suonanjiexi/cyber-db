package storage

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Clear 清空B+树
func (bt *BTree) Clear() {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	// 创建新的根节点
	root := &BTreeNode{
		IsLeaf:     true,
		Keys:       make([]string, 0),
		Values:     make([][]string, 0),
		keyMutexes: make([]sync.RWMutex, 0),
	}

	// 重置B+树
	bt.Root = root
	bt.NodeCount = 1
	bt.Height = 1

	// 清空缓存
	if bt.NodeCache != nil {
		bt.NodeCache.Purge()
	}

	// 重置统计信息
	if bt.Stats != nil {
		bt.Stats.mutex.Lock()
		bt.Stats.Queries = 0
		bt.Stats.Inserts = 0
		bt.Stats.Deletes = 0
		bt.Stats.Splits = 0
		bt.Stats.Merges = 0
		bt.Stats.CacheHits = 0
		bt.Stats.CacheMisses = 0
		bt.Stats.MinSearchTime = 0
		bt.Stats.MaxSearchTime = 0
		bt.Stats.AvgSearchTime = 0
		bt.Stats.mutex.Unlock()
	}
}

// Size 返回B+树中的键数量
func (bt *BTree) Size() int {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	return bt.countKeys(bt.Root)
}

// countKeys 递归计算节点及其子节点中的键数量
func (bt *BTree) countKeys(node *BTreeNode) int {
	if node == nil {
		return 0
	}

	if node.IsLeaf {
		return len(node.Keys)
	}

	count := 0
	for _, child := range node.Children {
		count += bt.countKeys(child)
	}

	return count
}

// IsEmpty 检查B+树是否为空
func (bt *BTree) IsEmpty() bool {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	return bt.Root == nil || (bt.Root.IsLeaf && len(bt.Root.Keys) == 0)
}

// ToJSON 将B+树转换为JSON格式
func (bt *BTree) ToJSON() ([]byte, error) {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	// 创建简化的树结构用于JSON序列化
	type SimpleNode struct {
		IsLeaf   bool          `json:"isLeaf"`
		Keys     []string      `json:"keys"`
		Children []*SimpleNode `json:"children,omitempty"`
	}

	var buildSimpleTree func(*BTreeNode) *SimpleNode
	buildSimpleTree = func(node *BTreeNode) *SimpleNode {
		if node == nil {
			return nil
		}

		simpleNode := &SimpleNode{
			IsLeaf: node.IsLeaf,
			Keys:   make([]string, len(node.Keys)),
		}

		copy(simpleNode.Keys, node.Keys)

		if !node.IsLeaf {
			simpleNode.Children = make([]*SimpleNode, len(node.Children))
			for i, child := range node.Children {
				simpleNode.Children[i] = buildSimpleTree(child)
			}
		}

		return simpleNode
	}

	// 构建简化的树结构
	simpleTree := buildSimpleTree(bt.Root)

	// 添加树的元数据
	treeData := struct {
		Degree    int         `json:"degree"`
		NodeCount int         `json:"nodeCount"`
		Height    int         `json:"height"`
		Root      *SimpleNode `json:"root"`
	}{
		Degree:    bt.Degree,
		NodeCount: bt.NodeCount,
		Height:    bt.Height,
		Root:      simpleTree,
	}

	// 序列化为JSON
	return json.Marshal(treeData)
}

// ValidateTree 验证B+树的结构是否正确
func (bt *BTree) ValidateTree() error {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	// 验证根节点
	if bt.Root == nil {
		return fmt.Errorf("root node is nil")
	}

	// 验证树的结构
	height, err := bt.validateNode(bt.Root, 1, nil, nil)
	if err != nil {
		return err
	}

	// 验证树高
	if height != bt.Height {
		return fmt.Errorf("tree height mismatch: stored %d, actual %d", bt.Height, height)
	}

	return nil
}

// validateNode 递归验证节点及其子节点
func (bt *BTree) validateNode(node *BTreeNode, level int, minKey, maxKey *string) (int, error) {
	if node == nil {
		return level - 1, nil
	}

	// 验证键的顺序
	for i := 1; i < len(node.Keys); i++ {
		if node.Keys[i-1] >= node.Keys[i] {
			return 0, fmt.Errorf("keys not in order at level %d: %s >= %s", level, node.Keys[i-1], node.Keys[i])
		}
	}

	// 验证键的范围
	if minKey != nil && len(node.Keys) > 0 && node.Keys[0] < *minKey {
		return 0, fmt.Errorf("key %s less than min key %s at level %d", node.Keys[0], *minKey, level)
	}

	if maxKey != nil && len(node.Keys) > 0 && node.Keys[len(node.Keys)-1] > *maxKey {
		return 0, fmt.Errorf("key %s greater than max key %s at level %d", node.Keys[len(node.Keys)-1], *maxKey, level)
	}

	// 如果是叶子节点，返回当前级别
	if node.IsLeaf {
		return level, nil
	}

	// 验证子节点数量
	if len(node.Children) != len(node.Keys)+1 {
		return 0, fmt.Errorf("invalid number of children at level %d: expected %d, got %d", level, len(node.Keys)+1, len(node.Children))
	}

	// 递归验证子节点
	maxHeight := 0
	for i, child := range node.Children {
		var childMinKey, childMaxKey *string

		if i > 0 {
			childMinKey = &node.Keys[i-1]
		} else {
			childMinKey = minKey
		}

		if i < len(node.Keys) {
			childMaxKey = &node.Keys[i]
		} else {
			childMaxKey = maxKey
		}

		height, err := bt.validateNode(child, level+1, childMinKey, childMaxKey)
		if err != nil {
			return 0, err
		}

		if height > maxHeight {
			maxHeight = height
		}
	}

	return maxHeight, nil
}

// Clone 克隆B+树
func (bt *BTree) Clone() *BTree {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	// 创建新的B+树
	newTree := NewBTree(bt.Degree)

	// 克隆根节点及其子节点
	newTree.Root = bt.cloneNode(bt.Root)
	newTree.NodeCount = bt.NodeCount
	newTree.Height = bt.Height

	return newTree
}

// cloneNode 递归克隆节点及其子节点
func (bt *BTree) cloneNode(node *BTreeNode) *BTreeNode {
	if node == nil {
		return nil
	}

	// 创建新节点
	newNode := &BTreeNode{
		IsLeaf:     node.IsLeaf,
		Keys:       make([]string, len(node.Keys)),
		keyMutexes: make([]sync.RWMutex, len(node.Keys)),
	}

	// 复制键
	copy(newNode.Keys, node.Keys)

	if node.IsLeaf {
		// 复制值
		newNode.Values = make([][]string, len(node.Values))
		for i, value := range node.Values {
			newNode.Values[i] = make([]string, len(value))
			copy(newNode.Values[i], value)
		}

		// 设置下一个叶子节点
		if node.Next != nil {
			// 注意：这里只是临时设置，后面需要更新
			newNode.Next = &BTreeNode{}
		}
	} else {
		// 复制子节点
		newNode.Children = make([]*BTreeNode, len(node.Children))
		for i, child := range node.Children {
			newNode.Children[i] = bt.cloneNode(child)
		}
	}

	return newNode
}

// UpdateLeafLinks 更新叶子节点链接
func (bt *BTree) UpdateLeafLinks() {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	// 获取所有叶子节点
	var leaves []*BTreeNode
	bt.collectLeaves(bt.Root, &leaves)

	// 更新叶子节点链接
	for i := 0; i < len(leaves)-1; i++ {
		leaves[i].Next = leaves[i+1]
	}

	// 最后一个叶子节点的Next为nil
	if len(leaves) > 0 {
		leaves[len(leaves)-1].Next = nil
	}
}

// collectLeaves 收集所有叶子节点
func (bt *BTree) collectLeaves(node *BTreeNode, leaves *[]*BTreeNode) {
	if node == nil {
		return
	}

	if node.IsLeaf {
		*leaves = append(*leaves, node)
		return
	}

	for _, child := range node.Children {
		bt.collectLeaves(child, leaves)
	}
}

// OptimizeTree 优化B+树结构
func (bt *BTree) OptimizeTree() {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	// 重新平衡树
	bt.rebalanceTree()

	// 更新叶子节点链接
	bt.UpdateLeafLinks()

	// 压缩节点
	bt.compactNodes(bt.Root)

	// 更新统计信息
	if bt.Stats != nil {
		bt.Stats.mutex.Lock()
		bt.Stats.LastRebalance = time.Now()
		bt.Stats.mutex.Unlock()
	}
}

// rebalanceTree 重新平衡整棵树
func (bt *BTree) rebalanceTree() {
	// 收集所有键和值
	var keys []string
	var values [][]string

	// 遍历所有叶子节点
	node := bt.findLeftmostLeaf(bt.Root)
	for node != nil {
		keys = append(keys, node.Keys...)
		values = append(values, node.Values...)
		node = node.Next
	}

	// 清空树
	bt.Clear()

	// 重新插入所有键值对
	for i, key := range keys {
		for _, recordID := range values[i] {
			_ = bt.Insert(key, recordID, false)
		}
	}
}

// findLeftmostLeaf 查找最左侧的叶子节点
func (bt *BTree) findLeftmostLeaf(node *BTreeNode) *BTreeNode {
	if node == nil {
		return nil
	}

	if node.IsLeaf {
		return node
	}

	return bt.findLeftmostLeaf(node.Children[0])
}

// compactNodes 压缩节点，减少内存占用
func (bt *BTree) compactNodes(node *BTreeNode) {
	if node == nil {
		return
	}

	// 压缩当前节点的容量
	if cap(node.Keys) > len(node.Keys)*2 {
		newKeys := make([]string, len(node.Keys))
		copy(newKeys, node.Keys)
		node.Keys = newKeys
	}

	if node.IsLeaf {
		if cap(node.Values) > len(node.Values)*2 {
			newValues := make([][]string, len(node.Values))
			copy(newValues, node.Values)
			node.Values = newValues
		}
	} else {
		// 递归压缩子节点
		for _, child := range node.Children {
			bt.compactNodes(child)
		}

		if cap(node.Children) > len(node.Children)*2 {
			newChildren := make([]*BTreeNode, len(node.Children))
			copy(newChildren, node.Children)
			node.Children = newChildren
		}
	}
}
