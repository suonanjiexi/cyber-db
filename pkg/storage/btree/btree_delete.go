package storage

import (
	"sync"
)

// Delete 从B+树中删除键
func (bt *BTree) Delete(key string) bool {
	bt.mutex.Lock()

	// 查找叶子节点
	leaf := bt.findLeaf(key)

	// 释放树锁，只保留叶子节点锁
	bt.mutex.Unlock()

	// 获取节点写锁
	leaf.mutex.Lock()
	defer leaf.mutex.Unlock()

	// 在叶子节点中查找键
	deletePos := -1
	for i, k := range leaf.Keys {
		if k == key {
			deletePos = i
			break
		}
	}

	// 如果键不存在，返回false
	if deletePos == -1 {
		return false
	}

	// 删除键值对
	leaf.Keys = append(leaf.Keys[:deletePos], leaf.Keys[deletePos+1:]...)
	leaf.Values = append(leaf.Values[:deletePos], leaf.Values[deletePos+1:]...)
	leaf.keyMutexes = append(leaf.keyMutexes[:deletePos], leaf.keyMutexes[deletePos+1:]...)

	// 更新统计信息
	if bt.Stats != nil {
		bt.Stats.mutex.Lock()
		bt.Stats.Deletes++
		bt.Stats.mutex.Unlock()
	}

	// 检查是否需要合并或重新分配
	if len(leaf.Keys) < bt.Degree && leaf != bt.Root {
		// 需要获取树锁进行合并操作
		bt.mutex.Lock()
		bt.handleUnderflow(leaf)
		bt.mutex.Unlock()
	}

	return true
}

// handleUnderflow 处理节点下溢
func (bt *BTree) handleUnderflow(node *BTreeNode) {
	// 如果是根节点且没有子节点，不需要处理
	if node == bt.Root {
		if !node.IsLeaf && len(node.Children) == 1 {
			// 根节点只有一个子节点，将子节点提升为新的根节点
			bt.Root = node.Children[0]
			bt.Height--
		}
		return
	}

	// 查找父节点
	parent := bt.findParent(bt.Root, node)

	// 获取父节点写锁
	parent.mutex.Lock()
	defer parent.mutex.Unlock()

	// 查找节点在父节点中的位置
	nodePos := -1
	for i, child := range parent.Children {
		if child == node {
			nodePos = i
			break
		}
	}

	// 尝试从左兄弟借键
	if nodePos > 0 {
		leftSibling := parent.Children[nodePos-1]
		leftSibling.mutex.Lock()

		// 如果左兄弟有足够的键，可以借用
		if len(leftSibling.Keys) > bt.Degree {
			// 借用左兄弟的最后一个键
			borrowKey := leftSibling.Keys[len(leftSibling.Keys)-1]
			borrowValue := leftSibling.Values[len(leftSibling.Values)-1]

			// 从左兄弟中删除
			leftSibling.Keys = leftSibling.Keys[:len(leftSibling.Keys)-1]
			leftSibling.Values = leftSibling.Values[:len(leftSibling.Values)-1]
			leftSibling.keyMutexes = leftSibling.keyMutexes[:len(leftSibling.keyMutexes)-1]

			// 插入到当前节点
			node.Keys = append([]string{borrowKey}, node.Keys...)
			node.Values = append([][]string{borrowValue}, node.Values...)
			node.keyMutexes = append([]sync.RWMutex{{}}, node.keyMutexes...)

			// 更新父节点中的分隔键
			parent.Keys[nodePos-1] = borrowKey

			leftSibling.mutex.Unlock()
			return
		}
		leftSibling.mutex.Unlock()
	}

	// 尝试从右兄弟借键
	if nodePos < len(parent.Children)-1 {
		rightSibling := parent.Children[nodePos+1]
		rightSibling.mutex.Lock()

		// 如果右兄弟有足够的键，可以借用
		if len(rightSibling.Keys) > bt.Degree {
			// 借用右兄弟的第一个键
			borrowKey := rightSibling.Keys[0]
			borrowValue := rightSibling.Values[0]

			// 从右兄弟中删除
			rightSibling.Keys = rightSibling.Keys[1:]
			rightSibling.Values = rightSibling.Values[1:]
			rightSibling.keyMutexes = rightSibling.keyMutexes[1:]

			// 插入到当前节点
			node.Keys = append(node.Keys, borrowKey)
			node.Values = append(node.Values, borrowValue)
			node.keyMutexes = append(node.keyMutexes, sync.RWMutex{})

			// 更新父节点中的分隔键
			parent.Keys[nodePos] = rightSibling.Keys[0]

			rightSibling.mutex.Unlock()
			return
		}
		rightSibling.mutex.Unlock()
	}

	// 如果无法借用，需要合并节点
	if nodePos > 0 {
		// 与左兄弟合并
		leftSibling := parent.Children[nodePos-1]
		leftSibling.mutex.Lock()
		bt.mergeNodes(leftSibling, node, parent, nodePos-1)
		leftSibling.mutex.Unlock()
	} else {
		// 与右兄弟合并
		rightSibling := parent.Children[nodePos+1]
		rightSibling.mutex.Lock()
		bt.mergeNodes(node, rightSibling, parent, nodePos)
		rightSibling.mutex.Unlock()
	}
}

// mergeNodes 合并两个节点
func (bt *BTree) mergeNodes(leftNode, rightNode *BTreeNode, parent *BTreeNode, keyIndex int) {
	// 合并键值对
	leftNode.Keys = append(leftNode.Keys, rightNode.Keys...)
	leftNode.Values = append(leftNode.Values, rightNode.Values...)
	leftNode.keyMutexes = append(leftNode.keyMutexes, rightNode.keyMutexes...)

	if !leftNode.IsLeaf {
		// 合并子节点
		leftNode.Children = append(leftNode.Children, rightNode.Children...)
	} else {
		// 更新叶子节点链表
		leftNode.Next = rightNode.Next
	}

	// 从父节点中删除分隔键和右节点
	parent.Keys = append(parent.Keys[:keyIndex], parent.Keys[keyIndex+1:]...)
	parent.Children = append(parent.Children[:keyIndex+1], parent.Children[keyIndex+2:]...)

	// 更新统计信息
	if bt.Stats != nil {
		bt.Stats.mutex.Lock()
		bt.Stats.Merges++
		bt.Stats.mutex.Unlock()
	}

	// 减少节点计数
	bt.NodeCount--

	// 检查父节点是否需要合并或重新分配
	if len(parent.Keys) < bt.Degree-1 && parent != bt.Root {
		bt.handleUnderflow(parent)
	}
}

// findParent 查找节点的父节点
func (bt *BTree) findParent(root, child *BTreeNode) *BTreeNode {
	// 使用子节点的parent指针
	if child.Parent != nil {
		return child.Parent
	}

	if root.IsLeaf || root == child {
		return nil
	}

	// 检查直接子节点
	for _, node := range root.Children {
		if node == child {
			// 更新子节点的parent指针
			child.Parent = root
			return root
		}
	}

	// 递归查找
	for _, node := range root.Children {
		if !node.IsLeaf {
			if parent := bt.findParent(node, child); parent != nil {
				return parent
			}
		}
	}

	return nil
}
