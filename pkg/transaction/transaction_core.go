package transaction

import (
	"fmt"
	"time"

	"github.com/suonanjiexi/cyber-db/pkg/storage"
)

// Commit 提交事务
func (tx *Transaction) Commit() error {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()

	// 检查事务状态
	if tx.status != TxStatusActive {
		return ErrTxNotActive
	}

	// 检查事务是否超时
	if time.Since(tx.startTime) > tx.timeout {
		tx.status = TxStatusAborted
		return ErrTxTimeout
	}

	// 将写集合应用到数据库
	for key, value := range tx.writeSet {
		var err error
		if value == nil {
			// 删除操作
			err = tx.db.Delete(key)
		} else {
			// 写入操作
			err = tx.db.Put(key, value)
		}

		if err != nil {
			tx.status = TxStatusAborted
			return fmt.Errorf("提交事务失败: %w", err)
		}
	}

	tx.status = TxStatusCommitted
	return nil
}

// Rollback 回滚事务
func (tx *Transaction) Rollback() error {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()

	// 检查事务状态
	if tx.status != TxStatusActive {
		return ErrTxNotActive
	}

	tx.status = TxStatusAborted
	return nil
}

// Get 从事务中读取数据
func (tx *Transaction) Get(key string) ([]byte, error) {
	tx.mutex.RLock()
	defer tx.mutex.RUnlock()

	// 检查事务状态
	if tx.status != TxStatusActive {
		return nil, ErrTxNotActive
	}

	// 检查事务是否超时
	if time.Since(tx.startTime) > tx.timeout {
		tx.status = TxStatusAborted
		return nil, ErrTxTimeout
	}

	// 首先检查写集合
	if value, ok := tx.writeSet[key]; ok {
		if value == nil { // 键已被删除
			return nil, storage.ErrKeyNotFound
		}
		return value, nil
	}

	// 然后从数据库读取
	value, err := tx.db.Get(key)
	if err != nil {
		return nil, err
	}

	// 添加到读集合
	tx.readSet[key] = struct{}{}
	return value, nil
}

// Put 在事务中写入数据
func (tx *Transaction) Put(key string, value []byte) error {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()

	// 检查事务状态
	if tx.status != TxStatusActive {
		return ErrTxNotActive
	}

	// 检查事务是否超时
	if time.Since(tx.startTime) > tx.timeout {
		tx.status = TxStatusAborted
		return ErrTxTimeout
	}

	// 将数据添加到写集合
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	tx.writeSet[key] = valueCopy
	return nil
}

// Delete 在事务中删除数据
func (tx *Transaction) Delete(key string) error {
	tx.mutex.Lock()
	defer tx.mutex.Unlock()

	// 检查事务状态
	if tx.status != TxStatusActive {
		return ErrTxNotActive
	}

	// 检查事务是否超时
	if time.Since(tx.startTime) > tx.timeout {
		tx.status = TxStatusAborted
		return ErrTxTimeout
	}

	// 将空值添加到写集合，表示删除
	tx.writeSet[key] = nil
	return nil
}

// Status 返回事务的当前状态
func (tx *Transaction) Status() int {
	tx.mutex.RLock()

	// 只读检查事务状态
	status := tx.status
	if status == TxStatusActive && time.Since(tx.startTime) > tx.timeout {
		// 如果检测到超时，需要升级为写锁来修改状态
		tx.mutex.RUnlock()
		tx.mutex.Lock()

		// 重新检查状态，因为可能在获取写锁期间已被其他goroutine修改
		if tx.status == TxStatusActive && time.Since(tx.startTime) > tx.timeout {
			tx.status = TxStatusAborted
		}

		status = tx.status
		tx.mutex.Unlock()
		return status
	}

	tx.mutex.RUnlock()
	return status
}

// GetMulti 批量获取多个键的值
func (tx *Transaction) GetMulti(keys []string) (map[string][]byte, error) {
	tx.mutex.RLock()
	defer tx.mutex.RUnlock()

	// 检查事务状态
	if tx.status != TxStatusActive {
		return nil, ErrTxNotActive
	}

	// 检查事务是否超时
	if time.Since(tx.startTime) > tx.timeout {
		tx.status = TxStatusAborted
		return nil, ErrTxTimeout
	}

	result := make(map[string][]byte, len(keys))
	var missingKeys []string

	// 首先检查写集合
	for _, key := range keys {
		if value, ok := tx.writeSet[key]; ok {
			if value == nil { // 键已被删除
				// 跳过已删除的键
				continue
			}
			result[key] = value
		} else {
			missingKeys = append(missingKeys, key)
		}
	}

	// 对于写集合中没有的键，从数据库中获取
	if len(missingKeys) > 0 {
		dbValues, err := tx.db.GetMulti(missingKeys)
		if err != nil {
			return nil, err
		}

		// 合并结果
		for k, v := range dbValues {
			result[k] = v
			// 添加到读集合
			tx.readSet[k] = struct{}{}
		}
	}

	return result, nil
}
