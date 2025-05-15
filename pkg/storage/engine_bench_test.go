package storage

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

const (
	// 测试使用的数据库名
	testDBName = "benchmark_db"
	// 测试使用的表名
	testTableName = "benchmark_table"
	// 基准测试的键值数量
	benchmarkDataCount = 100000
	// 读操作比例
	readRatio = 0.8
)

// 创建测试表结构
func createTestSchema() *TableSchema {
	return &TableSchema{
		Columns: []ColumnDefinition{
			{
				Name:    "id",
				Type:    TypeInt,
				Primary: true,
			},
			{
				Name: "name",
				Type: TypeString,
			},
			{
				Name: "value",
				Type: TypeInt,
			},
		},
		Indexes: []IndexDefinition{
			{
				Name:    "idx_id",
				Columns: []string{"id"},
				Unique:  true,
			},
		},
	}
}

// 生成随机键
func generateKey(i int) []byte {
	return []byte(fmt.Sprintf("key_%010d", i))
}

// 生成随机值
func generateValue(i int, size int) []byte {
	value := make([]byte, size)
	rand.Read(value)
	return value
}

// 设置基准测试环境
func setupBenchmarkEnv(b *testing.B, engine Engine) {
	ctx := context.Background()

	// 创建数据库
	if err := engine.CreateDB(ctx, testDBName); err != nil {
		b.Fatalf("创建数据库失败: %v", err)
	}

	// 创建表
	schema := createTestSchema()
	if err := engine.CreateTable(ctx, testDBName, testTableName, schema); err != nil {
		b.Fatalf("创建表失败: %v", err)
	}
}

// 清理基准测试环境
func cleanupBenchmarkEnv(b *testing.B, engine Engine) {
	ctx := context.Background()

	// 删除数据库
	if err := engine.DropDB(ctx, testDBName); err != nil {
		b.Fatalf("删除数据库失败: %v", err)
	}
}

// 加载测试数据
func loadBenchmarkData(b *testing.B, engine Engine, count int, valueSize int) {
	ctx := context.Background()

	// 开启事务批量插入
	tx, err := engine.BeginTx(ctx)
	if err != nil {
		b.Fatalf("开始事务失败: %v", err)
	}

	// 批量插入数据
	for i := 0; i < count; i++ {
		key := generateKey(i)
		value := generateValue(i, valueSize)
		if err := tx.Put(ctx, testDBName, testTableName, key, value); err != nil {
			tx.Rollback(ctx)
			b.Fatalf("插入数据失败: %v", err)
		}

		// 每1000条提交一次，避免事务过大
		if (i+1)%1000 == 0 {
			if err := tx.Commit(ctx); err != nil {
				b.Fatalf("提交事务失败: %v", err)
			}
			// 开启新事务
			tx, err = engine.BeginTx(ctx)
			if err != nil {
				b.Fatalf("开始事务失败: %v", err)
			}
		}
	}

	// 提交最后一批
	if err := tx.Commit(ctx); err != nil {
		b.Fatalf("提交事务失败: %v", err)
	}
}

// 获取临时目录
func getTempDir(prefix string) string {
	tmpDir, err := os.MkdirTemp("", prefix)
	if err != nil {
		panic(fmt.Sprintf("创建临时目录失败: %v", err))
	}
	return tmpDir
}

// BenchmarkMemoryEngine_OLTP 测试内存引擎在OLTP场景(读多写少)下的性能
func BenchmarkMemoryEngine_OLTP(b *testing.B) {
	engine := NewMemoryEngine()
	if err := engine.Open(); err != nil {
		b.Fatalf("打开内存引擎失败: %v", err)
	}
	defer engine.Close()

	setupBenchmarkEnv(b, engine)
	defer cleanupBenchmarkEnv(b, engine)

	// 加载测试数据，每条记录128字节
	loadBenchmarkData(b, engine, benchmarkDataCount, 128)

	ctx := context.Background()
	rand.Seed(time.Now().UnixNano())

	b.ResetTimer()

	// 在benchmark循环中执行操作
	for i := 0; i < b.N; i++ {
		// 随机决定是读还是写
		if rand.Float64() < readRatio {
			// 读操作：随机获取一条数据
			idx := rand.Intn(benchmarkDataCount)
			key := generateKey(idx)
			_, err := engine.Get(ctx, testDBName, testTableName, key)
			if err != nil && err != ErrKeyNotFound {
				b.Fatalf("读取数据失败: %v", err)
			}
		} else {
			// 写操作：随机更新一条数据
			idx := rand.Intn(benchmarkDataCount)
			key := generateKey(idx)
			value := generateValue(idx, 128)
			if err := engine.Put(ctx, testDBName, testTableName, key, value); err != nil {
				b.Fatalf("更新数据失败: %v", err)
			}
		}
	}
}

// BenchmarkDiskEngine_OLTP 测试磁盘引擎在OLTP场景(读多写少)下的性能
func BenchmarkDiskEngine_OLTP(b *testing.B) {
	// 创建临时目录
	tmpDir := getTempDir("disk_oltp_bench")
	defer os.RemoveAll(tmpDir)

	engine := NewDiskEngine(tmpDir)
	if err := engine.Open(); err != nil {
		b.Fatalf("打开磁盘引擎失败: %v", err)
	}
	defer engine.Close()

	setupBenchmarkEnv(b, engine)
	defer cleanupBenchmarkEnv(b, engine)

	// 加载测试数据，每条记录128字节
	loadBenchmarkData(b, engine, benchmarkDataCount, 128)

	ctx := context.Background()
	rand.Seed(time.Now().UnixNano())

	b.ResetTimer()

	// 在benchmark循环中执行操作
	for i := 0; i < b.N; i++ {
		// 随机决定是读还是写
		if rand.Float64() < readRatio {
			// 读操作：随机获取一条数据
			idx := rand.Intn(benchmarkDataCount)
			key := generateKey(idx)
			_, err := engine.Get(ctx, testDBName, testTableName, key)
			if err != nil && err != ErrKeyNotFound {
				b.Fatalf("读取数据失败: %v", err)
			}
		} else {
			// 写操作：随机更新一条数据
			idx := rand.Intn(benchmarkDataCount)
			key := generateKey(idx)
			value := generateValue(idx, 128)
			if err := engine.Put(ctx, testDBName, testTableName, key, value); err != nil {
				b.Fatalf("更新数据失败: %v", err)
			}
		}
	}
}

// BenchmarkHybridEngine_OLTP 测试混合引擎在OLTP场景(读多写少)下的性能
func BenchmarkHybridEngine_OLTP(b *testing.B) {
	// 创建临时目录
	tmpDir := getTempDir("hybrid_oltp_bench")
	defer os.RemoveAll(tmpDir)

	engine := NewHybridEngine(tmpDir, 1000, 64, true)
	if err := engine.Open(); err != nil {
		b.Fatalf("打开混合引擎失败: %v", err)
	}
	defer engine.Close()

	setupBenchmarkEnv(b, engine)
	defer cleanupBenchmarkEnv(b, engine)

	// 加载测试数据，每条记录128字节
	loadBenchmarkData(b, engine, benchmarkDataCount, 128)

	ctx := context.Background()
	rand.Seed(time.Now().UnixNano())

	b.ResetTimer()

	// 在benchmark循环中执行操作
	for i := 0; i < b.N; i++ {
		// 随机决定是读还是写
		if rand.Float64() < readRatio {
			// 读操作：随机获取一条数据
			idx := rand.Intn(benchmarkDataCount)
			key := generateKey(idx)
			_, err := engine.Get(ctx, testDBName, testTableName, key)
			if err != nil && err != ErrKeyNotFound {
				b.Fatalf("读取数据失败: %v", err)
			}
		} else {
			// 写操作：随机更新一条数据
			idx := rand.Intn(benchmarkDataCount)
			key := generateKey(idx)
			value := generateValue(idx, 128)
			if err := engine.Put(ctx, testDBName, testTableName, key, value); err != nil {
				b.Fatalf("更新数据失败: %v", err)
			}
		}
	}
}

// BenchmarkMemoryEngine_OLAP 测试内存引擎在OLAP场景(全表扫描)下的性能
func BenchmarkMemoryEngine_OLAP(b *testing.B) {
	engine := NewMemoryEngine()
	if err := engine.Open(); err != nil {
		b.Fatalf("打开内存引擎失败: %v", err)
	}
	defer engine.Close()

	setupBenchmarkEnv(b, engine)
	defer cleanupBenchmarkEnv(b, engine)

	// 加载测试数据，每条记录128字节
	loadBenchmarkData(b, engine, benchmarkDataCount, 128)

	ctx := context.Background()

	b.ResetTimer()

	// 在benchmark循环中执行操作
	for i := 0; i < b.N; i++ {
		// 全表扫描
		iter, err := engine.Scan(ctx, testDBName, testTableName, nil, nil)
		if err != nil {
			b.Fatalf("扫描数据失败: %v", err)
		}

		// 统计记录数
		count := 0
		for iter.Next() {
			count++
		}

		// 检查是否扫描了全部记录
		if count != benchmarkDataCount {
			b.Fatalf("扫描记录数不正确，期望: %d, 实际: %d", benchmarkDataCount, count)
		}

		iter.Close()
	}
}

// BenchmarkDiskEngine_OLAP 测试磁盘引擎在OLAP场景(全表扫描)下的性能
func BenchmarkDiskEngine_OLAP(b *testing.B) {
	// 创建临时目录
	tmpDir := getTempDir("disk_olap_bench")
	defer os.RemoveAll(tmpDir)

	engine := NewDiskEngine(tmpDir)
	if err := engine.Open(); err != nil {
		b.Fatalf("打开磁盘引擎失败: %v", err)
	}
	defer engine.Close()

	setupBenchmarkEnv(b, engine)
	defer cleanupBenchmarkEnv(b, engine)

	// 加载测试数据，每条记录128字节
	loadBenchmarkData(b, engine, benchmarkDataCount, 128)

	ctx := context.Background()

	b.ResetTimer()

	// 在benchmark循环中执行操作
	for i := 0; i < b.N; i++ {
		// 全表扫描
		iter, err := engine.Scan(ctx, testDBName, testTableName, nil, nil)
		if err != nil {
			b.Fatalf("扫描数据失败: %v", err)
		}

		// 统计记录数
		count := 0
		for iter.Next() {
			count++
		}

		// 检查是否扫描了全部记录
		if count != benchmarkDataCount {
			b.Fatalf("扫描记录数不正确，期望: %d, 实际: %d", benchmarkDataCount, count)
		}

		iter.Close()
	}
}

// BenchmarkHybridEngine_OLAP 测试混合引擎在OLAP场景(全表扫描)下的性能
func BenchmarkHybridEngine_OLAP(b *testing.B) {
	// 创建临时目录
	tmpDir := getTempDir("hybrid_olap_bench")
	defer os.RemoveAll(tmpDir)

	engine := NewHybridEngine(tmpDir, 1000, 64, true)
	if err := engine.Open(); err != nil {
		b.Fatalf("打开混合引擎失败: %v", err)
	}
	defer engine.Close()

	setupBenchmarkEnv(b, engine)
	defer cleanupBenchmarkEnv(b, engine)

	// 加载测试数据，每条记录128字节
	loadBenchmarkData(b, engine, benchmarkDataCount, 128)

	ctx := context.Background()

	b.ResetTimer()

	// 在benchmark循环中执行操作
	for i := 0; i < b.N; i++ {
		// 全表扫描
		iter, err := engine.Scan(ctx, testDBName, testTableName, nil, nil)
		if err != nil {
			b.Fatalf("扫描数据失败: %v", err)
		}

		// 统计记录数
		count := 0
		for iter.Next() {
			count++
		}

		// 检查是否扫描了全部记录
		if count != benchmarkDataCount {
			b.Fatalf("扫描记录数不正确，期望: %d, 实际: %d", benchmarkDataCount, count)
		}

		iter.Close()
	}
}

// BenchmarkHybridEngine_HTAP 测试混合引擎在HTAP场景(混合OLTP和OLAP)下的性能
func BenchmarkHybridEngine_HTAP(b *testing.B) {
	// 创建临时目录
	tmpDir := getTempDir("hybrid_htap_bench")
	defer os.RemoveAll(tmpDir)

	engine := NewHybridEngine(tmpDir, 1000, 64, true)
	if err := engine.Open(); err != nil {
		b.Fatalf("打开混合引擎失败: %v", err)
	}
	defer engine.Close()

	setupBenchmarkEnv(b, engine)
	defer cleanupBenchmarkEnv(b, engine)

	// 加载测试数据，每条记录128字节
	loadBenchmarkData(b, engine, benchmarkDataCount, 128)

	ctx := context.Background()
	rand.Seed(time.Now().UnixNano())

	b.ResetTimer()

	// 在benchmark循环中执行操作
	for i := 0; i < b.N; i++ {
		// 随机决定是OLTP还是OLAP操作
		if rand.Float64() < 0.9 { // 90% OLTP, 10% OLAP
			// OLTP操作：随机读写
			if rand.Float64() < readRatio {
				// 读操作：随机获取一条数据
				idx := rand.Intn(benchmarkDataCount)
				key := generateKey(idx)
				_, err := engine.Get(ctx, testDBName, testTableName, key)
				if err != nil && err != ErrKeyNotFound {
					b.Fatalf("读取数据失败: %v", err)
				}
			} else {
				// 写操作：随机更新一条数据
				idx := rand.Intn(benchmarkDataCount)
				key := generateKey(idx)
				value := generateValue(idx, 128)
				if err := engine.Put(ctx, testDBName, testTableName, key, value); err != nil {
					b.Fatalf("更新数据失败: %v", err)
				}
			}
		} else {
			// OLAP操作：范围扫描
			startIdx := rand.Intn(benchmarkDataCount / 2)
			endIdx := startIdx + benchmarkDataCount/10 // 扫描约10%的数据
			if endIdx > benchmarkDataCount {
				endIdx = benchmarkDataCount
			}

			startKey := generateKey(startIdx)
			endKey := generateKey(endIdx)

			iter, err := engine.Scan(ctx, testDBName, testTableName, startKey, endKey)
			if err != nil {
				b.Fatalf("扫描数据失败: %v", err)
			}

			// 统计记录数
			count := 0
			for iter.Next() {
				count++
			}

			iter.Close()
		}
	}
}

// 功能测试：测试混合引擎的正确性
func TestHybridEngine(t *testing.T) {
	// 创建临时目录
	tmpDir := getTempDir("hybrid_test")
	defer os.RemoveAll(tmpDir)

	// 创建引擎
	engine := NewHybridEngine(tmpDir, 100, 8, false)
	if err := engine.Open(); err != nil {
		t.Fatalf("打开混合引擎失败: %v", err)
	}
	defer engine.Close()

	ctx := context.Background()

	// 创建数据库
	if err := engine.CreateDB(ctx, "testdb"); err != nil {
		t.Fatalf("创建数据库失败: %v", err)
	}

	// 创建表
	schema := createTestSchema()
	if err := engine.CreateTable(ctx, "testdb", "testtable", schema); err != nil {
		t.Fatalf("创建表失败: %v", err)
	}

	// 插入数据
	testData := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("key1"), []byte("value1")},
		{[]byte("key2"), []byte("value2")},
		{[]byte("key3"), []byte("value3")},
	}

	for _, data := range testData {
		if err := engine.Put(ctx, "testdb", "testtable", data.key, data.value); err != nil {
			t.Fatalf("插入数据失败: %v", err)
		}
	}

	// 测试读取
	for _, data := range testData {
		value, err := engine.Get(ctx, "testdb", "testtable", data.key)
		if err != nil {
			t.Fatalf("读取数据失败: %v", err)
		}
		if string(value) != string(data.value) {
			t.Fatalf("读取数据不一致，期望: %s, 实际: %s", string(data.value), string(value))
		}
	}

	// 测试缓冲区刷盘
	if err := engine.FlushBuffers(); err != nil {
		t.Fatalf("刷盘失败: %v", err)
	}

	// 再次测试读取
	for _, data := range testData {
		value, err := engine.Get(ctx, "testdb", "testtable", data.key)
		if err != nil {
			t.Fatalf("刷盘后读取数据失败: %v", err)
		}
		if string(value) != string(data.value) {
			t.Fatalf("刷盘后读取数据不一致，期望: %s, 实际: %s", string(data.value), string(value))
		}
	}

	// 测试删除
	if err := engine.Delete(ctx, "testdb", "testtable", []byte("key2")); err != nil {
		t.Fatalf("删除数据失败: %v", err)
	}

	// 验证删除
	_, err := engine.Get(ctx, "testdb", "testtable", []byte("key2"))
	if err != ErrKeyNotFound {
		t.Fatalf("删除后仍能读取数据: %v", err)
	}

	// 测试刷盘后的删除持久性
	if err := engine.FlushBuffers(); err != nil {
		t.Fatalf("刷盘失败: %v", err)
	}

	_, err = engine.Get(ctx, "testdb", "testtable", []byte("key2"))
	if err != ErrKeyNotFound {
		t.Fatalf("刷盘后删除的数据仍能读取: %v", err)
	}

	// 测试扫描
	iter, err := engine.Scan(ctx, "testdb", "testtable", nil, nil)
	if err != nil {
		t.Fatalf("扫描数据失败: %v", err)
	}

	count := 0
	for iter.Next() {
		count++
		key := string(iter.Key())
		value := string(iter.Value())

		found := false
		for _, data := range testData {
			if string(data.key) == key && string(data.key) != "key2" {
				if string(data.value) != value {
					t.Fatalf("扫描数据不一致，键: %s, 期望值: %s, 实际值: %s", key, string(data.value), value)
				}
				found = true
				break
			}
		}

		if !found && key != "key2" {
			t.Fatalf("扫描到未知的键: %s", key)
		}
	}

	iter.Close()

	// 期望扫描到2条数据（key1和key3，key2已删除）
	if count != 2 {
		t.Fatalf("扫描记录数不正确，期望: 2, 实际: %d", count)
	}

	// 测试事务
	tx, err := engine.BeginTx(ctx)
	if err != nil {
		t.Fatalf("开始事务失败: %v", err)
	}

	// 事务中更新
	if err := tx.Put(ctx, "testdb", "testtable", []byte("key1"), []byte("value1_updated")); err != nil {
		t.Fatalf("事务中更新数据失败: %v", err)
	}

	// 事务中插入
	if err := tx.Put(ctx, "testdb", "testtable", []byte("key4"), []byte("value4")); err != nil {
		t.Fatalf("事务中插入数据失败: %v", err)
	}

	// 事务中读取（应该能看到自己的更改）
	value, err := tx.Get(ctx, "testdb", "testtable", []byte("key1"))
	if err != nil {
		t.Fatalf("事务中读取数据失败: %v", err)
	}
	if string(value) != "value1_updated" {
		t.Fatalf("事务中读取数据不一致，期望: value1_updated, 实际: %s", string(value))
	}

	// 提交事务
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("提交事务失败: %v", err)
	}

	// 验证事务提交后的更改
	value, err = engine.Get(ctx, "testdb", "testtable", []byte("key1"))
	if err != nil {
		t.Fatalf("提交后读取数据失败: %v", err)
	}
	if string(value) != "value1_updated" {
		t.Fatalf("提交后读取数据不一致，期望: value1_updated, 实际: %s", string(value))
	}

	value, err = engine.Get(ctx, "testdb", "testtable", []byte("key4"))
	if err != nil {
		t.Fatalf("提交后读取新插入数据失败: %v", err)
	}
	if string(value) != "value4" {
		t.Fatalf("提交后读取新插入数据不一致，期望: value4, 实际: %s", string(value))
	}

	// 删除数据库
	if err := engine.DropDB(ctx, "testdb"); err != nil {
		t.Fatalf("删除数据库失败: %v", err)
	}
}
