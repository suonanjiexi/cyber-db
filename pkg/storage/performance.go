package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// PerformanceStats 定义存储引擎性能统计信息
type PerformanceStats struct {
	// 基本操作计数器
	GetCount    int64 // 读操作计数
	PutCount    int64 // 写操作计数
	DeleteCount int64 // 删除操作计数
	ScanCount   int64 // 扫描操作计数
	TxnCount    int64 // 事务计数

	// 延迟统计（纳秒）
	GetTotalLatency    int64 // 读操作总延迟
	PutTotalLatency    int64 // 写操作总延迟
	DeleteTotalLatency int64 // 删除操作总延迟
	ScanTotalLatency   int64 // 扫描操作总延迟
	TxnTotalLatency    int64 // 事务总延迟

	// 吞吐量（每秒操作数）
	GetThroughput    float64 // 读吞吐量
	PutThroughput    float64 // 写吞吐量
	DeleteThroughput float64 // 删除吞吐量
	ScanThroughput   float64 // 扫描吞吐量

	// 缓存统计
	CacheHits     int64   // 缓存命中次数
	CacheMisses   int64   // 缓存未命中次数
	CacheHitRatio float64 // 缓存命中率

	// IO统计
	DiskReads    int64 // 磁盘读取次数
	DiskWrites   int64 // 磁盘写入次数
	BytesRead    int64 // 读取字节数
	BytesWritten int64 // 写入字节数

	// 压缩统计
	CompactionCount  int64 // 压缩次数
	CompactionTimeMs int64 // 压缩总耗时（毫秒）

	// 内存使用
	MemoryUsageBytes int64 // 内存使用量（字节）

	// 统计时间范围
	StartTime time.Time // 开始统计时间
	EndTime   time.Time // 结束统计时间

	// 额外信息
	ExtraInfo map[string]interface{} // 额外统计信息
}

// PerformanceCollector 性能数据收集器接口
type PerformanceCollector interface {
	// 记录操作延迟
	RecordGet(latencyNs int64)
	RecordPut(latencyNs int64)
	RecordDelete(latencyNs int64)
	RecordScan(latencyNs int64)
	RecordTxn(latencyNs int64)

	// 记录缓存统计
	RecordCacheHit()
	RecordCacheMiss()

	// 记录IO统计
	RecordDiskRead(bytes int64)
	RecordDiskWrite(bytes int64)

	// 记录压缩统计
	RecordCompaction(durationMs int64)

	// 记录内存使用
	RecordMemoryUsage(bytes int64)

	// 获取统计数据
	GetStats() *PerformanceStats

	// 重置统计数据
	Reset()

	// 开始收集
	Start()

	// 停止收集
	Stop()

	// 添加自定义指标
	AddMetric(name string, value interface{})
}

// DefaultPerformanceCollector 默认性能收集器实现
type DefaultPerformanceCollector struct {
	stats    PerformanceStats
	mu       sync.RWMutex
	running  bool
	stopChan chan struct{}
	interval time.Duration // 统计间隔
}

// NewPerformanceCollector 创建新的性能收集器
func NewPerformanceCollector(intervalSeconds int) PerformanceCollector {
	if intervalSeconds <= 0 {
		intervalSeconds = 10 // 默认10秒
	}

	return &DefaultPerformanceCollector{
		stats: PerformanceStats{
			StartTime: time.Now(),
			ExtraInfo: make(map[string]interface{}),
		},
		interval: time.Duration(intervalSeconds) * time.Second,
		stopChan: make(chan struct{}),
	}
}

// Start 开始收集性能数据
func (c *DefaultPerformanceCollector) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return
	}

	c.running = true
	c.stats.StartTime = time.Now()

	// 启动周期性统计计算
	go c.periodicCalculation()
}

// Stop 停止收集性能数据
func (c *DefaultPerformanceCollector) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.running {
		return
	}

	c.running = false
	c.stats.EndTime = time.Now()
	close(c.stopChan)
}

// 周期性计算吞吐量和其他衍生指标
func (c *DefaultPerformanceCollector) periodicCalculation() {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	var lastGetCount, lastPutCount, lastDeleteCount, lastScanCount int64

	for {
		select {
		case <-ticker.C:
			c.mu.Lock()

			// 计算时间窗口
			duration := float64(c.interval.Seconds())

			// 获取当前计数
			currentGetCount := atomic.LoadInt64(&c.stats.GetCount)
			currentPutCount := atomic.LoadInt64(&c.stats.PutCount)
			currentDeleteCount := atomic.LoadInt64(&c.stats.DeleteCount)
			currentScanCount := atomic.LoadInt64(&c.stats.ScanCount)

			// 计算吞吐量
			c.stats.GetThroughput = float64(currentGetCount-lastGetCount) / duration
			c.stats.PutThroughput = float64(currentPutCount-lastPutCount) / duration
			c.stats.DeleteThroughput = float64(currentDeleteCount-lastDeleteCount) / duration
			c.stats.ScanThroughput = float64(currentScanCount-lastScanCount) / duration

			// 更新上次计数
			lastGetCount = currentGetCount
			lastPutCount = currentPutCount
			lastDeleteCount = currentDeleteCount
			lastScanCount = currentScanCount

			// 计算缓存命中率
			totalCacheAccess := atomic.LoadInt64(&c.stats.CacheHits) + atomic.LoadInt64(&c.stats.CacheMisses)
			if totalCacheAccess > 0 {
				c.stats.CacheHitRatio = float64(c.stats.CacheHits) / float64(totalCacheAccess)
			}

			c.mu.Unlock()

		case <-c.stopChan:
			return
		}
	}
}

// RecordGet 记录Get操作
func (c *DefaultPerformanceCollector) RecordGet(latencyNs int64) {
	atomic.AddInt64(&c.stats.GetCount, 1)
	atomic.AddInt64(&c.stats.GetTotalLatency, latencyNs)
}

// RecordPut 记录Put操作
func (c *DefaultPerformanceCollector) RecordPut(latencyNs int64) {
	atomic.AddInt64(&c.stats.PutCount, 1)
	atomic.AddInt64(&c.stats.PutTotalLatency, latencyNs)
}

// RecordDelete 记录Delete操作
func (c *DefaultPerformanceCollector) RecordDelete(latencyNs int64) {
	atomic.AddInt64(&c.stats.DeleteCount, 1)
	atomic.AddInt64(&c.stats.DeleteTotalLatency, latencyNs)
}

// RecordScan 记录Scan操作
func (c *DefaultPerformanceCollector) RecordScan(latencyNs int64) {
	atomic.AddInt64(&c.stats.ScanCount, 1)
	atomic.AddInt64(&c.stats.ScanTotalLatency, latencyNs)
}

// RecordTxn 记录事务操作
func (c *DefaultPerformanceCollector) RecordTxn(latencyNs int64) {
	atomic.AddInt64(&c.stats.TxnCount, 1)
	atomic.AddInt64(&c.stats.TxnTotalLatency, latencyNs)
}

// RecordCacheHit 记录缓存命中
func (c *DefaultPerformanceCollector) RecordCacheHit() {
	atomic.AddInt64(&c.stats.CacheHits, 1)
}

// RecordCacheMiss 记录缓存未命中
func (c *DefaultPerformanceCollector) RecordCacheMiss() {
	atomic.AddInt64(&c.stats.CacheMisses, 1)
}

// RecordDiskRead 记录磁盘读取
func (c *DefaultPerformanceCollector) RecordDiskRead(bytes int64) {
	atomic.AddInt64(&c.stats.DiskReads, 1)
	atomic.AddInt64(&c.stats.BytesRead, bytes)
}

// RecordDiskWrite 记录磁盘写入
func (c *DefaultPerformanceCollector) RecordDiskWrite(bytes int64) {
	atomic.AddInt64(&c.stats.DiskWrites, 1)
	atomic.AddInt64(&c.stats.BytesWritten, bytes)
}

// RecordCompaction 记录压缩操作
func (c *DefaultPerformanceCollector) RecordCompaction(durationMs int64) {
	atomic.AddInt64(&c.stats.CompactionCount, 1)
	atomic.AddInt64(&c.stats.CompactionTimeMs, durationMs)
}

// RecordMemoryUsage 记录内存使用
func (c *DefaultPerformanceCollector) RecordMemoryUsage(bytes int64) {
	atomic.StoreInt64(&c.stats.MemoryUsageBytes, bytes)
}

// GetStats 获取统计信息
func (c *DefaultPerformanceCollector) GetStats() *PerformanceStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// 创建副本
	statsCopy := c.stats

	// 更新结束时间
	statsCopy.EndTime = time.Now()

	// 深拷贝额外信息
	statsCopy.ExtraInfo = make(map[string]interface{})
	for k, v := range c.stats.ExtraInfo {
		statsCopy.ExtraInfo[k] = v
	}

	return &statsCopy
}

// Reset 重置统计数据
func (c *DefaultPerformanceCollector) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stats = PerformanceStats{
		StartTime: time.Now(),
		ExtraInfo: make(map[string]interface{}),
	}
}

// AddMetric 添加自定义指标
func (c *DefaultPerformanceCollector) AddMetric(name string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stats.ExtraInfo[name] = value
}

// PerformanceAdvisor 性能顾问接口，分析性能数据并提供优化建议
type PerformanceAdvisor interface {
	// 分析性能数据
	Analyze(stats *PerformanceStats) []string
}

// DefaultPerformanceAdvisor 默认性能顾问实现
type DefaultPerformanceAdvisor struct {
	// 性能阈值
	slowGetThresholdNs      int64   // 慢读阈值（纳秒）
	slowPutThresholdNs      int64   // 慢写阈值（纳秒）
	lowCacheHitRatio        float64 // 低缓存命中率阈值
	highCompactionTimeRatio float64 // 高压缩时间比例阈值
}

// NewPerformanceAdvisor 创建新的性能顾问
func NewPerformanceAdvisor() PerformanceAdvisor {
	return &DefaultPerformanceAdvisor{
		slowGetThresholdNs:      5000000,  // 5毫秒
		slowPutThresholdNs:      10000000, // 10毫秒
		lowCacheHitRatio:        0.8,      // 80%
		highCompactionTimeRatio: 0.1,      // 10%
	}
}

// Analyze 分析性能数据，给出优化建议
func (a *DefaultPerformanceAdvisor) Analyze(stats *PerformanceStats) []string {
	var suggestions []string

	// 计算平均延迟
	var avgGetLatencyNs, avgPutLatencyNs, avgDeleteLatencyNs, avgScanLatencyNs int64

	if stats.GetCount > 0 {
		avgGetLatencyNs = stats.GetTotalLatency / stats.GetCount
	}
	if stats.PutCount > 0 {
		avgPutLatencyNs = stats.PutTotalLatency / stats.PutCount
	}
	if stats.DeleteCount > 0 {
		avgDeleteLatencyNs = stats.DeleteTotalLatency / stats.DeleteCount
	}
	if stats.ScanCount > 0 {
		avgScanLatencyNs = stats.ScanTotalLatency / stats.ScanCount
	}

	// 计算运行时间（秒）
	duration := stats.EndTime.Sub(stats.StartTime).Seconds()

	// 检查读延迟
	if avgGetLatencyNs > a.slowGetThresholdNs {
		suggestions = append(suggestions, fmt.Sprintf(
			"读操作平均延迟(%d纳秒)超过阈值(%d纳秒)，建议增加缓存大小或优化索引",
			avgGetLatencyNs, a.slowGetThresholdNs))
	}

	// 检查写延迟
	if avgPutLatencyNs > a.slowPutThresholdNs {
		suggestions = append(suggestions, fmt.Sprintf(
			"写操作平均延迟(%d纳秒)超过阈值(%d纳秒)，建议调整写缓冲区大小或降低WAL同步频率",
			avgPutLatencyNs, a.slowPutThresholdNs))
	}

	// 检查缓存命中率
	if stats.CacheHitRatio < a.lowCacheHitRatio && (stats.CacheHits+stats.CacheMisses) > 1000 {
		suggestions = append(suggestions, fmt.Sprintf(
			"缓存命中率(%.2f%%)过低，建议增加缓存大小或调整缓存策略",
			stats.CacheHitRatio*100))
	}

	// 检查压缩时间占比
	compactionTimeSeconds := float64(stats.CompactionTimeMs) / 1000.0
	compactionTimeRatio := compactionTimeSeconds / duration
	if compactionTimeRatio > a.highCompactionTimeRatio && stats.CompactionCount > 5 {
		suggestions = append(suggestions, fmt.Sprintf(
			"压缩操作占用时间比例(%.2f%%)过高，建议调整压缩策略或增加专用压缩线程",
			compactionTimeRatio*100))
	}

	// 检查读写比例
	if stats.PutCount+stats.DeleteCount > 0 {
		readWriteRatio := float64(stats.GetCount) / float64(stats.PutCount+stats.DeleteCount)
		if readWriteRatio < 1.0 {
			suggestions = append(suggestions, "写操作比例过高，建议使用批量写入或调整为写优化模式")
		} else if readWriteRatio > 10.0 {
			suggestions = append(suggestions, "读操作比例过高，建议使用读优化配置或增加索引")
		}
	}

	// 检查内存使用
	if stats.MemoryUsageBytes > 1073741824 { // 1GB
		suggestions = append(suggestions, fmt.Sprintf(
			"内存使用量(%.2fGB)较高，建议检查内存泄漏或调整内存限制",
			float64(stats.MemoryUsageBytes)/(1024*1024*1024)))
	}

	// 如果没有问题，添加一个积极的反馈
	if len(suggestions) == 0 {
		suggestions = append(suggestions, "性能指标良好，未发现明显问题")
	}

	return suggestions
}
