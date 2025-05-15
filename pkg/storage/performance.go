package storage

import (
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cyberdb/cyberdb/pkg/common/logger"
)

// MetricType 指标类型
type MetricType int

const (
	// MetricTypeCounter 计数器类型指标
	MetricTypeCounter MetricType = iota
	// MetricTypeGauge 仪表盘类型指标
	MetricTypeGauge
	// MetricTypeHistogram 直方图类型指标
	MetricTypeHistogram
	// MetricTypeTimer 计时器类型指标
	MetricTypeTimer
)

// Metric 性能指标接口
type Metric interface {
	// Name 获取指标名称
	Name() string
	// Type 获取指标类型
	Type() MetricType
	// Value 获取指标当前值
	Value() interface{}
	// Reset 重置指标
	Reset()
}

// Counter 计数器指标
type Counter struct {
	name  string
	value int64
	mu    sync.RWMutex
}

// NewCounter 创建新的计数器
func NewCounter(name string) *Counter {
	return &Counter{
		name:  name,
		value: 0,
	}
}

// Name 获取指标名称
func (c *Counter) Name() string {
	return c.name
}

// Type 获取指标类型
func (c *Counter) Type() MetricType {
	return MetricTypeCounter
}

// Value 获取指标当前值
func (c *Counter) Value() interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.value
}

// Inc 递增计数器
func (c *Counter) Inc() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value++
}

// Add 增加指定值
func (c *Counter) Add(delta int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value += delta
}

// Reset 重置计数器
func (c *Counter) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value = 0
}

// Gauge 仪表盘指标
type Gauge struct {
	name  string
	value float64
	mu    sync.RWMutex
}

// NewGauge 创建新的仪表盘
func NewGauge(name string) *Gauge {
	return &Gauge{
		name:  name,
		value: 0,
	}
}

// Name 获取指标名称
func (g *Gauge) Name() string {
	return g.name
}

// Type 获取指标类型
func (g *Gauge) Type() MetricType {
	return MetricTypeGauge
}

// Value 获取指标当前值
func (g *Gauge) Value() interface{} {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.value
}

// Set 设置值
func (g *Gauge) Set(value float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.value = value
}

// Inc 递增值
func (g *Gauge) Inc() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.value++
}

// Dec 递减值
func (g *Gauge) Dec() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.value--
}

// Add 增加指定值
func (g *Gauge) Add(delta float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.value += delta
}

// Reset 重置为0
func (g *Gauge) Reset() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.value = 0
}

// Histogram 直方图指标
type Histogram struct {
	name    string
	buckets []float64
	counts  []int64
	sum     float64
	count   int64
	mu      sync.RWMutex
}

// NewHistogram 创建新的直方图
func NewHistogram(name string, buckets []float64) *Histogram {
	return &Histogram{
		name:    name,
		buckets: buckets,
		counts:  make([]int64, len(buckets)+1),
		sum:     0,
		count:   0,
	}
}

// Name 获取指标名称
func (h *Histogram) Name() string {
	return h.name
}

// Type 获取指标类型
func (h *Histogram) Type() MetricType {
	return MetricTypeHistogram
}

// Value 获取指标当前值
func (h *Histogram) Value() interface{} {
	h.mu.RLock()
	defer h.mu.RUnlock()

	result := map[string]interface{}{
		"buckets": make([]map[string]interface{}, len(h.buckets)),
		"sum":     h.sum,
		"count":   h.count,
	}

	for i, threshold := range h.buckets {
		result["buckets"].([]map[string]interface{})[i] = map[string]interface{}{
			"le":    threshold,
			"count": h.counts[i],
		}
	}

	// 添加+Inf桶
	result["buckets"].([]map[string]interface{})[len(h.buckets)] = map[string]interface{}{
		"le":    "+Inf",
		"count": h.counts[len(h.buckets)],
	}

	return result
}

// Observe 观察一个值
func (h *Histogram) Observe(value float64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// 更新总和和计数
	h.sum += value
	h.count++

	// 更新桶计数
	for i, threshold := range h.buckets {
		if value <= threshold {
			h.counts[i]++
			return
		}
	}

	// 如果大于所有桶，更新最后一个桶
	h.counts[len(h.buckets)]++
}

// Reset 重置直方图
func (h *Histogram) Reset() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.sum = 0
	h.count = 0
	for i := range h.counts {
		h.counts[i] = 0
	}
}

// Timer 计时器指标
type Timer struct {
	name      string
	histogram *Histogram
	mu        sync.RWMutex
}

// NewTimer 创建新的计时器
func NewTimer(name string) *Timer {
	// 默认桶：1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s
	buckets := []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}

	return &Timer{
		name:      name,
		histogram: NewHistogram(name, buckets),
	}
}

// Name 获取指标名称
func (t *Timer) Name() string {
	return t.name
}

// Type 获取指标类型
func (t *Timer) Type() MetricType {
	return MetricTypeTimer
}

// Value 获取指标当前值
func (t *Timer) Value() interface{} {
	return t.histogram.Value()
}

// RecordDuration 记录持续时间（毫秒）
func (t *Timer) RecordDuration(duration float64) {
	t.histogram.Observe(duration)
}

// Time 计时函数执行时间
func (t *Timer) Time(f func()) {
	start := time.Now()
	f()
	duration := float64(time.Since(start).Milliseconds())
	t.RecordDuration(duration)
}

// Reset 重置计时器
func (t *Timer) Reset() {
	t.histogram.Reset()
}

// PerformanceMonitor 性能监控组件
type PerformanceMonitor struct {
	metrics   map[string]Metric
	mu        sync.RWMutex
	startTime time.Time

	// 存储性能数据的时间序列
	timeSeries map[string][]TimeSeriesPoint

	// 性能阈值
	thresholds map[string]float64

	// 是否启用
	enabled bool
}

// TimeSeriesPoint 时间序列数据点
type TimeSeriesPoint struct {
	Timestamp time.Time
	Value     interface{}
}

// NewPerformanceMonitor 创建新的性能监控组件
func NewPerformanceMonitor() *PerformanceMonitor {
	return &PerformanceMonitor{
		metrics:    make(map[string]Metric),
		startTime:  time.Now(),
		timeSeries: make(map[string][]TimeSeriesPoint),
		thresholds: make(map[string]float64),
		enabled:    true,
	}
}

// RegisterMetric 注册度量指标
func (pm *PerformanceMonitor) RegisterMetric(metric Metric) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.metrics[metric.Name()] = metric
}

// GetMetric 获取指标
func (pm *PerformanceMonitor) GetMetric(name string) (Metric, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	metric, ok := pm.metrics[name]
	return metric, ok
}

// SetThreshold 设置性能阈值
func (pm *PerformanceMonitor) SetThreshold(metricName string, threshold float64) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.thresholds[metricName] = threshold
}

// Enable 启用监控
func (pm *PerformanceMonitor) Enable() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.enabled = true
}

// Disable 禁用监控
func (pm *PerformanceMonitor) Disable() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.enabled = false
}

// IsEnabled 检查是否启用
func (pm *PerformanceMonitor) IsEnabled() bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	return pm.enabled
}

// RecordMetrics 记录当前指标到时间序列
func (pm *PerformanceMonitor) RecordMetrics() {
	if !pm.IsEnabled() {
		return
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	now := time.Now()

	// 记录每个指标的当前值
	for name, metric := range pm.metrics {
		// 检查时间序列是否存在
		if _, exists := pm.timeSeries[name]; !exists {
			pm.timeSeries[name] = make([]TimeSeriesPoint, 0)
		}

		// 添加当前值
		pm.timeSeries[name] = append(pm.timeSeries[name], TimeSeriesPoint{
			Timestamp: now,
			Value:     metric.Value(),
		})

		// 限制时间序列长度，避免内存占用过高
		if len(pm.timeSeries[name]) > 1000 {
			pm.timeSeries[name] = pm.timeSeries[name][len(pm.timeSeries[name])-1000:]
		}

		// 检查阈值并记录警告
		if threshold, exists := pm.thresholds[name]; exists {
			var currentValue float64

			switch v := metric.Value().(type) {
			case int64:
				currentValue = float64(v)
			case float64:
				currentValue = v
			case map[string]interface{}:
				// 对于直方图和计时器，检查平均值
				if count, ok := v["count"].(int64); ok && count > 0 {
					if sum, ok := v["sum"].(float64); ok {
						currentValue = sum / float64(count)
					}
				}
			}

			if currentValue > threshold {
				logger.Warn(fmt.Sprintf("性能指标 %s 超出阈值: %.2f > %.2f", name, currentValue, threshold))
			}
		}
	}

	// 添加系统指标
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	systemMetrics := map[string]interface{}{
		"goroutines":     runtime.NumGoroutine(),
		"heap_alloc":     memStats.HeapAlloc,
		"heap_idle":      memStats.HeapIdle,
		"heap_released":  memStats.HeapReleased,
		"gc_pause_total": memStats.PauseTotalNs,
		"gc_num":         memStats.NumGC,
	}

	for name, value := range systemMetrics {
		metricName := "system." + name

		if _, exists := pm.timeSeries[metricName]; !exists {
			pm.timeSeries[metricName] = make([]TimeSeriesPoint, 0)
		}

		pm.timeSeries[metricName] = append(pm.timeSeries[metricName], TimeSeriesPoint{
			Timestamp: now,
			Value:     value,
		})

		if len(pm.timeSeries[metricName]) > 1000 {
			pm.timeSeries[metricName] = pm.timeSeries[metricName][len(pm.timeSeries[metricName])-1000:]
		}
	}
}

// StartRecordingTimer 开始记录函数执行时间
func (pm *PerformanceMonitor) StartRecordingTimer(name string) func() {
	if !pm.IsEnabled() {
		return func() {}
	}

	start := time.Now()

	return func() {
		duration := float64(time.Since(start).Milliseconds())

		pm.mu.Lock()
		defer pm.mu.Unlock()

		// 查找或创建计时器
		metric, exists := pm.metrics[name]
		if !exists {
			timer := NewTimer(name)
			pm.metrics[name] = timer
			metric = timer
		}

		// 确保是计时器类型
		if timer, ok := metric.(*Timer); ok {
			timer.RecordDuration(duration)
		} else {
			logger.Error(fmt.Sprintf("指标 %s 不是计时器类型", name))
		}
	}
}

// GetMetricsSnapshot 获取所有指标的快照
func (pm *PerformanceMonitor) GetMetricsSnapshot() map[string]interface{} {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	snapshot := make(map[string]interface{})

	for name, metric := range pm.metrics {
		snapshot[name] = metric.Value()
	}

	// 添加运行时间
	snapshot["uptime_seconds"] = time.Since(pm.startTime).Seconds()

	// 添加系统信息
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	snapshot["system"] = map[string]interface{}{
		"goroutines":    runtime.NumGoroutine(),
		"heap_alloc_mb": float64(memStats.HeapAlloc) / 1024 / 1024,
		"heap_idle_mb":  float64(memStats.HeapIdle) / 1024 / 1024,
		"heap_objects":  memStats.HeapObjects,
		"gc_pause_ms":   float64(memStats.PauseTotalNs) / 1000000,
		"gc_num":        memStats.NumGC,
		"num_cpu":       runtime.NumCPU(),
	}

	return snapshot
}

// GetTimeSeriesData 获取指定指标的时间序列数据
func (pm *PerformanceMonitor) GetTimeSeriesData(name string, duration time.Duration) []TimeSeriesPoint {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	series, exists := pm.timeSeries[name]
	if !exists {
		return nil
	}

	// 如果未指定持续时间，返回所有数据
	if duration == 0 {
		return series
	}

	// 过滤指定时间范围内的数据
	cutoff := time.Now().Add(-duration)
	var result []TimeSeriesPoint

	for _, point := range series {
		if point.Timestamp.After(cutoff) {
			result = append(result, point)
		}
	}

	return result
}

// ExportMetricsJSON 将指标导出为JSON
func (pm *PerformanceMonitor) ExportMetricsJSON() ([]byte, error) {
	snapshot := pm.GetMetricsSnapshot()
	return json.Marshal(snapshot)
}

// ResetAllMetrics 重置所有指标
func (pm *PerformanceMonitor) ResetAllMetrics() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for _, metric := range pm.metrics {
		metric.Reset()
	}

	// 清空时间序列数据
	pm.timeSeries = make(map[string][]TimeSeriesPoint)

	// 更新启动时间
	pm.startTime = time.Now()
}

// PerformanceAdvisor 性能顾问组件
type PerformanceAdvisor struct {
	monitor     *PerformanceMonitor
	rules       []AdvisorRule
	suggestions []Suggestion
	enabled     bool
	mu          sync.RWMutex
}

// AdvisorRule 顾问规则
type AdvisorRule struct {
	Name        string
	Description string
	Evaluate    func(pm *PerformanceMonitor) []Suggestion
}

// Suggestion 优化建议
type Suggestion struct {
	RuleName    string
	Severity    SeverityLevel
	Description string
	Action      string
	Timestamp   time.Time
}

// SeverityLevel 严重级别
type SeverityLevel int

const (
	// SeverityInfo 信息级别
	SeverityInfo SeverityLevel = iota
	// SeverityWarning 警告级别
	SeverityWarning
	// SeverityCritical 严重级别
	SeverityCritical
)

// NewPerformanceAdvisor 创建新的性能顾问
func NewPerformanceAdvisor(monitor *PerformanceMonitor) *PerformanceAdvisor {
	advisor := &PerformanceAdvisor{
		monitor:     monitor,
		rules:       make([]AdvisorRule, 0),
		suggestions: make([]Suggestion, 0),
		enabled:     true,
	}

	// 注册默认规则
	advisor.RegisterDefaultRules()

	return advisor
}

// RegisterRule 注册规则
func (pa *PerformanceAdvisor) RegisterRule(rule AdvisorRule) {
	pa.mu.Lock()
	defer pa.mu.Unlock()

	pa.rules = append(pa.rules, rule)
}

// RegisterDefaultRules 注册默认规则
func (pa *PerformanceAdvisor) RegisterDefaultRules() {
	// 高延迟规则
	pa.RegisterRule(AdvisorRule{
		Name:        "high_latency",
		Description: "检测操作延迟是否过高",
		Evaluate: func(pm *PerformanceMonitor) []Suggestion {
			var suggestions []Suggestion

			// 检查"write_latency"和"read_latency"指标
			for _, metricName := range []string{"write_latency", "read_latency"} {
				metric, exists := pm.GetMetric(metricName)
				if !exists {
					continue
				}

				if timer, ok := metric.(*Timer); ok {
					histData := timer.Value().(map[string]interface{})

					count := histData["count"].(int64)
					if count == 0 {
						continue
					}

					sum := histData["sum"].(float64)
					avgLatency := sum / float64(count)

					// 检查平均延迟
					var severity SeverityLevel
					var action string

					if avgLatency > 100 { // 超过100ms
						severity = SeverityCritical
						action = "考虑添加索引，优化查询，或增加缓存"
					} else if avgLatency > 50 { // 超过50ms
						severity = SeverityWarning
						action = "检查查询效率，考虑批处理操作"
					} else if avgLatency > 20 { // 超过20ms
						severity = SeverityInfo
						action = "监视性能趋势，考虑未来优化"
					} else {
						continue // 性能正常，不需要建议
					}

					opType := "读取"
					if metricName == "write_latency" {
						opType = "写入"
					}

					suggestions = append(suggestions, Suggestion{
						RuleName:    "high_latency",
						Severity:    severity,
						Description: fmt.Sprintf("%s操作的平均延迟为%.2fms，高于推荐值", opType, avgLatency),
						Action:      action,
						Timestamp:   time.Now(),
					})
				}
			}

			return suggestions
		},
	})

	// 内存使用规则
	pa.RegisterRule(AdvisorRule{
		Name:        "memory_usage",
		Description: "检测内存使用是否过高",
		Evaluate: func(pm *PerformanceMonitor) []Suggestion {
			var suggestions []Suggestion

			snapshot := pm.GetMetricsSnapshot()
			if systemInfo, ok := snapshot["system"].(map[string]interface{}); ok {
				heapAllocMB, ok := systemInfo["heap_alloc_mb"].(float64)
				if !ok {
					return suggestions
				}

				var severity SeverityLevel
				var action string

				if heapAllocMB > 1024 { // 超过1GB
					severity = SeverityCritical
					action = "检查内存泄漏，考虑增加垃圾回收频率，或者分割大型数据集"
				} else if heapAllocMB > 512 { // 超过512MB
					severity = SeverityWarning
					action = "监控内存增长趋势，优化大型对象的创建和销毁"
				} else if heapAllocMB > 256 { // 超过256MB
					severity = SeverityInfo
					action = "注意内存使用情况，为未来增长做准备"
				} else {
					return suggestions // 内存使用正常
				}

				suggestions = append(suggestions, Suggestion{
					RuleName:    "memory_usage",
					Severity:    severity,
					Description: fmt.Sprintf("堆内存分配达到%.2fMB", heapAllocMB),
					Action:      action,
					Timestamp:   time.Now(),
				})
			}

			return suggestions
		},
	})

	// 缓存命中率规则
	pa.RegisterRule(AdvisorRule{
		Name:        "cache_hit_ratio",
		Description: "检测缓存命中率是否过低",
		Evaluate: func(pm *PerformanceMonitor) []Suggestion {
			var suggestions []Suggestion

			cacheHitRatioMetric, exists := pm.GetMetric("cache_hit_ratio")
			if !exists {
				return suggestions
			}

			if gauge, ok := cacheHitRatioMetric.(*Gauge); ok {
				hitRatio := gauge.Value().(float64)

				var severity SeverityLevel
				var action string

				if hitRatio < 0.5 { // 低于50%
					severity = SeverityCritical
					action = "调整缓存大小，优化缓存策略，或者预加载常用数据"
				} else if hitRatio < 0.7 { // 低于70%
					severity = SeverityWarning
					action = "分析访问模式，调整缓存替换算法"
				} else if hitRatio < 0.8 { // 低于80%
					severity = SeverityInfo
					action = "考虑微调缓存参数以进一步提高命中率"
				} else {
					return suggestions // 缓存效率良好
				}

				suggestions = append(suggestions, Suggestion{
					RuleName:    "cache_hit_ratio",
					Severity:    severity,
					Description: fmt.Sprintf("缓存命中率为%.1f%%", hitRatio*100),
					Action:      action,
					Timestamp:   time.Now(),
				})
			}

			return suggestions
		},
	})
}

// Analyze 分析性能并生成建议
func (pa *PerformanceAdvisor) Analyze() []Suggestion {
	if !pa.IsEnabled() {
		return nil
	}

	pa.mu.Lock()
	defer pa.mu.Unlock()

	var newSuggestions []Suggestion

	// 运行所有规则
	for _, rule := range pa.rules {
		suggestions := rule.Evaluate(pa.monitor)
		if len(suggestions) > 0 {
			newSuggestions = append(newSuggestions, suggestions...)
		}
	}

	// 添加到建议历史
	pa.suggestions = append(pa.suggestions, newSuggestions...)

	// 限制建议历史长度
	if len(pa.suggestions) > 100 {
		pa.suggestions = pa.suggestions[len(pa.suggestions)-100:]
	}

	return newSuggestions
}

// GetSuggestions 获取所有建议
func (pa *PerformanceAdvisor) GetSuggestions() []Suggestion {
	pa.mu.RLock()
	defer pa.mu.RUnlock()

	return pa.suggestions
}

// ClearSuggestions 清空建议历史
func (pa *PerformanceAdvisor) ClearSuggestions() {
	pa.mu.Lock()
	defer pa.mu.Unlock()

	pa.suggestions = make([]Suggestion, 0)
}

// Enable 启用顾问
func (pa *PerformanceAdvisor) Enable() {
	pa.mu.Lock()
	defer pa.mu.Unlock()

	pa.enabled = true
}

// Disable 禁用顾问
func (pa *PerformanceAdvisor) Disable() {
	pa.mu.Lock()
	defer pa.mu.Unlock()

	pa.enabled = false
}

// IsEnabled 检查是否启用
func (pa *PerformanceAdvisor) IsEnabled() bool {
	pa.mu.RLock()
	defer pa.mu.RUnlock()

	return pa.enabled
}

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
