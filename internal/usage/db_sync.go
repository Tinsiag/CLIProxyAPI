// Package usage provides asynchronous synchronization of usage statistics to database.
package usage

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

// DatabaseSyncPlugin implements coreusage.Plugin to synchronize usage data to database.
type DatabaseSyncPlugin struct {
	store     UsageDBStore
	config    *config.UsageStorageConfig

	// Buffer for async writes
	buffer     chan *RequestDetailRecord
	bufferMu   sync.Mutex

	// Hourly aggregation buffers
	hourlyStats    map[string]*HourlyStatsRecord
	globalHourly   map[string]*GlobalHourlyRecord
	hourlyMu       sync.Mutex

	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	running atomic.Bool
}

// NewDatabaseSyncPlugin creates a new database sync plugin.
func NewDatabaseSyncPlugin(store UsageDBStore, cfg *config.UsageStorageConfig) *DatabaseSyncPlugin {
	if cfg == nil {
		cfg = config.DefaultUsageStorageConfig()
	}

	bufferSize := cfg.Sync.BufferSize
	if bufferSize <= 0 {
		bufferSize = 1000
	}

	return &DatabaseSyncPlugin{
		store:        store,
		config:       cfg,
		buffer:       make(chan *RequestDetailRecord, bufferSize),
		hourlyStats:  make(map[string]*HourlyStatsRecord),
		globalHourly: make(map[string]*GlobalHourlyRecord),
	}
}

// Start begins the background synchronization goroutines.
func (p *DatabaseSyncPlugin) Start() error {
	if p.running.Swap(true) {
		return nil // already running
	}

	p.ctx, p.cancel = context.WithCancel(context.Background())

	// Start flush loop
	p.wg.Add(1)
	go p.flushLoop()

	// Start aggregation loop (runs every hour)
	p.wg.Add(1)
	go p.aggregationLoop()

	// Start cleanup loop (runs daily)
	p.wg.Add(1)
	go p.cleanupLoop()

	log.Info("Usage database sync plugin started")
	return nil
}

// HandleUsage implements coreusage.Plugin.
func (p *DatabaseSyncPlugin) HandleUsage(ctx context.Context, record coreusage.Record) {
	if !p.running.Load() {
		return
	}

	log.WithFields(log.Fields{
		"model":  record.Model,
		"tokens": record.Detail.TotalTokens,
	}).Debug("DatabaseSyncPlugin.HandleUsage called")

	// Convert record to database format
	detail := p.convertRecord(record)

	// Non-blocking write to buffer
	select {
	case p.buffer <- detail:
	default:
		// Buffer full, log warning and drop
		log.Warn("Usage sync buffer full, dropping record")
	}

	// Update hourly aggregation
	p.updateHourlyAggregation(detail)
}

// convertRecord converts coreusage.Record to RequestDetailRecord.
func (p *DatabaseSyncPlugin) convertRecord(record coreusage.Record) *RequestDetailRecord {
	timestamp := record.RequestedAt
	if timestamp.IsZero() {
		timestamp = time.Now()
	}

	return &RequestDetailRecord{
		APIKeyHash:      fnv1a64Hex(record.APIKey),
		ModelName:       record.Model,
		Source:          record.Source,
		AuthIndex:       record.AuthIndex,
		Timestamp:       timestamp,
		Failed:          record.Failed,
		InputTokens:     record.Detail.InputTokens,
		OutputTokens:    record.Detail.OutputTokens,
		ReasoningTokens: record.Detail.ReasoningTokens,
		CachedTokens:    record.Detail.CachedTokens,
		TotalTokens:     record.Detail.TotalTokens,
	}
}

// fnv1a64Hex computes FNV-1a 64-bit hash and returns as hex string.
func fnv1a64Hex(s string) string {
	if s == "" {
		return "unknown"
	}
	const (
		fnvOffsetBasis = 0xcbf29ce484222325
		fnvPrime       = 0x100000001b3
	)
	h := uint64(fnvOffsetBasis)
	for _, c := range s {
		h ^= uint64(c)
		h *= fnvPrime
	}
	return fmt.Sprintf("%016x", h)
}

// updateHourlyAggregation updates in-memory hourly aggregation.
func (p *DatabaseSyncPlugin) updateHourlyAggregation(detail *RequestDetailRecord) {
	p.hourlyMu.Lock()
	defer p.hourlyMu.Unlock()

	hourKey := fmt.Sprintf("%s|%02d", detail.Timestamp.Format("2006-01-02"), detail.Timestamp.Hour())

	// Per-API hourly stats
	apiHourKey := detail.APIKeyHash + "|" + hourKey
	if stats, ok := p.hourlyStats[apiHourKey]; ok {
		stats.RequestCount++
		stats.TokenCount += detail.TotalTokens
		if detail.Failed {
			stats.FailureCount++
		} else {
			stats.SuccessCount++
		}
	} else {
		p.hourlyStats[apiHourKey] = &HourlyStatsRecord{
			APIKeyHash:   detail.APIKeyHash,
			StatDate:     detail.Timestamp.Truncate(24 * time.Hour),
			StatHour:     detail.Timestamp.Hour(),
			RequestCount: 1,
			TokenCount:   detail.TotalTokens,
			SuccessCount: func() int64 { if detail.Failed { return 0 }; return 1 }(),
			FailureCount: func() int64 { if detail.Failed { return 1 }; return 0 }(),
		}
	}

	// Global hourly stats
	if stats, ok := p.globalHourly[hourKey]; ok {
		stats.RequestCount++
		stats.TokenCount += detail.TotalTokens
		if detail.Failed {
			stats.FailureCount++
		} else {
			stats.SuccessCount++
		}
	} else {
		p.globalHourly[hourKey] = &GlobalHourlyRecord{
			StatDate:     detail.Timestamp.Truncate(24 * time.Hour),
			StatHour:     detail.Timestamp.Hour(),
			RequestCount: 1,
			TokenCount:   detail.TotalTokens,
			SuccessCount: func() int64 { if detail.Failed { return 0 }; return 1 }(),
			FailureCount: func() int64 { if detail.Failed { return 1 }; return 0 }(),
		}
	}
}

// flushLoop periodically flushes buffer to database.
func (p *DatabaseSyncPlugin) flushLoop() {
	defer p.wg.Done()

	interval := p.config.Sync.Interval
	if interval <= 0 {
		interval = 10 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	batchSize := p.config.Sync.BatchSize
	if batchSize <= 0 {
		batchSize = 200
	}

	for {
		select {
		case <-p.ctx.Done():
			p.flush()
			return
		case <-ticker.C:
			p.flush()
		}
	}
}

// flush writes buffered data to database.
func (p *DatabaseSyncPlugin) flush() {
	// Collect details from buffer
	var details []*RequestDetailRecord
	p.bufferMu.Lock()
	for {
		select {
		case d := <-p.buffer:
			details = append(details, d)
			if len(details) >= p.config.Sync.BatchSize {
				goto flush
			}
		default:
			goto flush
		}
	}
flush:
	p.bufferMu.Unlock()

	if len(details) > 0 {
		log.WithField("count", len(details)).Debug("Flushing usage details to database")
		if err := p.store.SaveDetailsBatch(p.ctx, details); err != nil {
			log.WithError(err).Error("Failed to save usage details batch")
		}
	}

	// Flush hourly stats
	p.flushHourlyStats()
}

// flushHourlyStats writes hourly aggregation to database.
func (p *DatabaseSyncPlugin) flushHourlyStats() {
	p.hourlyMu.Lock()
	defer p.hourlyMu.Unlock()

	// Save per-API hourly stats
	for _, stats := range p.hourlyStats {
		if err := p.store.SaveHourlyStats(p.ctx, stats); err != nil {
			log.WithError(err).Error("Failed to save hourly stats")
		}
	}
	p.hourlyStats = make(map[string]*HourlyStatsRecord)

	// Save global hourly stats
	for _, stats := range p.globalHourly {
		if err := p.store.SaveGlobalHourly(p.ctx, stats); err != nil {
			log.WithError(err).Error("Failed to save global hourly stats")
		}
	}
	p.globalHourly = make(map[string]*GlobalHourlyRecord)
}

// aggregationLoop periodically aggregates old detailed data to daily summaries.
func (p *DatabaseSyncPlugin) aggregationLoop() {
	defer p.wg.Done()

	// Run every hour
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			detailDays := p.config.Retention.DetailDays
			if detailDays <= 0 {
				detailDays = 7
			}
			cutoff := time.Now().AddDate(0, 0, -detailDays)
			if err := p.store.AggregateToDaily(p.ctx, cutoff); err != nil {
				log.WithError(err).Error("Failed to aggregate usage to daily")
			} else {
				log.Debug("Aggregated old usage details to daily summaries")
			}
		}
	}
}

// cleanupLoop periodically purges old data.
func (p *DatabaseSyncPlugin) cleanupLoop() {
	defer p.wg.Done()

	// Run daily
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()

			// Purge old hourly stats
			hourlyDays := p.config.Retention.HourlyDays
			if hourlyDays <= 0 {
				hourlyDays = 30
			}
			cutoffHourly := now.AddDate(0, 0, -hourlyDays)
			if err := p.store.PurgeOldHourlyStats(p.ctx, cutoffHourly); err != nil {
				log.WithError(err).Error("Failed to purge old hourly stats")
			}
		}
	}
}

// RestoreToMemory loads historical data from database into memory.
func (p *DatabaseSyncPlugin) RestoreToMemory(stats *RequestStatistics) error {
	if stats == nil {
		return nil
	}

	ctx := context.Background()

	// Load recent detailed records (last N days)
	detailDays := p.config.Recovery.LoadDetailDays
	if detailDays <= 0 {
		detailDays = 7
	}
	sinceDetail := time.Now().AddDate(0, 0, -detailDays)

	details, err := p.store.LoadDetails(ctx, sinceDetail)
	if err != nil {
		return fmt.Errorf("failed to load details: %w", err)
	}

	// Convert details to snapshot and merge
	if len(details) > 0 {
		snapshot := p.detailsToSnapshot(details)
		result := stats.MergeSnapshot(snapshot)
		log.WithFields(log.Fields{
			"added":   result.Added,
			"skipped": result.Skipped,
		}).Info("Restored usage details from database")
	}

	// Load global daily stats for requests/tokens by day
	summaryDays := p.config.Recovery.LoadSummaryDays
	if summaryDays <= 0 {
		summaryDays = 365
	}
	sinceSummary := time.Now().AddDate(0, 0, -summaryDays)

	globalDaily, err := p.store.LoadGlobalDaily(ctx, sinceSummary)
	if err != nil {
		log.WithError(err).Warn("Failed to load global daily stats")
	} else {
		p.mergeGlobalDaily(stats, globalDaily)
	}

	// Load global hourly stats
	globalHourly, err := p.store.LoadGlobalHourly(ctx, sinceDetail)
	if err != nil {
		log.WithError(err).Warn("Failed to load global hourly stats")
	} else {
		p.mergeGlobalHourly(stats, globalHourly)
	}

	return nil
}

// detailsToSnapshot converts database records to StatisticsSnapshot.
func (p *DatabaseSyncPlugin) detailsToSnapshot(details []*RequestDetailRecord) StatisticsSnapshot {
	snapshot := StatisticsSnapshot{
		APIs:           make(map[string]APISnapshot),
		RequestsByDay:  make(map[string]int64),
		RequestsByHour: make(map[string]int64),
		TokensByDay:    make(map[string]int64),
		TokensByHour:   make(map[string]int64),
	}

	for _, d := range details {
		// Update totals
		snapshot.TotalRequests++
		if d.Failed {
			snapshot.FailureCount++
		} else {
			snapshot.SuccessCount++
		}
		snapshot.TotalTokens += d.TotalTokens

		// Update APIs map
		apiSnap, ok := snapshot.APIs[d.APIKeyHash]
		if !ok {
			apiSnap = APISnapshot{Models: make(map[string]ModelSnapshot)}
		}
		apiSnap.TotalRequests++
		apiSnap.TotalTokens += d.TotalTokens

		// Update model stats
		modelSnap := apiSnap.Models[d.ModelName]
		modelSnap.TotalRequests++
		modelSnap.TotalTokens += d.TotalTokens
		modelSnap.Details = append(modelSnap.Details, RequestDetail{
			Timestamp: d.Timestamp,
			Source:    d.Source,
			AuthIndex: d.AuthIndex,
			Tokens: TokenStats{
				InputTokens:     d.InputTokens,
				OutputTokens:    d.OutputTokens,
				ReasoningTokens: d.ReasoningTokens,
				CachedTokens:    d.CachedTokens,
				TotalTokens:     d.TotalTokens,
			},
			Failed: d.Failed,
		})
		apiSnap.Models[d.ModelName] = modelSnap
		snapshot.APIs[d.APIKeyHash] = apiSnap

		// Update day stats
		dayKey := d.Timestamp.Format("2006-01-02")
		snapshot.RequestsByDay[dayKey]++
		snapshot.TokensByDay[dayKey] += d.TotalTokens

		// Update hour stats (use string key "00"-"23")
		hourKey := fmt.Sprintf("%02d", d.Timestamp.Hour())
		snapshot.RequestsByHour[hourKey]++
		snapshot.TokensByHour[hourKey] += d.TotalTokens
	}

	return snapshot
}

// mergeGlobalDaily merges global daily stats into memory.
func (p *DatabaseSyncPlugin) mergeGlobalDaily(stats *RequestStatistics, records []*GlobalDailyRecord) {
	for _, r := range records {
		dayKey := r.StatDate.Format("2006-01-02")
		// Add to existing in-memory counts
		stats.mu.Lock()
		stats.requestsByDay[dayKey] += r.RequestCount
		stats.tokensByDay[dayKey] += r.TokenCount
		stats.mu.Unlock()
	}
	log.WithField("days", len(records)).Debug("Merged global daily stats")
}

// mergeGlobalHourly merges global hourly stats into memory.
func (p *DatabaseSyncPlugin) mergeGlobalHourly(stats *RequestStatistics, records []*GlobalHourlyRecord) {
	for _, r := range records {
		hourKey := r.StatHour
		stats.mu.Lock()
		stats.requestsByHour[hourKey] += r.RequestCount
		stats.tokensByHour[hourKey] += r.TokenCount
		stats.mu.Unlock()
	}
	log.WithField("hours", len(records)).Debug("Merged global hourly stats")
}

// Shutdown stops the plugin and flushes remaining data.
func (p *DatabaseSyncPlugin) Shutdown(ctx context.Context) error {
	if !p.running.Swap(false) {
		return nil // already stopped
	}

	// Stop goroutines
	p.cancel()
	p.wg.Wait()

	// Final flush
	p.flush()

	// Close store
	if err := p.store.Close(); err != nil {
		return fmt.Errorf("failed to close store: %w", err)
	}

	log.Info("Usage database sync plugin stopped")
	return nil
}
