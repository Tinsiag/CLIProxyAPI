// Package usage provides database storage interface for usage statistics persistence.
package usage

import (
	"context"
	"time"
)

// RequestDetailRecord represents a single request detail for database storage.
type RequestDetailRecord struct {
	ID            int64     `json:"id"`
	APIKeyHash    string    `json:"api_key_hash"`
	ModelName     string    `json:"model_name"`
	Source        string    `json:"source"`
	AuthIndex     string    `json:"auth_index"`
	Timestamp     time.Time `json:"timestamp"`
	Failed        bool      `json:"failed"`
	InputTokens   int64     `json:"input_tokens"`
	OutputTokens  int64     `json:"output_tokens"`
	ReasoningTokens int64   `json:"reasoning_tokens"`
	CachedTokens  int64     `json:"cached_tokens"`
	TotalTokens   int64     `json:"total_tokens"`
}

// DailySummaryRecord represents aggregated daily statistics for database storage.
type DailySummaryRecord struct {
	ID              int64     `json:"id"`
	APIKeyHash      string    `json:"api_key_hash"`
	ModelName       string    `json:"model_name"`
	Source          string    `json:"source"`
	StatDate        time.Time `json:"stat_date"`
	TotalRequests   int64     `json:"total_requests"`
	SuccessCount    int64     `json:"success_count"`
	FailureCount    int64     `json:"failure_count"`
	InputTokens     int64     `json:"input_tokens"`
	OutputTokens    int64     `json:"output_tokens"`
	ReasoningTokens int64     `json:"reasoning_tokens"`
	CachedTokens    int64     `json:"cached_tokens"`
	TotalTokens     int64     `json:"total_tokens"`
}

// HourlyStatsRecord represents hourly aggregated statistics.
type HourlyStatsRecord struct {
	ID            int64     `json:"id"`
	APIKeyHash    string    `json:"api_key_hash"`
	StatDate      time.Time `json:"stat_date"`
	StatHour      int       `json:"stat_hour"` // 0-23
	RequestCount  int64     `json:"request_count"`
	TokenCount    int64     `json:"token_count"`
	SuccessCount  int64     `json:"success_count"`
	FailureCount  int64     `json:"failure_count"`
}

// GlobalDailyRecord represents global daily statistics (not per API key).
type GlobalDailyRecord struct {
	StatDate      time.Time `json:"stat_date"`
	RequestCount  int64     `json:"request_count"`
	TokenCount    int64     `json:"token_count"`
	SuccessCount  int64     `json:"success_count"`
	FailureCount  int64     `json:"failure_count"`
}

// GlobalHourlyRecord represents global hourly statistics (not per API key).
type GlobalHourlyRecord struct {
	StatDate      time.Time `json:"stat_date"`
	StatHour      int       `json:"stat_hour"` // 0-23
	RequestCount  int64     `json:"request_count"`
	TokenCount    int64     `json:"token_count"`
	SuccessCount  int64     `json:"success_count"`
	FailureCount  int64     `json:"failure_count"`
}

// UsageDBStore defines the interface for usage statistics persistence.
type UsageDBStore interface {
	// SaveDetailsBatch saves a batch of request details to the database.
	SaveDetailsBatch(ctx context.Context, details []*RequestDetailRecord) error

	// SaveHourlyStats saves or updates hourly statistics.
	SaveHourlyStats(ctx context.Context, stats *HourlyStatsRecord) error

	// SaveGlobalHourly saves or updates global hourly statistics.
	SaveGlobalHourly(ctx context.Context, stats *GlobalHourlyRecord) error

	// AggregateToDaily aggregates detailed records older than the cutoff to daily summaries.
	AggregateToDaily(ctx context.Context, cutoffDate time.Time) error

	// LoadDetails loads request details since the given time.
	LoadDetails(ctx context.Context, since time.Time) ([]*RequestDetailRecord, error)

	// LoadDailySummary loads daily summaries since the given time.
	LoadDailySummary(ctx context.Context, since time.Time) ([]*DailySummaryRecord, error)

	// LoadHourlyStats loads hourly statistics since the given time.
	LoadHourlyStats(ctx context.Context, since time.Time) ([]*HourlyStatsRecord, error)

	// LoadGlobalDaily loads global daily statistics since the given time.
	LoadGlobalDaily(ctx context.Context, since time.Time) ([]*GlobalDailyRecord, error)

	// LoadGlobalHourly loads global hourly statistics since the given time.
	LoadGlobalHourly(ctx context.Context, since time.Time) ([]*GlobalHourlyRecord, error)

	// PurgeOldDetails deletes request details older than the given time.
	PurgeOldDetails(ctx context.Context, before time.Time) error

	// PurgeOldHourlyStats deletes hourly statistics older than the given time.
	PurgeOldHourlyStats(ctx context.Context, before time.Time) error

	// Close closes the database connection.
	Close() error
}
