// Package store provides MySQL storage implementation for usage statistics.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
)

// UsageMySQLStore implements usage.UsageDBStore using MySQL.
type UsageMySQLStore struct {
	db   *sql.DB
	once sync.Once
}

// NewUsageMySQLStore creates a new MySQL store for usage statistics.
func NewUsageMySQLStore(cfg *config.MySQLConfig) (*UsageMySQLStore, error) {
	dsn := cfg.DSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open mysql connection: %w", err)
	}

	// Apply connection pool settings
	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping mysql: %w", err)
	}

	return &UsageMySQLStore{db: db}, nil
}

// SaveDetailsBatch saves a batch of request details to the database.
func (s *UsageMySQLStore) SaveDetailsBatch(ctx context.Context, details []*usage.RequestDetailRecord) error {
	if len(details) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO usage_request_details 
			(api_key_hash, model_name, source, auth_index, timestamp, failed,
			 input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, d := range details {
		_, err := stmt.ExecContext(ctx,
			d.APIKeyHash,
			d.ModelName,
			d.Source,
			d.AuthIndex,
			d.Timestamp,
			d.Failed,
			d.InputTokens,
			d.OutputTokens,
			d.ReasoningTokens,
			d.CachedTokens,
			d.TotalTokens,
		)
		if err != nil {
			return fmt.Errorf("failed to insert detail record: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// SaveHourlyStats saves or updates hourly statistics.
func (s *UsageMySQLStore) SaveHourlyStats(ctx context.Context, stats *usage.HourlyStatsRecord) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO usage_hourly_stats 
			(api_key_hash, stat_date, stat_hour, request_count, token_count, success_count, failure_count)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			request_count = request_count + VALUES(request_count),
			token_count = token_count + VALUES(token_count),
			success_count = success_count + VALUES(success_count),
			failure_count = failure_count + VALUES(failure_count)
	`,
		stats.APIKeyHash,
		stats.StatDate,
		stats.StatHour,
		stats.RequestCount,
		stats.TokenCount,
		stats.SuccessCount,
		stats.FailureCount,
	)
	if err != nil {
		return fmt.Errorf("failed to save hourly stats: %w", err)
	}
	return nil
}

// SaveGlobalHourly saves or updates global hourly statistics.
func (s *UsageMySQLStore) SaveGlobalHourly(ctx context.Context, stats *usage.GlobalHourlyRecord) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO usage_global_hourly 
			(stat_date, stat_hour, request_count, token_count, success_count, failure_count)
		VALUES (?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			request_count = request_count + VALUES(request_count),
			token_count = token_count + VALUES(token_count),
			success_count = success_count + VALUES(success_count),
			failure_count = failure_count + VALUES(failure_count)
	`,
		stats.StatDate,
		stats.StatHour,
		stats.RequestCount,
		stats.TokenCount,
		stats.SuccessCount,
		stats.FailureCount,
	)
	if err != nil {
		return fmt.Errorf("failed to save global hourly stats: %w", err)
	}
	return nil
}

// AggregateToDaily aggregates detailed records older than the cutoff to daily summaries.
func (s *UsageMySQLStore) AggregateToDaily(ctx context.Context, cutoffDate time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Aggregate to daily summary
	_, err = tx.ExecContext(ctx, `
		INSERT INTO usage_daily_summary 
			(api_key_hash, model_name, source, stat_date, 
			 total_requests, success_count, failure_count,
			 input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens)
		SELECT 
			api_key_hash,
			model_name,
			source,
			DATE(timestamp) as stat_date,
			COUNT(*) as total_requests,
			SUM(CASE WHEN failed = 0 THEN 1 ELSE 0 END) as success_count,
			SUM(CASE WHEN failed = 1 THEN 1 ELSE 0 END) as failure_count,
			SUM(input_tokens),
			SUM(output_tokens),
			SUM(reasoning_tokens),
			SUM(cached_tokens),
			SUM(total_tokens)
		FROM usage_request_details
		WHERE timestamp < ?
		GROUP BY api_key_hash, model_name, source, DATE(timestamp)
		ON DUPLICATE KEY UPDATE
			total_requests = VALUES(total_requests),
			success_count = VALUES(success_count),
			failure_count = VALUES(failure_count),
			input_tokens = VALUES(input_tokens),
			output_tokens = VALUES(output_tokens),
			reasoning_tokens = VALUES(reasoning_tokens),
			cached_tokens = VALUES(cached_tokens),
			total_tokens = VALUES(total_tokens)
	`, cutoffDate)
	if err != nil {
		return fmt.Errorf("failed to aggregate to daily: %w", err)
	}

	// Aggregate to global daily
	_, err = tx.ExecContext(ctx, `
		INSERT INTO usage_global_daily 
			(stat_date, request_count, token_count, success_count, failure_count)
		SELECT 
			DATE(timestamp) as stat_date,
			COUNT(*) as request_count,
			SUM(total_tokens) as token_count,
			SUM(CASE WHEN failed = 0 THEN 1 ELSE 0 END) as success_count,
			SUM(CASE WHEN failed = 1 THEN 1 ELSE 0 END) as failure_count
		FROM usage_request_details
		WHERE timestamp < ?
		GROUP BY DATE(timestamp)
		ON DUPLICATE KEY UPDATE
			request_count = VALUES(request_count),
			token_count = VALUES(token_count),
			success_count = VALUES(success_count),
			failure_count = VALUES(failure_count)
	`, cutoffDate)
	if err != nil {
		return fmt.Errorf("failed to aggregate to global daily: %w", err)
	}

	// Delete aggregated details
	_, err = tx.ExecContext(ctx, `
		DELETE FROM usage_request_details WHERE timestamp < ?
	`, cutoffDate)
	if err != nil {
		return fmt.Errorf("failed to delete aggregated details: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// LoadDetails loads request details since the given time.
func (s *UsageMySQLStore) LoadDetails(ctx context.Context, since time.Time) ([]*usage.RequestDetailRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, api_key_hash, model_name, source, auth_index, timestamp, failed,
		       input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens
		FROM usage_request_details
		WHERE timestamp >= ?
		ORDER BY timestamp ASC
	`, since)
	if err != nil {
		return nil, fmt.Errorf("failed to query details: %w", err)
	}
	defer rows.Close()

	var details []*usage.RequestDetailRecord
	for rows.Next() {
		d := &usage.RequestDetailRecord{}
		var failedInt int
		err := rows.Scan(
			&d.ID,
			&d.APIKeyHash,
			&d.ModelName,
			&d.Source,
			&d.AuthIndex,
			&d.Timestamp,
			&failedInt,
			&d.InputTokens,
			&d.OutputTokens,
			&d.ReasoningTokens,
			&d.CachedTokens,
			&d.TotalTokens,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan detail: %w", err)
		}
		d.Failed = failedInt == 1
		details = append(details, d)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating details: %w", err)
	}

	return details, nil
}

// LoadDailySummary loads daily summaries since the given time.
func (s *UsageMySQLStore) LoadDailySummary(ctx context.Context, since time.Time) ([]*usage.DailySummaryRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, api_key_hash, model_name, source, stat_date,
		       total_requests, success_count, failure_count,
		       input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens
		FROM usage_daily_summary
		WHERE stat_date >= ?
		ORDER BY stat_date ASC
	`, since)
	if err != nil {
		return nil, fmt.Errorf("failed to query daily summary: %w", err)
	}
	defer rows.Close()

	var summaries []*usage.DailySummaryRecord
	for rows.Next() {
		s := &usage.DailySummaryRecord{}
		err := rows.Scan(
			&s.ID,
			&s.APIKeyHash,
			&s.ModelName,
			&s.Source,
			&s.StatDate,
			&s.TotalRequests,
			&s.SuccessCount,
			&s.FailureCount,
			&s.InputTokens,
			&s.OutputTokens,
			&s.ReasoningTokens,
			&s.CachedTokens,
			&s.TotalTokens,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan daily summary: %w", err)
		}
		summaries = append(summaries, s)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating daily summaries: %w", err)
	}

	return summaries, nil
}

// LoadHourlyStats loads hourly statistics since the given time.
func (s *UsageMySQLStore) LoadHourlyStats(ctx context.Context, since time.Time) ([]*usage.HourlyStatsRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, api_key_hash, stat_date, stat_hour, request_count, token_count, success_count, failure_count
		FROM usage_hourly_stats
		WHERE stat_date >= ?
		ORDER BY stat_date, stat_hour ASC
	`, since)
	if err != nil {
		return nil, fmt.Errorf("failed to query hourly stats: %w", err)
	}
	defer rows.Close()

	var stats []*usage.HourlyStatsRecord
	for rows.Next() {
		h := &usage.HourlyStatsRecord{}
		err := rows.Scan(
			&h.ID,
			&h.APIKeyHash,
			&h.StatDate,
			&h.StatHour,
			&h.RequestCount,
			&h.TokenCount,
			&h.SuccessCount,
			&h.FailureCount,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan hourly stats: %w", err)
		}
		stats = append(stats, h)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating hourly stats: %w", err)
	}

	return stats, nil
}

// LoadGlobalDaily loads global daily statistics since the given time.
func (s *UsageMySQLStore) LoadGlobalDaily(ctx context.Context, since time.Time) ([]*usage.GlobalDailyRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT stat_date, request_count, token_count, success_count, failure_count
		FROM usage_global_daily
		WHERE stat_date >= ?
		ORDER BY stat_date ASC
	`, since)
	if err != nil {
		return nil, fmt.Errorf("failed to query global daily: %w", err)
	}
	defer rows.Close()

	var records []*usage.GlobalDailyRecord
	for rows.Next() {
		r := &usage.GlobalDailyRecord{}
		err := rows.Scan(
			&r.StatDate,
			&r.RequestCount,
			&r.TokenCount,
			&r.SuccessCount,
			&r.FailureCount,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan global daily: %w", err)
		}
		records = append(records, r)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating global daily: %w", err)
	}

	return records, nil
}

// LoadGlobalHourly loads global hourly statistics since the given time.
func (s *UsageMySQLStore) LoadGlobalHourly(ctx context.Context, since time.Time) ([]*usage.GlobalHourlyRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT stat_date, stat_hour, request_count, token_count, success_count, failure_count
		FROM usage_global_hourly
		WHERE stat_date >= ?
		ORDER BY stat_date, stat_hour ASC
	`, since)
	if err != nil {
		return nil, fmt.Errorf("failed to query global hourly: %w", err)
	}
	defer rows.Close()

	var records []*usage.GlobalHourlyRecord
	for rows.Next() {
		r := &usage.GlobalHourlyRecord{}
		err := rows.Scan(
			&r.StatDate,
			&r.StatHour,
			&r.RequestCount,
			&r.TokenCount,
			&r.SuccessCount,
			&r.FailureCount,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan global hourly: %w", err)
		}
		records = append(records, r)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating global hourly: %w", err)
	}

	return records, nil
}

// PurgeOldDetails deletes request details older than the given time.
func (s *UsageMySQLStore) PurgeOldDetails(ctx context.Context, before time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM usage_request_details WHERE timestamp < ?
	`, before)
	if err != nil {
		return fmt.Errorf("failed to purge old details: %w", err)
	}
	return nil
}

// PurgeOldHourlyStats deletes hourly statistics older than the given time.
func (s *UsageMySQLStore) PurgeOldHourlyStats(ctx context.Context, before time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM usage_hourly_stats WHERE stat_date < ?
	`, before)
	if err != nil {
		return fmt.Errorf("failed to purge hourly stats: %w", err)
	}

	_, err = s.db.ExecContext(ctx, `
		DELETE FROM usage_global_hourly WHERE stat_date < ?
	`, before)
	if err != nil {
		return fmt.Errorf("failed to purge global hourly: %w", err)
	}

	return nil
}

// Close closes the database connection.
func (s *UsageMySQLStore) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
