// Package config provides configuration structures for usage storage persistence.
package config

import "time"

// UsageStorageConfig defines configuration for persisting usage statistics to a database.
type UsageStorageConfig struct {
	// Enabled toggles usage statistics persistence.
	Enabled bool `yaml:"enabled" json:"enabled"`

	// MySQL defines MySQL database connection settings.
	MySQL MySQLConfig `yaml:"mysql" json:"mysql"`

	// Sync controls the synchronization behavior.
	Sync SyncConfig `yaml:"sync" json:"sync"`

	// Retention controls data retention policies.
	Retention RetentionConfig `yaml:"retention" json:"retention"`

	// Recovery controls startup data recovery behavior.
	Recovery RecoveryConfig `yaml:"recovery" json:"recovery"`
}

// MySQLConfig defines MySQL database connection settings.
type MySQLConfig struct {
	// Host is the MySQL server hostname or IP address.
	Host string `yaml:"host" json:"host"`

	// Port is the MySQL server port.
	Port int `yaml:"port" json:"port"`

	// Database is the database name to use.
	Database string `yaml:"database" json:"database"`

	// Username is the database user.
	Username string `yaml:"username" json:"username"`

	// Password is the database password.
	Password string `yaml:"password" json:"password"`

	// MaxOpenConns sets the maximum number of open connections.
	MaxOpenConns int `yaml:"max-open-conns" json:"max-open-conns"`

	// MaxIdleConns sets the maximum number of idle connections.
	MaxIdleConns int `yaml:"max-idle-conns" json:"max-idle-conns"`

	// ConnMaxLifetime sets the maximum lifetime of a connection.
	ConnMaxLifetime time.Duration `yaml:"conn-max-lifetime" json:"conn-max-lifetime"`
}

// SyncConfig controls synchronization behavior.
type SyncConfig struct {
	// Interval is the duration between flush operations.
	Interval time.Duration `yaml:"interval" json:"interval"`

	// BatchSize is the maximum number of records to write in a single batch.
	BatchSize int `yaml:"batch-size" json:"batch-size"`

	// BufferSize is the size of the in-memory buffer channel.
	BufferSize int `yaml:"buffer-size" json:"buffer-size"`
}

// RetentionConfig controls data retention policies.
type RetentionConfig struct {
	// DetailDays is the number of days to keep detailed request records.
	DetailDays int `yaml:"detail-days" json:"detail-days"`

	// SummaryDays is the number of days to keep aggregated summary data.
	SummaryDays int `yaml:"summary-days" json:"summary-days"`

	// HourlyDays is the number of days to keep hourly statistics.
	HourlyDays int `yaml:"hourly-days" json:"hourly-days"`
}

// RecoveryConfig controls startup data recovery behavior.
type RecoveryConfig struct {
	// LoadDetailDays is the number of days of detailed records to load into memory on startup.
	LoadDetailDays int `yaml:"load-detail-days" json:"load-detail-days"`

	// LoadSummaryDays is the number of days of summary data to load on startup.
	LoadSummaryDays int `yaml:"load-summary-days" json:"load-summary-days"`
}

// DefaultUsageStorageConfig returns a UsageStorageConfig with sensible defaults.
func DefaultUsageStorageConfig() *UsageStorageConfig {
	return &UsageStorageConfig{
		Enabled: false,
		MySQL: MySQLConfig{
			Port:            3306,
			MaxOpenConns:    10,
			MaxIdleConns:    5,
			ConnMaxLifetime: 30 * time.Minute,
		},
		Sync: SyncConfig{
			Interval:   10 * time.Second,
			BatchSize:  200,
			BufferSize: 1000,
		},
		Retention: RetentionConfig{
			DetailDays:  7,
			SummaryDays: 365,
			HourlyDays:  30,
		},
		Recovery: RecoveryConfig{
			LoadDetailDays:  7,
			LoadSummaryDays: 365,
		},
	}
}

// DSN returns the MySQL data source name for connection.
func (c *MySQLConfig) DSN() string {
	return c.Username + ":" + c.Password + "@tcp(" + c.Host + ":" + itoa(c.Port) + ")/" + c.Database + "?parseTime=true&charset=utf8mb4&loc=Local"
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	n := i
	if n < 0 {
		n = -n
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if i < 0 {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
