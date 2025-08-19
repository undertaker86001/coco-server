package elasticsearch

import "time"

type Config struct {
	Endpoints    []string `config:"endpoints"`    // Support multi-endpoint cluster mode
	Username     string   `config:"username"`
	Password     string   `config:"password"`
	CredentialID string   `config:"credential_id"`
	
	DeploymentMode string `config:"deployment_mode"` // single, cluster, ha_cluster
	
	Indices      []string `config:"indices"`      // List of indices to scan
	Query        string   `config:"query"`        // Custom query DSL
	ScrollSize   int      `config:"scroll_size"`  // Scroll query size
	ScrollTime   string   `config:"scroll_time"`  // Scroll timeout
	
	BatchSize    int      `config:"batch_size"`   // Batch processing size
	Concurrency  int      `config:"concurrency"`  // Concurrency level
	
	TimestampField string `config:"timestamp_field"` // Timestamp field for incremental sync
	LastSyncTime   string `config:"last_sync_time"`  // Last sync time
	
	IncludeFields []string `config:"include_fields"` // Fields to include in documents
	ExcludeFields []string `config:"exclude_fields"` // Fields to exclude from documents
	Timeout       string   `config:"timeout"`        // Request timeout
	
	ReadStrategy  ReadStrategyConfig  `config:"read_strategy"`
	SyncPolicy    SyncPolicyConfig    `config:"sync_policy"`
	HealthCheck   HealthCheckConfig   `config:"health_check"`
	RetryConfig   RetryConfig         `config:"retry_config"`
}

type ReadStrategyConfig struct {
	ScrollSize    int    `config:"scroll_size"`
	ScrollTimeout string `config:"scroll_timeout"`
	SliceCount    int    `config:"slice_count"`
	Preference    string `config:"preference"`
}

type SyncPolicyConfig struct {
	FullSyncInterval        string   `config:"full_sync_interval"`
	IncrementalSyncInterval string   `config:"incremental_sync_interval"`
	Fields                  []string `config:"fields"`
	QueryFilter             string   `config:"query_filter"`
}

type HealthCheckConfig struct {
	Enabled           bool          `config:"enabled"`
	Interval          time.Duration `config:"interval"`
	FailureThreshold  int           `config:"failure_threshold"`
	RecoveryThreshold int           `config:"recovery_threshold"`
}

type RetryConfig struct {
	MaxRetries    int           `config:"max_retries"`
	InitialDelay  time.Duration `config:"initial_delay"`
	MaxDelay      time.Duration `config:"max_delay"`
	BackoffFactor float64       `config:"backoff_factor"`
}

func (c *Config) GetScrollSize() int {
	if c.ReadStrategy.ScrollSize > 0 {
		return c.ReadStrategy.ScrollSize
	}
	if c.ScrollSize <= 0 {
		return DefaultScrollSize
	}
	return c.ScrollSize
}

func (c *Config) GetScrollTime() string {
	if c.ReadStrategy.ScrollTimeout != "" {
		return c.ReadStrategy.ScrollTimeout
	}
	if c.ScrollTime == "" {
		return DefaultScrollTimeout
	}
	return c.ScrollTime
}

func (c *Config) GetBatchSize() int {
	if c.BatchSize <= 0 {
		return DefaultBatchSize
	}
	return c.BatchSize
}

func (c *Config) GetConcurrency() int {
	if c.Concurrency <= 0 {
		return DefaultConcurrency
	}
	return c.Concurrency
}

func (c *Config) GetTimeout() string {
	if c.Timeout == "" {
		return "30s"
	}
	return c.Timeout
}

func (c *Config) GetIndices() []string {
	if len(c.Indices) == 0 {
		return []string{"*"}
	}
	return c.Indices
}

func (c *ReadStrategyConfig) GetSliceCount() int {
	if c.SliceCount <= 0 {
		return DefaultSliceCount
	}
	return c.SliceCount
}

func (c *ReadStrategyConfig) GetPreference() string {
	if c.Preference == "" {
		return DefaultPreference
	}
	return c.Preference
}

func (c *RetryConfig) GetMaxRetries() int {
	if c.MaxRetries <= 0 {
		return DefaultMaxRetries
	}
	return c.MaxRetries
}

func (c *RetryConfig) GetInitialDelay() time.Duration {
	if c.InitialDelay <= 0 {
		return DefaultInitialDelay
	}
	return c.InitialDelay
}

func (c *RetryConfig) GetMaxDelay() time.Duration {
	if c.MaxDelay <= 0 {
		return DefaultMaxDelay
	}
	return c.MaxDelay
}

func (c *RetryConfig) GetBackoffFactor() float64 {
	if c.BackoffFactor <= 0 {
		return DefaultBackoffFactor
	}
	return c.BackoffFactor
}
