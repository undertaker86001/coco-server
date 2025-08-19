package elasticsearch

type Config struct {
	Endpoints    []string `config:"endpoints"`    // Support multi-endpoint cluster mode
	Username     string   `config:"username"`
	Password     string   `config:"password"`
	CredentialID string   `config:"credential_id"`
	
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
}

func (c *Config) GetScrollSize() int {
	if c.ScrollSize <= 0 {
		return 1000
	}
	return c.ScrollSize
}

func (c *Config) GetScrollTime() string {
	if c.ScrollTime == "" {
		return "5m"
	}
	return c.ScrollTime
}

func (c *Config) GetBatchSize() int {
	if c.BatchSize <= 0 {
		return 100
	}
	return c.BatchSize
}

func (c *Config) GetConcurrency() int {
	if c.Concurrency <= 0 {
		return 1
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
