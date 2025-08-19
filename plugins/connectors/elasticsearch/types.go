package elasticsearch

import (
	"time"
)

type DeploymentMode string

const (
	DeploymentModeSingle    DeploymentMode = "single"
	DeploymentModeCluster   DeploymentMode = "cluster"
	DeploymentModeHACluster DeploymentMode = "ha_cluster"
)

type SyncPolicy struct {
	FullSyncInterval        string `config:"full_sync_interval"`
	IncrementalSyncInterval string `config:"incremental_sync_interval"`
	Fields                  []string `config:"fields"`
	QueryFilter             string `config:"query_filter"`
}

type ReadStrategy struct {
	ScrollSize    int    `config:"scroll_size"`
	ScrollTimeout string `config:"scroll_timeout"`
	SliceCount    int    `config:"slice_count"`
	Preference    string `config:"preference"`
}

type HealthCheckConfig struct {
	Enabled         bool          `config:"enabled"`
	Interval        time.Duration `config:"interval"`
	FailureThreshold int          `config:"failure_threshold"`
	RecoveryThreshold int         `config:"recovery_threshold"`
}

type SyncState struct {
	LastFullSync        time.Time `json:"last_full_sync"`
	LastIncrementalSync time.Time `json:"last_incremental_sync"`
	LastSeqNo           int64     `json:"last_seq_no"`
	LastPrimaryTerm     int64     `json:"last_primary_term"`
	ProcessedDocuments  int64     `json:"processed_documents"`
	ErrorCount          int       `json:"error_count"`
	Status              string    `json:"status"`
}

type NodeHealth struct {
	NodeID    string    `json:"node_id"`
	NodeName  string    `json:"node_name"`
	Host      string    `json:"host"`
	Status    string    `json:"status"`
	LastCheck time.Time `json:"last_check"`
	Available bool      `json:"available"`
}

type ClusterStatus struct {
	ClusterName   string       `json:"cluster_name"`
	Status        string       `json:"status"`
	Nodes         []NodeHealth `json:"nodes"`
	ActiveNodes   int          `json:"active_nodes"`
	TotalNodes    int          `json:"total_nodes"`
	LastHealthCheck time.Time  `json:"last_health_check"`
}

type RetryConfig struct {
	MaxRetries      int           `config:"max_retries"`
	InitialDelay    time.Duration `config:"initial_delay"`
	MaxDelay        time.Duration `config:"max_delay"`
	BackoffFactor   float64       `config:"backoff_factor"`
}

type PerformanceMetrics struct {
	DocumentsPerSecond float64       `json:"documents_per_second"`
	AverageLatency     time.Duration `json:"average_latency"`
	ErrorRate          float64       `json:"error_rate"`
	ThroughputMB       float64       `json:"throughput_mb"`
	LastUpdated        time.Time     `json:"last_updated"`
}

const (
	DefaultScrollSize         = 1000
	DefaultScrollTimeout      = "5m"
	DefaultSliceCount         = 4
	DefaultPreference         = "_primary"
	DefaultBatchSize          = 100
	DefaultConcurrency        = 2
	DefaultMaxRetries         = 3
	DefaultInitialDelay       = time.Second
	DefaultMaxDelay           = time.Minute
	DefaultBackoffFactor      = 2.0
	DefaultHealthCheckInterval = 30 * time.Second
	DefaultFailureThreshold   = 3
	DefaultRecoveryThreshold  = 2
)

const (
	StatusIdle        = "idle"
	StatusRunning     = "running"
	StatusError       = "error"
	StatusRecovering  = "recovering"
	StatusStopped     = "stopped"
)
