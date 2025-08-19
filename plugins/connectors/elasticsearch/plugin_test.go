/* Copyright © INFINI LTD. All rights reserved.
 * Web: https://infinilabs.com
 * Email: hello#infini.ltd */

package elasticsearch

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"infini.sh/coco/modules/common"
	"infini.sh/coco/plugins/connectors"
	"infini.sh/framework/core/elastic"
	"infini.sh/framework/core/kv"
	"infini.sh/framework/core/module"
	"infini.sh/framework/core/queue"
	"infini.sh/framework/core/util"
)

func bytesToBase64String(key []byte) string {
	return base64.StdEncoding.EncodeToString(key)
}

type cache map[string][]byte

type mockKVStore struct {
	store map[string]cache
}

func (s *mockKVStore) Open() error {
	s.store = make(map[string]cache)
	return nil
}

func (s *mockKVStore) Close() error {
	return nil
}

func (s *mockKVStore) GetValue(bucket string, key []byte) ([]byte, error) {
	target := s.store[bucket]
	return target[bytesToBase64String(key)], nil
}

func (s *mockKVStore) GetCompressedValue(bucket string, key []byte) ([]byte, error) {
	return s.GetValue(bucket, key)
}

func (s *mockKVStore) AddValueCompress(bucket string, key []byte, value []byte) error {
	return s.AddValue(bucket, key, value)
}

func (s *mockKVStore) AddValue(bucket string, key []byte, value []byte) error {
	target := s.store[bucket]
	if target == nil {
		target = cache{}
		s.store[bucket] = target
	}
	target[bytesToBase64String(key)] = value
	return nil
}

func (s *mockKVStore) ExistsKey(bucket string, key []byte) (bool, error) {
	target := s.store[bucket]
	if target == nil {
		return false, nil
	}
	return target[bytesToBase64String(key)] != nil, nil
}

func (s *mockKVStore) DeleteKey(bucket string, key []byte) error {
	delete(s.store[bucket], bytesToBase64String(key))
	return nil
}

type mockQueue map[string][][]byte

func (q mockQueue) Name() string {
	return "indexing_documents"
}

func (q mockQueue) Init(s string) error {
	q[s] = [][]byte{}
	return nil
}

func (q mockQueue) Close(s string) error {
	q[s] = nil
	return nil
}

func (q mockQueue) GetStorageSize(k string) uint64 {
	return uint64(len(q[k]))
}

func (q mockQueue) Destroy(s string) error {
	clear(q[s])
	return nil
}

func (q mockQueue) GetQueues() []string {
	var ret []string
	for name := range q {
		ret = append(ret, name)
	}
	return ret
}

func (q mockQueue) Push(s string, bytes []byte) error {
	q[s] = append(q[s], bytes)
	return nil
}

const mockSearchResponse = `{
  "took": 5,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 2,
      "relation": "eq"
    },
    "max_score": 1.0,
    "hits": [
      {
        "_index": "test-index",
        "_type": "_doc",
        "_id": "1",
        "_score": 1.0,
        "_source": {
          "title": "Test Document 1",
          "content": "This is test content for document 1",
          "url": "https://example.com/doc1",
          "@timestamp": "2024-01-01T00:00:00Z",
          "_seq_no": 1,
          "_primary_term": 1
        }
      },
      {
        "_index": "test-index",
        "_type": "_doc",
        "_id": "2",
        "_score": 1.0,
        "_source": {
          "title": "Test Document 2",
          "content": "This is test content for document 2",
          "url": "https://example.com/doc2",
          "@timestamp": "2024-01-02T00:00:00Z",
          "_seq_no": 2,
          "_primary_term": 1
        }
      }
    ]
  },
  "_scroll_id": "test-scroll-id-123"
}`

const mockClusterHealthResponse = `{
  "cluster_name": "test-cluster",
  "status": "green",
  "timed_out": false,
  "number_of_nodes": 3,
  "number_of_data_nodes": 3,
  "active_primary_shards": 5,
  "active_shards": 10,
  "relocating_shards": 0,
  "initializing_shards": 0,
  "unassigned_shards": 0
}`

const mockCatNodesResponse = `[
  {
    "ip": "192.168.1.1",
    "heap.percent": "50",
    "ram.percent": "60",
    "cpu": "10",
    "load_1m": "1.5",
    "load_5m": "1.2",
    "load_15m": "1.0",
    "node.role": "dilmrt",
    "master": "*",
    "name": "node-1",
    "id": "node-1-id"
  },
  {
    "ip": "192.168.1.2",
    "heap.percent": "45",
    "ram.percent": "55",
    "cpu": "8",
    "load_1m": "1.2",
    "load_5m": "1.0",
    "load_15m": "0.8",
    "node.role": "dilmrt",
    "master": "-",
    "name": "node-2",
    "id": "node-2-id"
  }
]`

const mockIndicesResponse = `{
  "test-index": {
    "aliases": {},
    "mappings": {
      "properties": {
        "title": {"type": "text"},
        "content": {"type": "text"},
        "url": {"type": "keyword"},
        "@timestamp": {"type": "date"}
      }
    },
    "settings": {
      "index": {
        "number_of_shards": "1",
        "number_of_replicas": "1"
      }
    }
  }
}`

func createMockESServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		
		switch {
		case r.URL.Path == "/_cluster/health":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(mockClusterHealthResponse))
		case r.URL.Path == "/_cat/nodes":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(mockCatNodesResponse))
		case strings.Contains(r.URL.Path, "/_search"):
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(mockSearchResponse))
		case strings.Contains(r.URL.Path, "/_search/scroll"):
			emptyResponse := `{"hits":{"hits":[]},"_scroll_id":"test-scroll-id-123"}`
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(emptyResponse))
		case r.URL.Path == "/test-index":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(mockIndicesResponse))
		case r.URL.Path == "/":
			versionResponse := `{"version":{"number":"8.0.0"}}`
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(versionResponse))
		default:
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"error":"not found"}`))
		}
	}))
}

func TestConfigParsing(t *testing.T) {
	tests := []struct {
		name     string
		config   map[string]interface{}
		expected Config
	}{
		{
			name: "basic config",
			config: map[string]interface{}{
				"endpoints": []interface{}{"http://localhost:9200"},
				"username":  "elastic",
				"password":  "password",
				"indices":   []interface{}{"test-index"},
			},
			expected: Config{
				Endpoints: []string{"http://localhost:9200"},
				Username:  "elastic",
				Password:  "password",
				Indices:   []string{"test-index"},
			},
		},
		{
			name: "enhanced config with deployment mode",
			config: map[string]interface{}{
				"endpoints":       []interface{}{"http://localhost:9200"},
				"deployment_mode": "cluster",
				"read_strategy": map[string]interface{}{
					"slice_count": 4,
					"preference":  "_primary",
				},
				"health_check": map[string]interface{}{
					"enabled":  true,
					"interval": "30s",
				},
			},
			expected: Config{
				Endpoints:      []string{"http://localhost:9200"},
				DeploymentMode: "cluster",
				ReadStrategy: ReadStrategyConfig{
					SliceCount: 4,
					Preference: "_primary",
				},
				HealthCheck: HealthCheckConfig{
					Enabled:  true,
					Interval: 30 * time.Second,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connector := &common.Connector{ID: "elasticsearch"}
			datasource := &common.DataSource{
				ID:   "test-ds",
				Name: "Test DataSource",
				Connector: common.ConnectorConfig{
					ConnectorID: "elasticsearch",
					Config:      tt.config,
				},
			}

		var cfg Config
		err := connectors.ParseConnectorConfigure(connector, datasource, &cfg)
			assert.NoError(t, err)
			
			assert.Equal(t, tt.expected.Endpoints, cfg.Endpoints)
			assert.Equal(t, tt.expected.Username, cfg.Username)
			assert.Equal(t, tt.expected.Password, cfg.Password)
			assert.Equal(t, tt.expected.DeploymentMode, cfg.DeploymentMode)
			
			if tt.expected.ReadStrategy.SliceCount > 0 {
				assert.Equal(t, tt.expected.ReadStrategy.SliceCount, cfg.ReadStrategy.SliceCount)
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg := Config{}
	
	assert.Equal(t, DefaultScrollSize, cfg.GetScrollSize())
	assert.Equal(t, DefaultScrollTimeout, cfg.GetScrollTime())
	assert.Equal(t, DefaultBatchSize, cfg.GetBatchSize())
	assert.Equal(t, DefaultConcurrency, cfg.GetConcurrency())
	assert.Equal(t, []string{"*"}, cfg.GetIndices())
	
	assert.Equal(t, DefaultSliceCount, cfg.ReadStrategy.GetSliceCount())
	assert.Equal(t, DefaultPreference, cfg.ReadStrategy.GetPreference())
	
	assert.Equal(t, DefaultMaxRetries, cfg.RetryConfig.GetMaxRetries())
	assert.Equal(t, DefaultInitialDelay, cfg.RetryConfig.GetInitialDelay())
	assert.Equal(t, DefaultMaxDelay, cfg.RetryConfig.GetMaxDelay())
	assert.Equal(t, DefaultBackoffFactor, cfg.RetryConfig.GetBackoffFactor())
}

func TestDeploymentModeTypes(t *testing.T) {
	assert.Equal(t, "single", string(DeploymentModeSingle))
	assert.Equal(t, "cluster", string(DeploymentModeCluster))
	assert.Equal(t, "ha_cluster", string(DeploymentModeHACluster))
}

func TestStatusConstants(t *testing.T) {
	assert.Equal(t, "idle", StatusIdle)
	assert.Equal(t, "running", StatusRunning)
	assert.Equal(t, "error", StatusError)
	assert.Equal(t, "recovering", StatusRecovering)
	assert.Equal(t, "stopped", StatusStopped)
}

func TestPluginScanSuccess(t *testing.T) {
	theQueue := mockQueue{}
	queue.RegisterDefaultHandler(theQueue)

	kv.Register("indexing_documents", &mockKVStore{store: make(map[string]cache)})

	server := createMockESServer()
	defer server.Close()

	testQueueName := "indexing_documents"
	plugin := &Plugin{}
	plugin.Queue = &queue.QueueConfig{Name: testQueueName}
	module.RegisterUserPlugin(plugin)
	plugin.Queue = queue.SmartGetOrInitConfig(plugin.Queue)
	plugin.Setup()

	connector := &common.Connector{ID: "elasticsearch"}
	dataSource := &common.DataSource{
		ID:   "test-datasource-id",
		Name: "Test ES Source",
		Connector: common.ConnectorConfig{
			ConnectorID: "elasticsearch",
			Config: map[string]interface{}{
				"endpoints":       []interface{}{server.URL},
				"indices":         []interface{}{"test-index"},
				"deployment_mode": "single",
				"scroll_size":     100,
				"batch_size":      50,
			},
		},
	}

	// Defensive testing
	didPanic := false
	var panicValue interface{}
	func() {
		defer func() {
			if r := recover(); r != nil {
				didPanic = true
				panicValue = r
			}
		}()
		plugin.Scan(connector, dataSource)
	}()

	assert.False(t, didPanic, fmt.Sprintf("Scan panicked with: %v", panicValue))

	queueID := plugin.Queue.ID
	queueSize := len(theQueue[queueID])
	assert.Greater(t, queueSize, 0, "Expected documents to be pushed to the queue")

	if queueSize > 0 {
		data := theQueue[queueID][0]
		var doc common.Document
		err := json.Unmarshal(data, &doc)
		assert.NoError(t, err)
		
		assert.Equal(t, "Test Document 1", doc.Title)
		assert.Equal(t, "This is test content for document 1", doc.Content)
		assert.Equal(t, "https://example.com/doc1", doc.URL)
		assert.Equal(t, "elasticsearch", doc.Type)
	}
}

func TestPluginScanWithClusterMode(t *testing.T) {
	theQueue := mockQueue{}
	queue.RegisterDefaultHandler(theQueue)

	kv.Register("indexing_documents", &mockKVStore{store: make(map[string]cache)})

	server := createMockESServer()
	defer server.Close()

	testQueueName := "indexing_documents"
	plugin := &Plugin{}
	plugin.Queue = &queue.QueueConfig{Name: testQueueName}
	module.RegisterUserPlugin(plugin)
	plugin.Queue = queue.SmartGetOrInitConfig(plugin.Queue)
	plugin.Setup()

	connector := &common.Connector{ID: "elasticsearch"}
	dataSource := &common.DataSource{
		ID:   "test-datasource-id",
		Name: "Test ES Cluster Source",
		Connector: common.ConnectorConfig{
			ConnectorID: "elasticsearch",
			Config: map[string]interface{}{
				"endpoints":       []interface{}{server.URL},
				"indices":         []interface{}{"test-index"},
				"deployment_mode": "cluster",
				"read_strategy": map[string]interface{}{
					"slice_count": 2,
					"preference":  "_primary",
				},
				"health_check": map[string]interface{}{
					"enabled":  true,
					"interval": "10s",
				},
			},
		},
	}

	// Defensive testing
	didPanic := false
	var panicValue interface{}
	func() {
		defer func() {
			if r := recover(); r != nil {
				didPanic = true
				panicValue = r
			}
		}()
		plugin.Scan(connector, dataSource)
	}()

	assert.False(t, didPanic, fmt.Sprintf("Scan with cluster mode panicked with: %v", panicValue))

	queueID := plugin.Queue.ID
	queueSize := len(theQueue[queueID])
	assert.Greater(t, queueSize, 0, "Expected documents to be pushed to the queue in cluster mode")
}

func TestPluginScanMissingEndpoints(t *testing.T) {
	theQueue := mockQueue{}
	queue.RegisterDefaultHandler(theQueue)

	testQueueName := "indexing_documents"
	plugin := &Plugin{}
	plugin.Queue = &queue.QueueConfig{Name: testQueueName}
	module.RegisterUserPlugin(plugin)
	plugin.Queue = queue.SmartGetOrInitConfig(plugin.Queue)
	plugin.Setup()

	connector := &common.Connector{ID: "elasticsearch"}
	dataSource := &common.DataSource{
		ID:   "test-datasource-id",
		Name: "Test ES Source",
		Connector: common.ConnectorConfig{
			ConnectorID: "elasticsearch",
			Config: map[string]interface{}{
				"indices": []interface{}{"test-index"},
			},
		},
	}

	didPanic := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				didPanic = true
			}
		}()
		plugin.Scan(connector, dataSource)
	}()

	assert.False(t, didPanic, "Scan should handle missing endpoints gracefully")
	
	assert.Equal(t, StatusError, plugin.syncState.Status)
}

func TestRetryMechanism(t *testing.T) {
	plugin := &Plugin{}
	plugin.Setup()
	plugin.ctx = context.Background()

	mockClient := &ESClient{}
	
	retryConfig := RetryConfig{
		MaxRetries:    2,
		InitialDelay:  10 * time.Millisecond,
		MaxDelay:      100 * time.Millisecond,
		BackoffFactor: 2.0,
	}

	start := time.Now()
	err := plugin.testConnectionWithRetry(mockClient, retryConfig)
	duration := time.Since(start)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection failed after")
	
	assert.Greater(t, duration, 10*time.Millisecond)
}

func TestSyncStateManagement(t *testing.T) {
	plugin := &Plugin{}
	plugin.Setup()

	assert.Equal(t, StatusIdle, plugin.syncState.Status)
	assert.Equal(t, 0, plugin.syncState.ErrorCount)

	plugin.updateSyncState(StatusRunning, "Starting scan")
	assert.Equal(t, StatusRunning, plugin.syncState.Status)

	plugin.updateSyncState(StatusError, "Connection failed")
	assert.Equal(t, StatusError, plugin.syncState.Status)
	assert.Equal(t, 1, plugin.syncState.ErrorCount)

	plugin.updateSyncState(StatusError, "Another error")
	assert.Equal(t, 2, plugin.syncState.ErrorCount)
}

func TestMetricsUpdate(t *testing.T) {
	plugin := &Plugin{}
	plugin.Setup()

	totalDocs := int64(1000)
	duration := 10 * time.Second

	plugin.updateMetrics(totalDocs, duration)

	assert.Equal(t, totalDocs, plugin.syncState.ProcessedDocuments)
	assert.Equal(t, float64(100), plugin.metrics.DocumentsPerSecond) // 1000 docs / 10 seconds
	assert.Equal(t, duration, plugin.metrics.AverageLatency)
	assert.True(t, time.Since(plugin.metrics.LastUpdated) < time.Second)
}

func TestDocumentConversion(t *testing.T) {
	plugin := &Plugin{}
	plugin.Setup()

	esDoc := elastic.IndexDocument{
		Index: "test-index",
		Type:  "_doc",
		Id:    "test-id",
		Source: map[string]interface{}{
			"title":      "Test Title",
			"content":    "Test Content",
			"url":        "https://example.com",
			"@timestamp": "2024-01-01T00:00:00Z",
			"author":     "Test Author",
		},
	}

	datasource := &common.DataSource{
		ID:   "test-ds",
		Name: "Test DataSource",
	}

	doc := plugin.convertToCocoDocument(esDoc, datasource, "test-index")

	assert.Equal(t, "Test Title", doc.Title)
	assert.Equal(t, "Test Content", doc.Content)
	assert.Equal(t, "https://example.com", doc.URL)
	assert.Equal(t, "elasticsearch", doc.Type)
	assert.Equal(t, "Test Author", doc.Owner.UserName)
	assert.Equal(t, "test-ds", doc.DataSourceID)
	assert.Equal(t, "test-index", doc.Category)
}
