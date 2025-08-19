/* Copyright © INFINI LTD. All rights reserved.
 * Web: https://infinilabs.com
 * Email: hello#infini.ltd */

//go:build integration

package elasticsearch

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"infini.sh/coco/modules/common"
	"infini.sh/coco/plugins/connectors"
	"infini.sh/framework/core/kv"
	"infini.sh/framework/core/module"
	"infini.sh/framework/core/queue"
)

func TestFullWorkflowIntegration(t *testing.T) {
	theQueue := mockQueue{}
	queue.RegisterDefaultHandler(theQueue)
	kv.Register("indexing_documents", &mockKVStore{store: make(map[string]cache)})

	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.Header().Set("Content-Type", "application/json")
		
		switch {
		case r.URL.Path == "/":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"version":{"number":"8.0.0","build_flavor":"default"}}`))
			
		case r.URL.Path == "/_cluster/health":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"cluster_name": "test-cluster",
				"status": "green",
				"timed_out": false,
				"number_of_nodes": 3,
				"number_of_data_nodes": 3,
				"active_primary_shards": 5,
				"active_shards": 10
			}`))
			
		case r.URL.Path == "/_cat/nodes":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`[
				{
					"ip": "192.168.1.1",
					"name": "node-1",
					"id": "node-1-id",
					"node.role": "dilmrt",
					"master": "*"
				},
				{
					"ip": "192.168.1.2", 
					"name": "node-2",
					"id": "node-2-id",
					"node.role": "dilmrt",
					"master": "-"
				}
			]`))
			
		case r.URL.Path == "/test-index/_search":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"took": 5,
				"timed_out": false,
				"_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
				"hits": {
					"total": {"value": 3, "relation": "eq"},
					"max_score": 1.0,
					"hits": [
						{
							"_index": "test-index",
							"_type": "_doc", 
							"_id": "1",
							"_score": 1.0,
							"_source": {
								"title": "Integration Test Document 1",
								"content": "This is content for integration test document 1",
								"url": "https://example.com/doc1",
								"@timestamp": "2024-01-01T00:00:00Z",
								"author": "Test Author 1",
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
								"title": "Integration Test Document 2",
								"content": "This is content for integration test document 2",
								"url": "https://example.com/doc2",
								"@timestamp": "2024-01-02T00:00:00Z",
								"author": "Test Author 2",
								"_seq_no": 2,
								"_primary_term": 1
							}
						}
					]
				},
				"_scroll_id": "test-scroll-id-123"
			}`))
			
		case r.URL.Path == "/_search/scroll":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"took": 2,
				"timed_out": false,
				"_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
				"hits": {
					"total": {"value": 0, "relation": "eq"},
					"max_score": null,
					"hits": []
				},
				"_scroll_id": "test-scroll-id-123"
			}`))
			
		case r.URL.Path == "/test-index":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"test-index": {
					"aliases": {},
					"mappings": {
						"properties": {
							"title": {"type": "text"},
							"content": {"type": "text"},
							"url": {"type": "keyword"},
							"@timestamp": {"type": "date"},
							"author": {"type": "keyword"}
						}
					},
					"settings": {
						"index": {
							"number_of_shards": "1",
							"number_of_replicas": "1"
						}
					}
				}
			}`))
			
		default:
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"error":"not found"}`))
		}
	}))
	defer server.Close()

	testQueueName := "indexing_documents"
	plugin := &Plugin{}
	plugin.Queue = &queue.QueueConfig{Name: testQueueName}
	module.RegisterUserPlugin(plugin)
	plugin.Queue = queue.SmartGetOrInitConfig(plugin.Queue)
	plugin.Setup()

	testCases := []struct {
		name           string
		deploymentMode string
		sliceCount     int
		healthCheck    bool
		expectedDocs   int
	}{
		{
			name:           "Single Node Mode",
			deploymentMode: "single",
			sliceCount:     1,
			healthCheck:    false,
			expectedDocs:   2,
		},
		{
			name:           "Cluster Mode with Slice Scroll",
			deploymentMode: "cluster",
			sliceCount:     2,
			healthCheck:    true,
			expectedDocs:   2,
		},
		{
			name:           "HA Cluster Mode",
			deploymentMode: "ha_cluster",
			sliceCount:     4,
			healthCheck:    true,
			expectedDocs:   2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			theQueue[testQueueName] = [][]byte{}
			
			connector := &common.Connector{ID: "elasticsearch"}
			dataSource := &common.DataSource{
				ID:   "test-datasource-" + tc.deploymentMode,
				Name: "Test ES Source - " + tc.name,
				Connector: common.ConnectorConfig{
					ConnectorID: "elasticsearch",
					Config: map[string]interface{}{
						"endpoints":       []interface{}{server.URL},
						"indices":         []interface{}{"test-index"},
						"deployment_mode": tc.deploymentMode,
						"scroll_size":     100,
						"batch_size":      50,
						"read_strategy": map[string]interface{}{
							"slice_count": tc.sliceCount,
							"preference":  "_primary",
						},
						"health_check": map[string]interface{}{
							"enabled":  tc.healthCheck,
							"interval": "5s",
						},
						"retry_config": map[string]interface{}{
							"max_retries":    2,
							"initial_delay":  "100ms",
							"max_delay":      "1s",
							"backoff_factor": 2.0,
						},
					},
				},
			}

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

			assert.False(t, didPanic, "Scan should not panic for %s: %v", tc.name, panicValue)

			assert.NotEqual(t, StatusError, plugin.syncState.Status, "Sync should not be in error state for %s", tc.name)

			queueID := plugin.Queue.ID
			queueSize := len(theQueue[queueID])
			
			if queueSize > 0 {
				data := theQueue[queueID][0]
				var doc common.Document
				err := json.Unmarshal(data, &doc)
				assert.NoError(t, err, "Document should unmarshal correctly for %s", tc.name)
				
				assert.Equal(t, "Integration Test Document 1", doc.Title)
				assert.Equal(t, "This is content for integration test document 1", doc.Content)
				assert.Equal(t, "https://example.com/doc1", doc.URL)
				assert.Equal(t, "elasticsearch", doc.Type)
				assert.Equal(t, "Test Author 1", doc.Owner.UserName)
				assert.Equal(t, dataSource.ID, doc.DataSourceID)
				assert.Equal(t, "test-index", doc.Category)
			}

			assert.Greater(t, plugin.metrics.LastUpdated.Unix(), int64(0), "Metrics should be updated for %s", tc.name)
			
			if plugin.syncState.Status != StatusError {
				assert.GreaterOrEqual(t, plugin.syncState.ProcessedDocuments, int64(0), "Processed documents should be tracked for %s", tc.name)
			}
		})
	}
}

func TestErrorHandlingIntegration(t *testing.T) {
	theQueue := mockQueue{}
	queue.RegisterDefaultHandler(theQueue)
	kv.Register("indexing_documents", &mockKVStore{store: make(map[string]cache)})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"error":"service unavailable"}`))
		default:
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error":"internal server error"}`))
		}
	}))
	defer server.Close()

	testQueueName := "indexing_documents"
	plugin := &Plugin{}
	plugin.Queue = &queue.QueueConfig{Name: testQueueName}
	module.RegisterUserPlugin(plugin)
	plugin.Queue = queue.SmartGetOrInitConfig(plugin.Queue)
	plugin.Setup()

	connector := &common.Connector{ID: "elasticsearch"}
	dataSource := &common.DataSource{
		ID:   "test-error-datasource",
		Name: "Test Error ES Source",
		Connector: common.ConnectorConfig{
			ConnectorID: "elasticsearch",
			Config: map[string]interface{}{
				"endpoints": []interface{}{server.URL},
				"indices":   []interface{}{"test-index"},
				"retry_config": map[string]interface{}{
					"max_retries":    1,
					"initial_delay":  "10ms",
					"max_delay":      "100ms",
					"backoff_factor": 2.0,
				},
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

	assert.False(t, didPanic, "Scan should handle errors gracefully without panicking")
	assert.Equal(t, StatusError, plugin.syncState.Status, "Sync state should be error after connection failure")
	assert.Greater(t, plugin.syncState.ErrorCount, 0, "Error count should be incremented")
}

func TestHealthMonitoringIntegration(t *testing.T) {
	theQueue := mockQueue{}
	queue.RegisterDefaultHandler(theQueue)
	kv.Register("indexing_documents", &mockKVStore{store: make(map[string]cache)})

	healthCheckCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		
		switch r.URL.Path {
		case "/":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"version":{"number":"8.0.0"}}`))
			
		case "/_cluster/health":
			healthCheckCount++
			if healthCheckCount <= 2 {
				w.WriteHeader(http.StatusServiceUnavailable)
				w.Write([]byte(`{"error":"cluster unavailable"}`))
			} else {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{
					"cluster_name": "test-cluster",
					"status": "green",
					"number_of_nodes": 2
				}`))
			}
			
		case "/_cat/nodes":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`[{"ip": "192.168.1.1", "name": "node-1", "id": "node-1-id"}]`))
			
		case "/test-index/_search":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"hits": {"hits": []},
				"_scroll_id": "test-scroll-id"
			}`))
			
		case "/_search/scroll":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"hits": {"hits": []}}`))
			
		default:
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{}`))
		}
	}))
	defer server.Close()

	testQueueName := "indexing_documents"
	plugin := &Plugin{}
	plugin.Queue = &queue.QueueConfig{Name: testQueueName}
	module.RegisterUserPlugin(plugin)
	plugin.Queue = queue.SmartGetOrInitConfig(plugin.Queue)
	plugin.Setup()

	connector := &common.Connector{ID: "elasticsearch"}
	dataSource := &common.DataSource{
		ID:   "test-health-datasource",
		Name: "Test Health Monitoring ES Source",
		Connector: common.ConnectorConfig{
			ConnectorID: "elasticsearch",
			Config: map[string]interface{}{
				"endpoints":       []interface{}{server.URL},
				"indices":         []interface{}{"test-index"},
				"deployment_mode": "ha_cluster",
				"health_check": map[string]interface{}{
					"enabled":           true,
					"interval":          "100ms",
					"failure_threshold": 2,
				},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	plugin.ctx = ctx
	plugin.Scan(connector, dataSource)

	time.Sleep(500 * time.Millisecond)

	assert.Greater(t, healthCheckCount, 0, "Health checks should have been performed")
}

func TestConfigurationValidationIntegration(t *testing.T) {
	theQueue := mockQueue{}
	queue.RegisterDefaultHandler(theQueue)

	testQueueName := "indexing_documents"
	plugin := &Plugin{}
	plugin.Queue = &queue.QueueConfig{Name: testQueueName}
	module.RegisterUserPlugin(plugin)
	plugin.Queue = queue.SmartGetOrInitConfig(plugin.Queue)
	plugin.Setup()

	invalidConfigs := []struct {
		name   string
		config map[string]interface{}
	}{
		{
			name:   "Missing endpoints",
			config: map[string]interface{}{
				"indices": []interface{}{"test-index"},
			},
		},
		{
			name: "Invalid deployment mode",
			config: map[string]interface{}{
				"endpoints":       []interface{}{"http://localhost:9200"},
				"deployment_mode": "invalid_mode",
			},
		},
	}

	for _, tc := range invalidConfigs {
		t.Run(tc.name, func(t *testing.T) {
			connector := &common.Connector{ID: "elasticsearch"}
			dataSource := &common.DataSource{
				ID:   "test-invalid-config",
				Name: "Test Invalid Config",
				Connector: common.ConnectorConfig{
					ConnectorID: "elasticsearch",
					Config:      tc.config,
				},
			}

			plugin.syncState.Status = StatusIdle
			plugin.syncState.ErrorCount = 0

			didPanic := false
			func() {
				defer func() {
					if r := recover(); r != nil {
						didPanic = true
					}
				}()
				plugin.Scan(connector, dataSource)
			}()

			assert.False(t, didPanic, "Invalid config should not cause panic for %s", tc.name)
			
			if tc.name == "Missing endpoints" {
				assert.Equal(t, StatusError, plugin.syncState.Status, "Missing endpoints should result in error state")
			}
		})
	}
}
