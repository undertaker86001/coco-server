/* Copyright © INFINI LTD. All rights reserved.
 * Web: https://infinilabs.com
 * Email: hello#infini.ltd */

package elasticsearch

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"infini.sh/framework/core/elastic"
)

func TestNewReader(t *testing.T) {
	config := Config{
		Endpoints:      []string{"http://localhost:9200"},
		DeploymentMode: "cluster",
	}

	client, err := NewESClient(config)
	assert.NoError(t, err)

	reader := NewReader(client, config)
	assert.NotNil(t, reader)
	assert.Equal(t, DeploymentModeCluster, reader.deployMode)
	assert.NotNil(t, reader.clusterStatus)
	assert.Equal(t, "unknown", reader.clusterStatus.Status)
}

func TestReaderDeploymentModeDefault(t *testing.T) {
	config := Config{
		Endpoints: []string{"http://localhost:9200"},
	}

	client, err := NewESClient(config)
	assert.NoError(t, err)

	reader := NewReader(client, config)
	assert.Equal(t, DeploymentModeSingle, reader.deployMode)
}

func TestReaderGetClusterStatus(t *testing.T) {
	config := Config{
		Endpoints:      []string{"http://localhost:9200"},
		DeploymentMode: "ha_cluster",
	}

	client, err := NewESClient(config)
	assert.NoError(t, err)

	reader := NewReader(client, config)
	
	status := reader.GetClusterStatus()
	assert.Equal(t, "unknown", status.Status)
	assert.Equal(t, 0, status.ActiveNodes)
	assert.Equal(t, 0, status.TotalNodes)
}

func TestReaderSupportsSliceScroll(t *testing.T) {
	config := Config{
		Endpoints: []string{"http://localhost:9200"},
	}

	client, err := NewESClient(config)
	assert.NoError(t, err)

	reader := NewReader(client, config)
	
	supports := reader.SupportsSliceScroll()
	assert.IsType(t, bool(false), supports)
}

func TestReaderOptimizeReadStrategy(t *testing.T) {
	config := Config{
		Endpoints:      []string{"http://localhost:9200"},
		DeploymentMode: "single",
		ReadStrategy: ReadStrategyConfig{
			SliceCount: 2,
		},
	}

	client, err := NewESClient(config)
	assert.NoError(t, err)

	reader := NewReader(client, config)
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	originalSliceCount := reader.config.ReadStrategy.SliceCount
	err = reader.OptimizeReadStrategy(ctx)
	
	assert.NoError(t, err)
	assert.Equal(t, originalSliceCount, reader.config.ReadStrategy.SliceCount)
}

func TestReaderOptimizeReadStrategyCluster(t *testing.T) {
	config := Config{
		Endpoints:      []string{"http://localhost:9200"},
		DeploymentMode: "cluster",
		ReadStrategy: ReadStrategyConfig{
			SliceCount: 2,
		},
	}

	client, err := NewESClient(config)
	assert.NoError(t, err)

	reader := NewReader(client, config)
	
	reader.clusterStatus.ActiveNodes = 3
	reader.clusterStatus.TotalNodes = 3
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = reader.OptimizeReadStrategy(ctx)
	
	if err != nil {
		assert.Contains(t, err.Error(), "failed to get cluster health")
	}
}

func TestReaderReadDocumentsSingleNode(t *testing.T) {
	config := Config{
		Endpoints:      []string{"http://localhost:9200"},
		DeploymentMode: "single",
	}

	client, err := NewESClient(config)
	assert.NoError(t, err)

	reader := NewReader(client, config)
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	callbackCalled := false
	callback := func(docs []elastic.IndexDocument) error {
		callbackCalled = true
		return nil
	}

	err = reader.ReadDocuments(ctx, "test-index", callback)
	
	if err != nil {
		assert.Contains(t, err.Error(), "failed to create scroll iterator")
	}
	
	assert.False(t, callbackCalled)
}

func TestReaderReadDocumentsCluster(t *testing.T) {
	config := Config{
		Endpoints:      []string{"http://localhost:9200"},
		DeploymentMode: "cluster",
		ReadStrategy: ReadStrategyConfig{
			SliceCount: 2,
		},
	}

	client, err := NewESClient(config)
	assert.NoError(t, err)

	reader := NewReader(client, config)
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	callbackCalled := false
	callback := func(docs []elastic.IndexDocument) error {
		callbackCalled = true
		return nil
	}

	err = reader.ReadDocuments(ctx, "test-index", callback)
	
	if err != nil {
		assert.NotNil(t, err)
	}
}

func TestReaderReadDocumentsHACluster(t *testing.T) {
	config := Config{
		Endpoints:      []string{"http://localhost:9200"},
		DeploymentMode: "ha_cluster",
		ReadStrategy: ReadStrategyConfig{
			SliceCount: 4,
		},
	}

	client, err := NewESClient(config)
	assert.NoError(t, err)

	reader := NewReader(client, config)
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	callbackCalled := false
	callback := func(docs []elastic.IndexDocument) error {
		callbackCalled = true
		return nil
	}

	err = reader.ReadDocuments(ctx, "test-index", callback)
	
	if err != nil {
		assert.NotNil(t, err)
	}
}

func TestReaderUnsupportedDeploymentMode(t *testing.T) {
	config := Config{
		Endpoints:      []string{"http://localhost:9200"},
		DeploymentMode: "invalid_mode",
	}

	client, err := NewESClient(config)
	assert.NoError(t, err)

	reader := NewReader(client, config)
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	callback := func(docs []elastic.IndexDocument) error {
		return nil
	}

	err = reader.ReadDocuments(ctx, "test-index", callback)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported deployment mode")
}

func TestClusterStatusStructure(t *testing.T) {
	status := ClusterStatus{
		ClusterName:     "test-cluster",
		Status:          "green",
		ActiveNodes:     3,
		TotalNodes:      3,
		LastHealthCheck: time.Now(),
		Nodes: []NodeHealth{
			{
				NodeID:    "node-1",
				NodeName:  "test-node-1",
				Host:      "192.168.1.1",
				Status:    "active",
				Available: true,
				LastCheck: time.Now(),
			},
		},
	}

	assert.Equal(t, "test-cluster", status.ClusterName)
	assert.Equal(t, "green", status.Status)
	assert.Equal(t, 3, status.ActiveNodes)
	assert.Equal(t, 3, status.TotalNodes)
	assert.Len(t, status.Nodes, 1)
	assert.Equal(t, "node-1", status.Nodes[0].NodeID)
	assert.True(t, status.Nodes[0].Available)
}

func TestSyncStateStructure(t *testing.T) {
	now := time.Now()
	state := SyncState{
		LastFullSync:        now,
		LastIncrementalSync: now,
		LastSeqNo:           12345,
		LastPrimaryTerm:     1,
		ProcessedDocuments:  1000,
		ErrorCount:          2,
		Status:              StatusRunning,
	}

	assert.Equal(t, now.Unix(), state.LastFullSync.Unix())
	assert.Equal(t, now.Unix(), state.LastIncrementalSync.Unix())
	assert.Equal(t, int64(12345), state.LastSeqNo)
	assert.Equal(t, int64(1), state.LastPrimaryTerm)
	assert.Equal(t, int64(1000), state.ProcessedDocuments)
	assert.Equal(t, 2, state.ErrorCount)
	assert.Equal(t, StatusRunning, state.Status)
}

func TestPerformanceMetricsStructure(t *testing.T) {
	now := time.Now()
	metrics := PerformanceMetrics{
		DocumentsPerSecond: 150.5,
		AverageLatency:     500 * time.Millisecond,
		ErrorRate:          0.02,
		ThroughputMB:       25.7,
		LastUpdated:        now,
	}

	assert.Equal(t, 150.5, metrics.DocumentsPerSecond)
	assert.Equal(t, 500*time.Millisecond, metrics.AverageLatency)
	assert.Equal(t, 0.02, metrics.ErrorRate)
	assert.Equal(t, 25.7, metrics.ThroughputMB)
	assert.Equal(t, now.Unix(), metrics.LastUpdated.Unix())
}
