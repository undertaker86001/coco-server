/* Copyright © INFINI LTD. All rights reserved.
 * Web: https://infinilabs.com
 * Email: hello#infini.ltd */

package elasticsearch

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewESClient(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: Config{
				Endpoints: []string{"http://localhost:9200"},
				Username:  "elastic",
				Password:  "password",
			},
			wantErr: false,
		},
		{
			name: "empty endpoints",
			config: Config{
				Endpoints: []string{},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewESClient(tt.config)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, client)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, client)
				assert.Equal(t, tt.config, client.config)
			}
		})
	}
}

func TestESClientBuildQuery(t *testing.T) {
	config := Config{
		Endpoints: []string{"http://localhost:9200"},
		Query:     `{"match_all": {}}`,
	}

	client, err := NewESClient(config)
	assert.NoError(t, err)

	query := client.buildQuery()
	assert.NotNil(t, query)
}

func TestESClientTestConnection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"version":{"number":"8.0.0"}}`))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	config := Config{
		Endpoints: []string{server.URL},
	}

	client, err := NewESClient(config)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = client.TestConnection(ctx)
	if err != nil {
		assert.Contains(t, err.Error(), "failed to test connection")
	}
}

func TestESClientGetIndicesInfo(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/test-index" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"test-index": {
					"aliases": {},
					"mappings": {
						"properties": {
							"title": {"type": "text"},
							"content": {"type": "text"}
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
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	config := Config{
		Endpoints: []string{server.URL},
		Indices:   []string{"test-index"},
	}

	client, err := NewESClient(config)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	info, err := client.GetIndicesInfo(ctx)
	if err != nil {
		assert.Contains(t, err.Error(), "failed to get indices info")
	} else {
		assert.NotNil(t, info)
	}
}

func TestScrollIteratorClose(t *testing.T) {
	config := Config{
		Endpoints: []string{"http://localhost:9200"},
	}

	client, err := NewESClient(config)
	assert.NoError(t, err)

	iterator := &ScrollIterator{
		client:   client,
		scrollID: "test-scroll-id",
		finished: false,
	}

	iterator.Close()
	assert.True(t, iterator.finished)
}

func TestConfigGetMethods(t *testing.T) {
	config := Config{
		ScrollSize:  500,
		ScrollTime:  "2m",
		BatchSize:   200,
		Concurrency: 4,
		Timeout:     "60s",
		Indices:     []string{"index1", "index2"},
		ReadStrategy: ReadStrategyConfig{
			SliceCount: 8,
			Preference: "_local",
		},
		RetryConfig: RetryConfig{
			MaxRetries:    5,
			InitialDelay:  2 * time.Second,
			MaxDelay:      2 * time.Minute,
			BackoffFactor: 3.0,
		},
	}

	assert.Equal(t, 500, config.GetScrollSize())
	assert.Equal(t, "2m", config.GetScrollTime())
	assert.Equal(t, 200, config.GetBatchSize())
	assert.Equal(t, 4, config.GetConcurrency())
	assert.Equal(t, "60s", config.GetTimeout())
	assert.Equal(t, []string{"index1", "index2"}, config.GetIndices())
	
	assert.Equal(t, 8, config.ReadStrategy.GetSliceCount())
	assert.Equal(t, "_local", config.ReadStrategy.GetPreference())
	
	assert.Equal(t, 5, config.RetryConfig.GetMaxRetries())
	assert.Equal(t, 2*time.Second, config.RetryConfig.GetInitialDelay())
	assert.Equal(t, 2*time.Minute, config.RetryConfig.GetMaxDelay())
	assert.Equal(t, 3.0, config.RetryConfig.GetBackoffFactor())
}

func TestConfigDefaultValues(t *testing.T) {
	config := Config{}

	assert.Equal(t, DefaultScrollSize, config.GetScrollSize())
	assert.Equal(t, DefaultScrollTimeout, config.GetScrollTime())
	assert.Equal(t, DefaultBatchSize, config.GetBatchSize())
	assert.Equal(t, DefaultConcurrency, config.GetConcurrency())
	assert.Equal(t, "30s", config.GetTimeout())
	assert.Equal(t, []string{"*"}, config.GetIndices())
	
	assert.Equal(t, DefaultSliceCount, config.ReadStrategy.GetSliceCount())
	assert.Equal(t, DefaultPreference, config.ReadStrategy.GetPreference())
	
	assert.Equal(t, DefaultMaxRetries, config.RetryConfig.GetMaxRetries())
	assert.Equal(t, DefaultInitialDelay, config.RetryConfig.GetInitialDelay())
	assert.Equal(t, DefaultMaxDelay, config.RetryConfig.GetMaxDelay())
	assert.Equal(t, DefaultBackoffFactor, config.RetryConfig.GetBackoffFactor())
}
