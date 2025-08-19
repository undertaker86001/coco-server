package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	log "github.com/cihub/seelog"
	"infini.sh/framework/core/elastic"
	"infini.sh/framework/core/model"
	"infini.sh/framework/core/util"
	"infini.sh/framework/modules/elastic/common"
)

type ESClient struct {
	client elastic.API
	config Config
}

type ScrollIterator struct {
	client   elastic.API
	scrollID string
	config   Config
	index    string
	finished bool
}

func NewESClient(cfg Config) (*ESClient, error) {
	esConfig := elastic.ElasticsearchConfig{
		Name:    "coco-connector-es",
		Enabled: true,
	}
	
	if len(cfg.Endpoints) > 0 {
		esConfig.Endpoint = cfg.Endpoints
	} else {
		return nil, fmt.Errorf("no endpoints specified")
	}
	
	if cfg.Username != "" && cfg.Password != "" {
		esConfig.BasicAuth = &model.BasicAuth{
			Username: cfg.Username,
			Password: cfg.Password,
		}
	}
	
	if cfg.CredentialID != "" {
		esConfig.CredentialID = cfg.CredentialID
	}
	
	if cfg.GetTimeout() != "" {
		esConfig.ClientTimeout = cfg.GetTimeout()
	}
	
	client, err := common.InitClientWithConfig(esConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize ES client: %w", err)
	}
	
	return &ESClient{client: client, config: cfg}, nil
}

func (c *ESClient) NewScrollIterator(ctx context.Context, index string) (*ScrollIterator, error) {
	query := c.buildQuery()
	
	searchRequest := &elastic.SearchRequest{
		Size: c.config.GetScrollSize(),
		Query: query,
	}
	
	if len(c.config.IncludeFields) > 0 || len(c.config.ExcludeFields) > 0 {
		source := make(map[string]interface{})
		if len(c.config.IncludeFields) > 0 {
			source["includes"] = c.config.IncludeFields
		}
		if len(c.config.ExcludeFields) > 0 {
			source["excludes"] = c.config.ExcludeFields
		}
		searchRequest.Source = source
	}
	
	scrollID, err := c.client.NewScroll(index, c.config.GetScrollTime(), c.config.GetScrollSize(), searchRequest, 0, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to create scroll: %w", err)
	}
	
	return &ScrollIterator{
		client:   c.client,
		scrollID: string(scrollID),
		config:   c.config,
		index:    index,
		finished: false,
	}, nil
}

func (c *ESClient) buildQuery() *elastic.Query {
	if c.config.Query != "" {
		var queryMap map[string]interface{}
		if err := json.Unmarshal([]byte(c.config.Query), &queryMap); err == nil {
			return &elastic.Query{
				RawQuery: queryMap,
			}
		}
		log.Warnf("Failed to parse custom query DSL, using match_all: %v", err)
	}
	
	if c.config.TimestampField != "" && c.config.LastSyncTime != "" {
		return &elastic.Query{
			BoolQuery: &elastic.BoolQuery{
				Must: []interface{}{
					map[string]interface{}{
						"range": map[string]interface{}{
							c.config.TimestampField: map[string]interface{}{
								"gte": c.config.LastSyncTime,
							},
						},
					},
				},
			},
		}
	}
	
	return &elastic.Query{
		MatchAllQuery: &elastic.MatchAllQuery{},
	}
}

func (iter *ScrollIterator) Next(ctx context.Context) ([]elastic.IndexDocument, error) {
	if iter.finished {
		return nil, nil
	}
	
	apiCtx := &elastic.APIContext{
		Context: ctx,
	}
	
	response, err := iter.client.NextScroll(apiCtx, iter.config.GetScrollTime(), iter.scrollID)
	if err != nil {
		return nil, fmt.Errorf("failed to get next scroll: %w", err)
	}
	
	var searchResponse elastic.SearchResponse
	if err := json.Unmarshal(response, &searchResponse); err != nil {
		return nil, fmt.Errorf("failed to parse scroll response: %w", err)
	}
	
	if searchResponse.ScrollId != "" {
		iter.scrollID = searchResponse.ScrollId
	}
	
	if len(searchResponse.Hits.Hits) == 0 {
		iter.finished = true
		iter.Close()
		return nil, nil
	}
	
	docs := make([]elastic.IndexDocument, len(searchResponse.Hits.Hits))
	for i, hit := range searchResponse.Hits.Hits {
		docs[i] = elastic.IndexDocument{
			Index:  hit.Index,
			Type:   hit.Type,
			Id:     hit.ID,
			Source: hit.Source,
		}
	}
	
	return docs, nil
}

func (iter *ScrollIterator) Close() error {
	if iter.scrollID != "" {
		err := iter.client.ClearScroll(iter.scrollID)
		iter.scrollID = ""
		return err
	}
	return nil
}

func (c *ESClient) TestConnection(ctx context.Context) error {
	health, err := c.client.ClusterHealth(ctx)
	if err != nil {
		return fmt.Errorf("failed to get cluster health: %w", err)
	}
	
	log.Infof("ES cluster health: %s, nodes: %d", health.Status, health.NumberOfNodes)
	return nil
}

func (c *ESClient) GetIndicesInfo(ctx context.Context) (map[string]interface{}, error) {
	indices := strings.Join(c.config.GetIndices(), ",")
	return c.client.GetIndex(indices)
}
