package elasticsearch

import (
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/cihub/seelog"
	"infini.sh/framework/core/elastic"
	"infini.sh/framework/core/util"
)

type Reader struct {
	client       *ESClient
	config       Config
	deployMode   DeploymentMode
	clusterStatus *ClusterStatus
	mu           sync.RWMutex
}

func NewReader(client *ESClient, config Config) *Reader {
	deployMode := DeploymentMode(config.DeploymentMode)
	if deployMode == "" {
		deployMode = DeploymentModeSingle
	}

	return &Reader{
		client:     client,
		config:     config,
		deployMode: deployMode,
		clusterStatus: &ClusterStatus{
			Status: "unknown",
		},
	}
}

func (r *Reader) ReadDocuments(ctx context.Context, index string, callback func([]elastic.IndexDocument) error) error {
	switch r.deployMode {
	case DeploymentModeSingle:
		return r.readSingleNode(ctx, index, callback)
	case DeploymentModeCluster:
		return r.readCluster(ctx, index, callback)
	case DeploymentModeHACluster:
		return r.readHACluster(ctx, index, callback)
	default:
		return fmt.Errorf("unsupported deployment mode: %s", r.deployMode)
	}
}

func (r *Reader) readSingleNode(ctx context.Context, index string, callback func([]elastic.IndexDocument) error) error {
	log.Infof("Reading from single node ES: %s", index)
	
	iterator, err := r.client.NewScrollIterator(ctx, index)
	if err != nil {
		return fmt.Errorf("failed to create scroll iterator: %w", err)
	}
	defer iterator.Close()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		docs, err := iterator.Next(ctx)
		if err != nil {
			return fmt.Errorf("failed to get next batch: %w", err)
		}

		if len(docs) == 0 {
			break
		}

		if err := callback(docs); err != nil {
			return fmt.Errorf("callback failed: %w", err)
		}
	}

	return nil
}

func (r *Reader) readCluster(ctx context.Context, index string, callback func([]elastic.IndexDocument) error) error {
	log.Infof("Reading from ES cluster with slice scroll: %s", index)
	
	sliceCount := r.config.ReadStrategy.GetSliceCount()
	if sliceCount <= 1 {
		return r.readSingleNode(ctx, index, callback)
	}

	var wg sync.WaitGroup
	errChan := make(chan error, sliceCount)
	
	for sliceID := 0; sliceID < sliceCount; sliceID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if err := r.readSlice(ctx, index, id, sliceCount, callback); err != nil {
				errChan <- fmt.Errorf("slice %d failed: %w", id, err)
			}
		}(sliceID)
	}

	go func() {
		wg.Wait()
		close(errChan)
	}()

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *Reader) readSlice(ctx context.Context, index string, sliceID, maxSlices int, callback func([]elastic.IndexDocument) error) error {
	query := r.client.buildQuery()
	
	if query.RawQuery == nil {
		query.RawQuery = make(map[string]interface{})
	}
	
	query.Set("slice", util.MapStr{
		"id":  sliceID,
		"max": maxSlices,
	})

	searchRequest := &elastic.SearchRequest{
		Size:  r.config.GetScrollSize(),
		Query: query,
	}

	scrollID, err := r.client.client.NewScroll(
		index,
		r.config.GetScrollTime(),
		r.config.GetScrollSize(),
		searchRequest,
		sliceID,
		maxSlices,
	)
	if err != nil {
		return fmt.Errorf("failed to create slice scroll: %w", err)
	}

	defer func() {
		if err := r.client.client.ClearScroll(string(scrollID)); err != nil {
			log.Warnf("Failed to clear scroll: %v", err)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		apiCtx := &elastic.APIContext{Context: ctx}
		response, err := r.client.client.NextScroll(apiCtx, r.config.GetScrollTime(), string(scrollID))
		if err != nil {
			return fmt.Errorf("failed to get next scroll: %w", err)
		}

		var searchResponse elastic.SearchResponse
		if err := util.FromJSONBytes(response, &searchResponse); err != nil {
			return fmt.Errorf("failed to parse scroll response: %w", err)
		}

		if len(searchResponse.Hits.Hits) == 0 {
			break
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

		if err := callback(docs); err != nil {
			return fmt.Errorf("callback failed: %w", err)
		}

		if searchResponse.ScrollId != "" {
			scrollID = []byte(searchResponse.ScrollId)
		}
	}

	return nil
}

func (r *Reader) readHACluster(ctx context.Context, index string, callback func([]elastic.IndexDocument) error) error {
	log.Infof("Reading from HA ES cluster with failover: %s", index)
	
	if err := r.updateClusterHealth(ctx); err != nil {
		log.Warnf("Failed to check cluster health: %v", err)
	}

	return r.readCluster(ctx, index, callback)
}

func (r *Reader) updateClusterHealth(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	health, err := r.client.client.ClusterHealth(ctx)
	if err != nil {
		return fmt.Errorf("failed to get cluster health: %w", err)
	}

	r.clusterStatus.ClusterName = health.Name
	r.clusterStatus.Status = health.Status
	r.clusterStatus.LastHealthCheck = time.Now()

	nodes, err := r.client.client.CatNodes("")
	if err != nil {
		log.Warnf("Failed to get node information: %v", err)
		return nil
	}

	nodeHealths := make([]NodeHealth, len(nodes))
	activeNodes := 0
	
	for i, node := range nodes {
		nodeHealths[i] = NodeHealth{
			NodeID:    node.NodeID,
			NodeName:  node.Name,
			Host:      node.IP,
			Status:    "active",
			LastCheck: time.Now(),
			Available: true,
		}
		activeNodes++
	}

	r.clusterStatus.Nodes = nodeHealths
	r.clusterStatus.ActiveNodes = activeNodes
	r.clusterStatus.TotalNodes = len(nodes)

	log.Infof("Cluster health updated: %s, active nodes: %d/%d", 
		r.clusterStatus.Status, r.clusterStatus.ActiveNodes, r.clusterStatus.TotalNodes)

	return nil
}

func (r *Reader) GetClusterStatus() ClusterStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return *r.clusterStatus
}

func (r *Reader) SupportsSliceScroll() bool {
	version := r.client.client.GetVersion()
	return version.Major >= 5
}

func (r *Reader) OptimizeReadStrategy(ctx context.Context) error {
	if r.deployMode == DeploymentModeSingle {
		return nil
	}

	if err := r.updateClusterHealth(ctx); err != nil {
		return err
	}

	if r.clusterStatus.ActiveNodes > 0 {
		optimalSlices := r.clusterStatus.ActiveNodes * 2
		if optimalSlices > 8 {
			optimalSlices = 8
		}
		if optimalSlices < 2 {
			optimalSlices = 2
		}
		
		if r.config.ReadStrategy.SliceCount != optimalSlices {
			log.Infof("Optimizing slice count from %d to %d based on %d active nodes",
				r.config.ReadStrategy.SliceCount, optimalSlices, r.clusterStatus.ActiveNodes)
			r.config.ReadStrategy.SliceCount = optimalSlices
		}
	}

	return nil
}
