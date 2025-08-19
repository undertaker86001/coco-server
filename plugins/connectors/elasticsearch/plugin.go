package elasticsearch

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/cihub/seelog"
	"infini.sh/coco/modules/common"
	"infini.sh/coco/plugins/connectors"
	"infini.sh/framework/core/elastic"
	"infini.sh/framework/core/global"
	"infini.sh/framework/core/module"
	"infini.sh/framework/core/queue"
	"infini.sh/framework/core/util"
)

const ConnectorElasticsearch = "elasticsearch"

type Plugin struct {
	connectors.BasePlugin
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
}

func (p *Plugin) Name() string {
	return ConnectorElasticsearch
}

func (p *Plugin) Setup() {
	p.BasePlugin.Init("connector.elasticsearch", "indexing elasticsearch documents", p)
}

func (p *Plugin) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ctx, p.cancel = context.WithCancel(context.Background())
	return p.BasePlugin.Start(connectors.DefaultSyncInterval)
}

func (p *Plugin) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cancel != nil {
		p.cancel()
	}
	return nil
}

func (p *Plugin) Scan(connector *common.Connector, datasource *common.DataSource) {
	cfg := Config{}
	err := connectors.ParseConnectorConfigure(connector, datasource, &cfg)
	if err != nil {
		log.Errorf("[%v connector] Parsing connector configuration failed: %v", ConnectorElasticsearch, err)
		panic(err)
	}

	log.Debugf("[%v connector] Handling datasource: %v", ConnectorElasticsearch, datasource.Name)

	if len(cfg.Endpoints) == 0 {
		log.Errorf("[%v connector] Missing required configuration for datasource [%s]: endpoints", ConnectorElasticsearch, datasource.Name)
		return
	}

	client, err := NewESClient(cfg)
	if err != nil {
		log.Errorf("[%v connector] Failed to create ES client for datasource [%s]: %v", ConnectorElasticsearch, datasource.Name, err)
		panic(err)
	}

	if err := client.TestConnection(p.ctx); err != nil {
		log.Errorf("[%v connector] Failed to connect to ES cluster for datasource [%s]: %v", ConnectorElasticsearch, datasource.Name, err)
		panic(err)
	}

	indicesInfo, err := client.GetIndicesInfo(p.ctx)
	if err != nil {
		log.Warnf("[%v connector] Failed to get indices info for datasource [%s]: %v", ConnectorElasticsearch, datasource.Name, err)
	} else {
		log.Infof("[%v connector] Found %d indices for datasource [%s]", ConnectorElasticsearch, len(indicesInfo), datasource.Name)
	}

	indices := cfg.GetIndices()
	if cfg.GetConcurrency() > 1 {
		p.scanIndicesConcurrently(client, indices, datasource, cfg)
	} else {
		p.scanIndicesSequentially(client, indices, datasource, cfg)
	}

	log.Infof("[%v connector] Finished scanning indices %v for datasource [%s]", ConnectorElasticsearch, indices, datasource.Name)
}

func (p *Plugin) scanIndicesSequentially(client *ESClient, indices []string, datasource *common.DataSource, cfg Config) {
	for _, index := range indices {
		if p.ctx.Err() != nil {
			log.Infof("[%v connector] Context cancelled, stopping scan", ConnectorElasticsearch)
			return
		}
		p.scanIndex(client, index, datasource, cfg)
	}
}

func (p *Plugin) scanIndicesConcurrently(client *ESClient, indices []string, datasource *common.DataSource, cfg Config) {
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, cfg.GetConcurrency())

	for _, index := range indices {
		if p.ctx.Err() != nil {
			log.Infof("[%v connector] Context cancelled, stopping scan", ConnectorElasticsearch)
			break
		}

		wg.Add(1)
		go func(idx string) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			p.scanIndex(client, idx, datasource, cfg)
		}(index)
	}

	wg.Wait()
}

func (p *Plugin) scanIndex(client *ESClient, index string, datasource *common.DataSource, cfg Config) {
	log.Infof("[%v connector] Starting scan of index [%s] for datasource [%s]", ConnectorElasticsearch, index, datasource.Name)

	iterator, err := client.NewScrollIterator(p.ctx, index)
	if err != nil {
		log.Errorf("[%v connector] Failed to create scroll iterator for index [%s]: %v", ConnectorElasticsearch, index, err)
		return
	}
	defer iterator.Close()

	docCount := 0
	batchCount := 0
	batchSize := cfg.GetBatchSize()
	batch := make([]common.Document, 0, batchSize)

	for {
		if p.ctx.Err() != nil {
			log.Infof("[%v connector] Context cancelled, stopping index scan", ConnectorElasticsearch)
			break
		}

		docs, err := iterator.Next(p.ctx)
		if err != nil {
			log.Errorf("[%v connector] Error during scroll for index [%s]: %v", ConnectorElasticsearch, index, err)
			break
		}

		if docs == nil || len(docs) == 0 {
			break
		}

		for _, doc := range docs {
			cocoDoc := p.convertToCocoDocument(doc, datasource, index)
			batch = append(batch, cocoDoc)
			docCount++

			if len(batch) >= batchSize {
				p.processBatch(batch, datasource.Name)
				batch = batch[:0]
				batchCount++
			}
		}
	}

	if len(batch) > 0 {
		p.processBatch(batch, datasource.Name)
		batchCount++
	}

	log.Infof("[%v connector] Completed scan of index [%s]: %d documents in %d batches", ConnectorElasticsearch, index, docCount, batchCount)
}

func (p *Plugin) convertToCocoDocument(esDoc elastic.IndexDocument, datasource *common.DataSource, index string) common.Document {
	doc := common.Document{
		Source: common.DataSourceReference{
			ID:   datasource.ID,
			Type: "connector",
			Name: datasource.Name,
		},
		Type:     ConnectorElasticsearch,
		Icon:     "elasticsearch",
		Category: index,
		System:   datasource.System,
		Metadata: make(map[string]interface{}),
	}

	doc.ID = util.MD5digest(fmt.Sprintf("%s-%s-%s", datasource.ID, esDoc.Index, esDoc.Id))

	if esDoc.Source != nil {
		sourceMap, ok := esDoc.Source.(map[string]interface{})
		if ok {
			if title, exists := sourceMap["title"]; exists {
				if titleStr, ok := title.(string); ok {
					doc.Title = titleStr
				}
			} else if name, exists := sourceMap["name"]; exists {
				if nameStr, ok := name.(string); ok {
					doc.Title = nameStr
				}
			} else {
				doc.Title = fmt.Sprintf("Document %s", esDoc.Id)
			}

			if content, exists := sourceMap["content"]; exists {
				if contentStr, ok := content.(string); ok {
					doc.Content = contentStr
				}
			} else if body, exists := sourceMap["body"]; exists {
				if bodyStr, ok := body.(string); ok {
					doc.Content = bodyStr
				}
			} else if message, exists := sourceMap["message"]; exists {
				if messageStr, ok := message.(string); ok {
					doc.Content = messageStr
				}
			}

			if url, exists := sourceMap["url"]; exists {
				if urlStr, ok := url.(string); ok {
					doc.URL = urlStr
				}
			}

			if created, exists := sourceMap["created"]; exists {
				if createdTime := p.parseTime(created); createdTime != nil {
					doc.Created = createdTime
				}
			} else if timestamp, exists := sourceMap["@timestamp"]; exists {
				if timestampTime := p.parseTime(timestamp); timestampTime != nil {
					doc.Created = timestampTime
				}
			}

			if updated, exists := sourceMap["updated"]; exists {
				if updatedTime := p.parseTime(updated); updatedTime != nil {
					doc.Updated = updatedTime
				}
			} else if modified, exists := sourceMap["modified"]; exists {
				if modifiedTime := p.parseTime(modified); modifiedTime != nil {
					doc.Updated = modifiedTime
				}
			}

			if owner, exists := sourceMap["owner"]; exists {
				if ownerMap, ok := owner.(map[string]interface{}); ok {
					userInfo := &common.UserInfo{}
					if id, exists := ownerMap["id"]; exists {
						if idStr, ok := id.(string); ok {
							userInfo.UserID = idStr
						}
					}
					if name, exists := ownerMap["name"]; exists {
						if nameStr, ok := name.(string); ok {
							userInfo.UserName = nameStr
						}
					}
					if userInfo.UserID != "" || userInfo.UserName != "" {
						doc.Owner = userInfo
					}
				}
			}

			for key, value := range sourceMap {
				if !p.isReservedField(key) {
					doc.Metadata[key] = value
				}
			}
		}
	}

	doc.Metadata["_index"] = esDoc.Index
	doc.Metadata["_type"] = esDoc.Type
	doc.Metadata["_id"] = esDoc.Id

	return doc
}

func (p *Plugin) parseTime(timeValue interface{}) *time.Time {
	switch v := timeValue.(type) {
	case string:
		formats := []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02T15:04:05.000Z",
			"2006-01-02T15:04:05Z",
			"2006-01-02 15:04:05",
			"2006-01-02",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return &t
			}
		}
	case float64:
		t := time.Unix(int64(v), 0)
		return &t
	case int64:
		t := time.Unix(v, 0)
		return &t
	}
	return nil
}

func (p *Plugin) isReservedField(field string) bool {
	reservedFields := []string{
		"title", "name", "content", "body", "message",
		"url", "created", "updated", "modified", "@timestamp",
		"owner",
	}
	for _, reserved := range reservedFields {
		if field == reserved {
			return true
		}
	}
	return false
}

func (p *Plugin) processBatch(batch []common.Document, datasourceName string) {
	for _, doc := range batch {
		data := util.MustToJSONBytes(doc)
		if global.Env().IsDebug {
			log.Tracef("[%v connector] Queuing document: %s", ConnectorElasticsearch, string(data))
		}

		if err := queue.Push(p.Queue, data); err != nil {
			log.Errorf("[%v connector] Failed to push document to queue for datasource [%s]: %v", ConnectorElasticsearch, datasourceName, err)
			panic(err)
		}
	}
}

func init() {
	module.RegisterUserPlugin(&Plugin{})
}
