package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

var knowTables = make(map[string]struct{})

type Consumer struct {
	client       *kgo.Client
	useNamespace bool
	quit         chan struct{}
}

type HandlerFn func(changes []Change, sources map[string]any) error

func New(opts []kgo.Opt, useNamespace bool) (*Consumer, error) {
	if err := kgo.ValidateOpts(opts...); err != nil {
		return nil, fmt.Errorf("invalid kafka options: %w", err)
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Consumer{
		client: client,
		quit:   make(chan struct{}),
	}, nil
}

func (c *Consumer) AddPartitions(partitions map[string]map[int32]kgo.Offset) {
	c.client.AddConsumePartitions(partitions)
}

func (c *Consumer) AddTopics(topics ...string) {
	c.client.AddConsumeTopics(topics...)
}

func (c *Consumer) PurgeTopicsFromClient(topics ...string) {
	c.client.PurgeTopicsFromClient(topics...)
}

func (c *Consumer) SetOffsets(offsets map[string]map[int32]kgo.EpochOffset) {
	c.client.SetOffsets(offsets)
}

func (c *Consumer) Start(logger *slog.Logger, handler HandlerFn) {
	for {
		select {
		case <-c.quit:
			return
		default:
			fetches := c.client.PollFetches(context.Background())
			if fetches.IsClientClosed() {
				return
			}
			fetches.EachError(func(t string, p int32, err error) {
				logger.Error("fetch error", "topic", t, "partition", p, "error", err)
			})
			var rs []*kgo.Record
			fetches.EachRecord(func(r *kgo.Record) {
				if len(r.Value) == 0 {
					rs = append(rs, r)
					return
				}
				var envelope debeziumEnvelope
				if err := json.Unmarshal(r.Value, &envelope); err != nil {
					rs = append(rs, r)
					return
				}

				schema, table, ok := schemaTableFromTopic(r.Topic)
				if !ok {
					rs = append(rs, r)
					return
				}

				var changes []Change
				fullTableName := table
				if c.useNamespace {
					fullTableName = fmt.Sprintf("%s.%s", schema, table)
				}
				if _, ok := knowTables[fullTableName]; !ok {
					var key debeziumKey
					json.Unmarshal(r.Key, &key)
					var pkColumns []string
					for _, col := range key.Schema.Fields {
						pkColumns = append(pkColumns, col.Field)
					}
					changes = append(changes, envelope.createTableChange(fullTableName, pkColumns))
					knowTables[fullTableName] = struct{}{}
				}
				change := envelope.change()
				change.Schema = schema
				change.Table = table
				changes = append(changes, change)
				err := handler(changes, envelope.Payload.Source)
				if err != nil {
					return
				}
				rs = append(rs, r)
			})
			if len(rs) == 0 {
				continue
			}
			if err := c.client.CommitRecords(context.Background(), rs...); err != nil {
				logger.Error("commit records", "error", err)
			}
		}
	}
}

func (c *Consumer) Stop() {
	close(c.quit)
}

func schemaTableFromTopic(topic string) (schema, table string, ok bool) {
	parts := strings.Split(topic, ".")
	if len(parts) == 1 {
		return "main", parts[0], ok
	}
	if len(parts) > 1 {
		schema = parts[len(parts)-2]
		table = parts[len(parts)-1]
		ok = true
	}
	return
}
