package extension

import (
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/walterwanderley/sqlite"

	"github.com/litesql/debezium-sink/consumer"
)

type ConsumerVirtualTable struct {
	virtualTableName string
	tableName        string
	client           *consumer.Consumer
	subscriptions    []*subscription
	stmtMu           sync.Mutex
	mu               sync.Mutex
	logger           *slog.Logger
	loggerCloser     io.Closer
}

type subscription struct {
	topic   string
	offsets map[int32]kgo.EpochOffset
}

func NewConsumerVirtualTable(virtualTableName string, opts []kgo.Opt, useNamespace bool, conn *sqlite.Conn, loggerDef string) (*ConsumerVirtualTable, error) {

	vtab := ConsumerVirtualTable{
		virtualTableName: virtualTableName,
	}

	client, err := consumer.New(opts, useNamespace)
	if err != nil {
		return nil, fmt.Errorf("creating new consumer: %w", err)
	}
	vtab.client = client

	logger, loggerCloser, err := loggerFromConfig(loggerDef)
	if err != nil {
		return nil, err
	}
	vtab.loggerCloser = loggerCloser
	vtab.logger = logger

	go client.Start(logger, vtab.handle(conn, useNamespace))

	return &vtab, nil
}

func (vt *ConsumerVirtualTable) handle(conn *sqlite.Conn, useNamespace bool) consumer.HandlerFn {
	return func(changeset []consumer.Change) error {
		vt.stmtMu.Lock()
		defer vt.stmtMu.Unlock()

		err := conn.Exec("BEGIN IMMEDIATE", nil)
		if err != nil {
			return err
		}
		defer conn.Exec("ROLLBACK", nil)

		for _, change := range changeset {
			vt.logger.Debug("applying changeset", "change", change)
			tableName := change.Table
			if useNamespace {
				if change.Schema == "db" {
					change.Schema = "main"
				}
				tableName = fmt.Sprintf("%s.%s", change.Schema, change.Table)
			}
			var sql string
			switch change.Kind {
			case "INSERT":
				sql = fmt.Sprintf("REPLACE INTO `%s` (%s) VALUES (%s)", tableName, strings.Join(change.ColumnNames, ", "), placeholders(len(change.ColumnValues)))
				err = conn.Exec(sql, nil, change.ColumnValues...)
			case "UPDATE":
				setClause := make([]string, len(change.ColumnNames))
				for i, col := range change.ColumnNames {
					setClause[i] = fmt.Sprintf("%s = ?", col)
				}
				if len(change.OldKeys.KeyNames) == 0 {
					return fmt.Errorf("missing old keys for update on table %s.%s", change.Schema, change.Table)
				}
				var args []any
				args = append(args, change.ColumnValues...)
				whereClause := make([]string, len(change.OldKeys.KeyNames))
				for i, col := range change.OldKeys.KeyNames {
					if change.OldKeys.KeyValues[i] == nil {
						whereClause[i] = fmt.Sprintf("%s IS NULL", col)
					} else {
						args = append(args, change.OldKeys.KeyValues[i])
						whereClause[i] = fmt.Sprintf("%s = ?", col)
					}
				}
				sql = fmt.Sprintf("UPDATE `%s` SET %s WHERE %s", tableName, strings.Join(setClause, ", "), strings.Join(whereClause, " AND "))
				err = conn.Exec(sql, nil, args...)
			case "DELETE":
				whereClause := make([]string, len(change.ColumnNames))
				var args []any
				for i, col := range change.ColumnNames {
					if change.ColumnValues[i] == nil {
						whereClause[i] = fmt.Sprintf("%s IS NULL", col)
					} else {
						args = append(args, change.ColumnValues[i])
						whereClause[i] = fmt.Sprintf("%s = ?", col)
					}
				}
				sql = fmt.Sprintf("DELETE FROM `%s` WHERE %s", tableName, strings.Join(whereClause, " AND "))
				err = conn.Exec(sql, nil, args...)
			case "SQL":
				err = conn.Exec(change.SQL, nil)
			default:
				continue
			}
			if err != nil {
				vt.logger.Error("failed to exec statement", "sql", sql, "error", err)
				return fmt.Errorf("failed to exec %q: %w", sql, err)
			}
		}
		return conn.Exec("COMMIT", nil)
	}
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	var b strings.Builder
	for i := range n {
		b.WriteString(fmt.Sprintf("?%d,", i+1))
	}
	return strings.TrimRight(b.String(), ",")
}

func (vt *ConsumerVirtualTable) BestIndex(in *sqlite.IndexInfoInput) (*sqlite.IndexInfoOutput, error) {
	return &sqlite.IndexInfoOutput{EstimatedCost: 1000000}, nil
}

func (vt *ConsumerVirtualTable) Open() (sqlite.VirtualCursor, error) {
	return newSubscriptionsCursor(vt.subscriptions), nil
}

func (vt *ConsumerVirtualTable) Disconnect() error {
	var err error
	if vt.loggerCloser != nil {
		err = vt.loggerCloser.Close()
	}
	if vt.client != nil {
		vt.client.Stop()
	}

	return err
}

func (vt *ConsumerVirtualTable) Destroy() error {
	return nil
}

func (vt *ConsumerVirtualTable) Insert(values ...sqlite.Value) (int64, error) {
	topic := values[0].Text()
	if topic == "" {
		return 0, fmt.Errorf("topic is required")
	}
	offsets := make(map[int32]kgo.Offset)
	newOffsets := values[1].Text()
	if newOffsets != "" {
		var tmp map[int32]string
		err := json.Unmarshal([]byte(newOffsets), &tmp)
		if err != nil {
			return 0, fmt.Errorf("unmarshal offsets: %w", err)
		}
		for partition, offset := range tmp {
			switch offset {
			case "earliest":
				offsets[partition] = kgo.NewOffset().AtStart()
			case "latest":
				offsets[partition] = kgo.NewOffset().AtEnd()
			default:
				i, err := strconv.ParseInt(offset, 10, 64)
				if err != nil {
					return 0, fmt.Errorf("invalid offset for partition %d! use 'earliest', 'latest' or a number", partition)
				}
				offsets[partition] = kgo.NewOffset().At(i)
			}
		}
	}
	vt.mu.Lock()
	defer vt.mu.Unlock()
	if vt.contains(topic) {
		return 0, fmt.Errorf("already subscribed to the %q topic", topic)
	}
	if len(offsets) > 0 {
		vt.client.AddPartitions(map[string]map[int32]kgo.Offset{
			topic: offsets,
		})
	} else {
		vt.client.AddTopics(topic)
	}
	vt.subscriptions = append(vt.subscriptions, &subscription{topic: topic})
	return 1, nil
}

func (vt *ConsumerVirtualTable) Update(id sqlite.Value, values ...sqlite.Value) error {
	topic := values[0].Text()
	if topic == "" {
		return fmt.Errorf("topic is required")
	}
	index := id.Int()
	// slices are 0 based
	index--
	if !(index >= 0 && index < len(vt.subscriptions)) {
		// nothing to update
		return nil
	}
	if vt.subscriptions[index].topic != topic {
		return fmt.Errorf("updates are restricted to offsets")
	}
	newOffsets := values[1].Text()
	if newOffsets != "" {
		var offsets map[int32]kgo.EpochOffset
		err := json.Unmarshal([]byte(newOffsets), &offsets)
		if err != nil {
			return fmt.Errorf("unmarshal offsets: %w", err)
		}
		if len(offsets) > 0 {
			vt.mu.Lock()
			defer vt.mu.Unlock()
			vt.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{
				topic: offsets,
			})
		}
	}
	return nil
}

func (vt *ConsumerVirtualTable) Replace(old sqlite.Value, new sqlite.Value, _ ...sqlite.Value) error {
	return fmt.Errorf("REPLACE operations on %q are not supported", vt.virtualTableName)
}

func (vt *ConsumerVirtualTable) Delete(v sqlite.Value) error {
	vt.mu.Lock()
	defer vt.mu.Unlock()
	index := v.Int()
	// slices are 0 based
	index--
	if index >= 0 && index < len(vt.subscriptions) {
		vt.client.PurgeTopicsFromClient(vt.subscriptions[index].topic)
		vt.subscriptions = slices.Delete(vt.subscriptions, index, index+1)
	}
	return nil
}

func (vt *ConsumerVirtualTable) contains(topic string) bool {
	for _, subscription := range vt.subscriptions {
		if subscription.topic == topic {
			return true
		}
	}
	return false
}

type subscriptionsCursor struct {
	data    []*subscription
	current subscription // current row that the cursor points to
	rowid   int64        // current rowid .. negative for EOF
}

func newSubscriptionsCursor(data []*subscription) *subscriptionsCursor {
	slices.SortFunc(data, func(a, b *subscription) int {
		return cmp.Compare(a.topic, b.topic)
	})
	return &subscriptionsCursor{
		data: data,
	}
}

func (c *subscriptionsCursor) Next() error {
	// EOF
	if c.rowid < 0 || int(c.rowid) >= len(c.data) {
		c.rowid = -1
		return sqlite.SQLITE_OK
	}
	// slices are zero based
	c.current = *c.data[c.rowid]
	c.rowid += 1

	return sqlite.SQLITE_OK
}

func (c *subscriptionsCursor) Column(ctx *sqlite.VirtualTableContext, i int) error {
	switch i {
	case 0:
		ctx.ResultText(c.current.topic)
	case 1:
		b, err := json.Marshal(c.current.offsets)
		if err != nil {
			return fmt.Errorf("marshal offsets: %w", err)
		}
		ctx.ResultText(string(b))
		ctx.ResultSubType(74)
	}

	return nil
}

func (c *subscriptionsCursor) Filter(int, string, ...sqlite.Value) error {
	c.rowid = 0
	return c.Next()
}

func (c *subscriptionsCursor) Rowid() (int64, error) {
	return c.rowid, nil
}

func (c *subscriptionsCursor) Eof() bool {
	return c.rowid < 0
}

func (c *subscriptionsCursor) Close() error {
	return nil
}
