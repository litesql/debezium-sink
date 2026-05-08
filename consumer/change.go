package consumer

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"
)

type Change struct {
	ServerTime   time.Time `json:"servertime"`
	Kind         string    `json:"kind"`
	Schema       string    `json:"schema"`
	Table        string    `json:"table"`
	ColumnNames  []string  `json:"columnnames"`
	ColumnValues []any     `json:"columnvalues"`
	OldKeys      struct {
		KeyNames  []string `json:"keynames,omitempty"`
		KeyValues []any    `json:"keyvalues,omitempty"`
	} `json:"oldkeys"`
	SQL string `json:"sql"`
}

type debeziumEnvelope struct {
	Schema  debeziumSchema  `json:"schema"`
	Payload debeziumPayload `json:"payload"`
}

type debeziumSchema struct {
	Type     string          `json:"type"`
	Fields   []debeziumField `json:"fields"`
	Optional bool            `json:"optional"`
	Name     string          `json:"name"`
	Version  int             `json:"version"`
}

type debeziumField struct {
	Type     string          `json:"type"`
	Fields   []debeziumField `json:"fields,omitempty"`
	Optional bool            `json:"optional"`
	Name     string          `json:"name,omitempty"`
	Field    string          `json:"field"`
	Version  int             `json:"version,omitempty"`
}

type debeziumPayload struct {
	Before map[string]any `json:"before"`
	After  map[string]any `json:"after"`
	Source map[string]any `json:"source"`
	Op     string         `json:"op"`
	TsNs   int64          `json:"ts_ns"`
}

type debeziumKey struct {
	Schema struct {
		Type   string `json:"type"`
		Fields []struct {
			Type     string `json:"type"`
			Optional bool   `json:"optional"`
			Field    string `json:"field"`
		} `json:"fields"`
		Optional bool   `json:"optional"`
		Name     string `json:"name"`
	} `json:"schema"`
}

func (e debeziumEnvelope) change() Change {
	var c Change
	c.ServerTime = time.Unix(0, e.Payload.TsNs)
	switch e.Payload.Op {
	case "c":
		c.Kind = "INSERT"
		for k, v := range e.Payload.After {
			c.ColumnNames = append(c.ColumnNames, k)
			c.ColumnValues = append(c.ColumnValues, e.valueAdapter(v, k))
		}
	case "u":
		c.Kind = "UPDATE"
		for k, v := range e.Payload.Before {
			c.OldKeys.KeyNames = append(c.OldKeys.KeyNames, k)
			c.OldKeys.KeyValues = append(c.OldKeys.KeyValues, e.valueAdapter(v, k))
		}
		for k, v := range e.Payload.After {
			c.ColumnNames = append(c.ColumnNames, k)
			c.ColumnValues = append(c.ColumnValues, e.valueAdapter(v, k))
		}
	case "d":
		c.Kind = "DELETE"
		for k, v := range e.Payload.Before {
			c.ColumnNames = append(c.ColumnNames, k)
			c.ColumnValues = append(c.ColumnValues, e.valueAdapter(v, k))
		}
	case "t":
		c.Kind = "TRUNCATE"
	}
	return c
}

func (e debeziumEnvelope) createTableChange(fullTableName string, pkColumns []string) Change {
	var buf strings.Builder
	fmt.Fprintf(&buf, "CREATE TABLE IF NOT EXISTS `%s` (\n", fullTableName)
	for i, field := range e.Schema.Fields[0].Fields {
		if i > 0 {
			buf.WriteString(",\n")
		}
		fmt.Fprintf(&buf, "\t%s ", field.Field)
		switch {
		case strings.HasPrefix(field.Type, "int"):
			buf.WriteString("INTEGER")
		case strings.HasPrefix(field.Type, "float"):
			buf.WriteString("REAL")
		case strings.HasPrefix(field.Type, "bytes"):
			buf.WriteString("BLOB")
		default:
			buf.WriteString("TEXT")
		}
	}
	if len(pkColumns) > 0 {
		fmt.Fprintf(&buf, ",\n\tPRIMARY KEY(%s)", strings.Join(pkColumns, ", "))
	}
	buf.WriteString("\n)")
	return Change{
		Kind: "SQL",
		SQL:  buf.String(),
	}
}

// https://github.com/debezium/debezium/tree/main/debezium-connector-common/src/main/java/io/debezium
func (e debeziumEnvelope) valueAdapter(x any, fieldName string) any {
	if x == nil {
		return nil
	}
	var field debeziumField
	if len(e.Schema.Fields) > 0 {
		for _, f := range e.Schema.Fields[0].Fields {
			if f.Field == fieldName {
				field = f
				break
			}
		}
	}
	switch v := x.(type) {
	case map[string]any:
		switch field.Name {
		case "io.debezium.data.VariableScaleDecimal":
			if value, ok := v["value"]; ok {
				if str, ok := value.(string); ok {
					b, err := base64.StdEncoding.DecodeString(str)
					if err != nil {
						return str
					}
					i := new(big.Int).SetBytes(b)
					return i.String()
				}
			}
		default:
			jsonValue, _ := json.Marshal(v)
			return string(jsonValue)
		}

	case float64:
		switch field.Name {
		case "io.debezium.time.MicroTime", "io.debezium.time.MicroTimestamp":
			return time.Unix(0, int64(v)*1000)
		case "io.debezium.time.NanoTime", "io.debezium.time.NanoTimestamp":
			return time.Unix(0, int64(v))
		default:
			return v
		}
	default:
		return v
	}

	return x
}
