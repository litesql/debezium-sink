# SQLite Debezium Sink
SQLite Extension to integrate with Debezium.

## Installation

Download **debezium** extension from the [releases page](https://github.com/litesql/debezium-sink/releases).
Here's a great article that explains [how to install the SQLite extension.](https://antonz.org/install-sqlite-extension/)

### Compiling from source

- [Go 1.25+](https://go.dev) and CGO_ENABLED=1 is required.

```sh
go build -ldflags="-s -w" -buildmode=c-shared -o debezium.so
```

- Use .so extension for Linux, .dylib for MacOS and .dll for Windows

## Basic usage

### Loading the extension

```sh
sqlite3

# Load the extension
.load ./debezium

# check version (optional)
SELECT debezium_info();
```

### Consumer

```sh
# Create a virtual table using DEBEZIUM to configure the connection to the broker
CREATE VIRTUAL TABLE temp.debezium_sink USING debezium(brokers='localhost:9092', consumer_group='sqlite-debezium');

# Insert the topic name into the created virtual table to subscribe
INSERT INTO temp.debezium_sink(topic) VALUES('my_topic');

# To start consuming a partition at specific offset. Example:
# partition 0 at earliest offset
# partition 1 at latest offset
# partition 2 at offset 42
INSERT INTO temp.debezium_sink(topic, offsets) VALUES('my_topic', 
'{
  "0": "earliest", 
  "1": "latest",
  "2": "42"
}');
```

Consumer table schema:

```sql
TABLE temp.debezium_sink(
  topic TEXT,
  offsets JSONB
)
```

### Subscriptions management

Query the subscription virtual table (the virtual table created using **debezium**) to view all the active subscriptions for the current SQLite connection.

```sql
SELECT topic FROM temp.debezium_sink;
┌────────────┐
│   topic    │
├────────────┤
│ 'my_topic' │
└────────────┘
```

Delete the row to unsubscribe from the topic:

```sql
DELETE FROM temp.debezium_sink WHERE topic = 'my_topic';
```

#### Set offsets

To set the consumer's partition offsets (using consumer group), just update the offsets column:

```sql
SELECT topic, offsets FROM temp.debezium_sink;
┌────────────┬─────────────────────────────────┐
│   topic    │             offsets             │
├────────────┼─────────────────────────────────┤
│ 'my_topic' │ '{"0":{"Epoch":0,"Offset":36}}' │
└────────────┴─────────────────────────────────┘

UPDATE temp.debezium_sink SET offsets = '{"0":{"Epoch":0,"Offset":12}}' WHERE topic = 'my_topic';
```

## Configuring

You can configure the connection to the broker by passing parameters to the VIRTUAL TABLE.

| Param | Description | Default |
|-------|-------------|---------|
| brokers | Comma delimited list of seed brokers | localhost:9092 |
| client_id | Client ID  | sqlite |
| consumer_group | Consumer group | |
| isolation_level | Fetch isolation level. 0 = read uncommitted, 1 = read committed | 0 |
| auto_offset_reset | Determines the behavior of a consumer group when there is no valid committed offset for a partition. (latest, earliest or none) | |
use_namespace | Keep schema/namespace instead of using the main database | false |
position_tracker_table | Table for replication position checkpoints | debezium_stat |
| sasl_type| SASL type: plain, sha256 or sha512 | |
| sasl_user | SASL user | |
| sasl_pass | SASL pass | |
| insecure  | Insecure skip TLS validation |  |
| cert_file | TLS: Path to certificate file | |
| cert_key_file | TLS: Path to certificate key file | |
| ca_file | TLS: Path to CA certificate file | |
| logger | Log errors to stdout, stderr or file:/path/to/file.log |
