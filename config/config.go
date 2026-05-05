package config

const (
	// Common config
	Brokers     = "brokers"       // Comma delimited list of seed brokers
	ClientID    = "client_id"     // ClientID (default sqlite)
	SaslType    = "sasl_type"     // SASL type: plain, sha256, sha512
	SaslUser    = "sasl_user"     // SASL user
	SaslPass    = "sasl_pass"     // SASL pass
	CertFile    = "cert_file"     // TLS: path to certificate file
	CertKeyFile = "cert_key_file" // TLS: path to .pem certificate key file
	CertCAFile  = "ca_file"       // TLS: path to CA certificate file
	Insecure    = "insecure"      // TLS: Insecure skip TLS verification
	Logger      = "logger"        // Log errors to "stdout, stderr or file:/path/to/log.txt"

	// Consumer module config
	UseNamespace    = "use_namespace"     // Keep schema/namespace (otherwise always use main database)
	ConsumerGroup   = "consumer_group"    // Consumer group
	IsolationLevel  = "isolation_level"   // Fetch isolation level: 0 = read uncommitted (default), 1 = read committed
	AutoOffsetReset = "auto_offset_reset" // Determines the behavior of a consumer group when there is no valid committed offset for a partition. (latest, earliest, none)

	DefaultConsumerVTabName = "debezium"
	DefaultClientID         = "sqlite"
)
