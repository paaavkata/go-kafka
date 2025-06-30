package gokafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/spf13/viper"
)

const (
	// Default values
	DefaultProducerTimeout     = 5 * time.Second
	DefaultProducerRetries     = 3
	DefaultProducerMaxBytes    = 1000000
	DefaultProducerCompression = "none"
	DefaultProducerBatchSize   = 16384
	DefaultProducerBatchWait   = 100 * time.Millisecond
)

// Producer provides Kafka message production capabilities
type Producer struct {
	producer sarama.SyncProducer
	topic    string
	config   *ProducerConfig
}

// ProducerConfig holds the configuration for Kafka producers
type ProducerConfig struct {
	Brokers           []string
	ClientID          string
	Topic             string
	Timeout           time.Duration
	Retries           int
	MaxBytes          int
	Compression       string
	BatchSize         int
	BatchWait         time.Duration
	RequiredAcks      sarama.RequiredAcks
	MaxMessageBytes   int
	RetryBackoff      time.Duration
	RetryMax          int
	EnableIdempotence bool
}

// NewProducerConfigFromViper creates a new producer configuration from Viper
func NewProducerConfigFromViper() *ProducerConfig {
	config := &ProducerConfig{
		Brokers:           viper.GetStringSlice("kafka.brokers"),
		ClientID:          viper.GetString("kafka.client_id"),
		Topic:             viper.GetString("kafka.topic"),
		Timeout:           viper.GetDuration("kafka.producer.timeout"),
		Retries:           viper.GetInt("kafka.producer.retries"),
		MaxBytes:          viper.GetInt("kafka.producer.max_bytes"),
		Compression:       viper.GetString("kafka.producer.compression"),
		BatchSize:         viper.GetInt("kafka.producer.batch_size"),
		BatchWait:         viper.GetDuration("kafka.producer.batch_wait"),
		RequiredAcks:      getRequiredAcks(viper.GetString("kafka.producer.required_acks")),
		MaxMessageBytes:   viper.GetInt("kafka.producer.max_message_bytes"),
		RetryBackoff:      viper.GetDuration("kafka.producer.retry_backoff"),
		RetryMax:          viper.GetInt("kafka.producer.retry_max"),
		EnableIdempotence: viper.GetBool("kafka.producer.enable_idempotence"),
	}

	// Set defaults if not configured
	if len(config.Brokers) == 0 {
		config.Brokers = []string{"localhost:9092"}
	}
	if config.ClientID == "" {
		config.ClientID = "file-convert-producer"
	}
	if config.Timeout == 0 {
		config.Timeout = DefaultProducerTimeout
	}
	if config.Retries == 0 {
		config.Retries = DefaultProducerRetries
	}
	if config.MaxBytes == 0 {
		config.MaxBytes = DefaultProducerMaxBytes
	}
	if config.Compression == "" {
		config.Compression = DefaultProducerCompression
	}
	if config.BatchSize == 0 {
		config.BatchSize = DefaultProducerBatchSize
	}
	if config.BatchWait == 0 {
		config.BatchWait = DefaultProducerBatchWait
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = DefaultProducerMaxBytes
	}
	if config.RetryBackoff == 0 {
		config.RetryBackoff = 100 * time.Millisecond
	}
	if config.RetryMax == 0 {
		config.RetryMax = DefaultProducerRetries
	}

	return config
}

// NewProducer creates a new Kafka producer
func NewProducer(config *ProducerConfig) (*Producer, error) {
	if config == nil {
		config = NewProducerConfigFromViper()
	}

	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("at least one broker is required")
	}

	if config.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Producer.Timeout = config.Timeout
	saramaConfig.Producer.Retry.Max = config.RetryMax
	saramaConfig.Producer.Retry.Backoff = config.RetryBackoff
	saramaConfig.Producer.MaxMessageBytes = config.MaxMessageBytes
	saramaConfig.Producer.RequiredAcks = config.RequiredAcks
	saramaConfig.Producer.Compression = getCompressionCodec(config.Compression)
	saramaConfig.Producer.Flush.Bytes = config.BatchSize
	saramaConfig.Producer.Flush.Frequency = config.BatchWait
	saramaConfig.Producer.Idempotent = config.EnableIdempotence
	saramaConfig.ClientID = config.ClientID

	producer, err := sarama.NewSyncProducer(config.Brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return &Producer{
		producer: producer,
		topic:    config.Topic,
		config:   config,
	}, nil
}

// Close closes the Kafka producer
func (p *Producer) Close() error {
	if p.producer != nil {
		return p.producer.Close()
	}
	return nil
}

// SendMessage sends a message to Kafka
func (p *Producer) SendMessage(key string, value interface{}) error {
	jsonValue, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(jsonValue),
	}

	_, _, err = p.producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}

// SendMessageWithContext sends a message to Kafka with context
func (p *Producer) SendMessageWithContext(ctx context.Context, key string, value interface{}) error {
	jsonValue, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(jsonValue),
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		_, _, err := p.producer.SendMessage(msg)
		if err != nil {
			return fmt.Errorf("failed to send message: %w", err)
		}
		return nil
	}
}

// Helper functions
func getRequiredAcks(value string) sarama.RequiredAcks {
	switch value {
	case "none":
		return sarama.NoResponse
	case "leader":
		return sarama.WaitForLocal
	case "all":
		return sarama.WaitForAll
	default:
		return sarama.WaitForAll
	}
}

func getCompressionCodec(value string) sarama.CompressionCodec {
	switch value {
	case "none":
		return sarama.CompressionNone
	case "gzip":
		return sarama.CompressionGZIP
	case "snappy":
		return sarama.CompressionSnappy
	case "lz4":
		return sarama.CompressionLZ4
	case "zstd":
		return sarama.CompressionZSTD
	default:
		return sarama.CompressionNone
	}
}
