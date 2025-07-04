package gokafka

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/spf13/viper"
)

// Message represents a Kafka message with all necessary fields
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

const (
	// Default values for consumer
	DefaultConsumerGroupID           = "file-convert-group"
	DefaultConsumerMaxWaitTime       = 250 * time.Millisecond
	DefaultConsumerMaxProcessingTime = 100 * time.Millisecond
	DefaultConsumerFetchMin          = 1
	DefaultConsumerFetchDefault      = 1024 * 1024
	DefaultConsumerFetchMax          = 0
	DefaultConsumerRetryBackoff      = 2 * time.Second
	DefaultConsumerMaxRetries        = 3
)

// Consumer provides Kafka message consumption capabilities
type Consumer struct {
	consumer sarama.ConsumerGroup
	topics   []string
	config   *ConsumerConfig
}

// ConsumerConfig holds the configuration for Kafka consumers
type ConsumerConfig struct {
	Brokers            []string
	GroupID            string
	Topics             []string
	MaxWaitTime        time.Duration
	MaxProcessingTime  time.Duration
	FetchMin           int32
	FetchDefault       int32
	FetchMax           int32
	RetryBackoff       time.Duration
	MaxRetries         int
	AutoOffsetReset    string
	EnableAutoCommit   bool
	AutoCommitInterval time.Duration
}

// NewConsumerConfigFromViper creates a new consumer configuration from Viper
func NewConsumerConfigFromViper() *ConsumerConfig {
	config := &ConsumerConfig{
		Brokers:            viper.GetStringSlice("kafka.brokers"),
		GroupID:            viper.GetString("kafka.consumer.group_id"),
		Topics:             viper.GetStringSlice("kafka.consumer.topics"),
		MaxWaitTime:        viper.GetDuration("kafka.consumer.max_wait_time"),
		MaxProcessingTime:  viper.GetDuration("kafka.consumer.max_processing_time"),
		FetchMin:           int32(viper.GetInt("kafka.consumer.fetch_min")),
		FetchDefault:       int32(viper.GetInt("kafka.consumer.fetch_default")),
		FetchMax:           int32(viper.GetInt("kafka.consumer.fetch_max")),
		RetryBackoff:       viper.GetDuration("kafka.consumer.retry_backoff"),
		MaxRetries:         viper.GetInt("kafka.consumer.max_retries"),
		AutoOffsetReset:    viper.GetString("kafka.consumer.auto_offset_reset"),
		EnableAutoCommit:   viper.GetBool("kafka.consumer.enable_auto_commit"),
		AutoCommitInterval: viper.GetDuration("kafka.consumer.auto_commit_interval"),
	}

	// Set defaults if not configured
	if len(config.Brokers) == 0 {
		config.Brokers = []string{"localhost:9092"}
	}
	if config.GroupID == "" {
		config.GroupID = DefaultConsumerGroupID
	}
	if config.MaxWaitTime == 0 {
		config.MaxWaitTime = DefaultConsumerMaxWaitTime
	}
	if config.MaxProcessingTime == 0 {
		config.MaxProcessingTime = DefaultConsumerMaxProcessingTime
	}
	if config.FetchMin == 0 {
		config.FetchMin = DefaultConsumerFetchMin
	}
	if config.FetchDefault == 0 {
		config.FetchDefault = DefaultConsumerFetchDefault
	}
	if config.FetchMax == 0 {
		config.FetchMax = DefaultConsumerFetchMax
	}
	if config.RetryBackoff == 0 {
		config.RetryBackoff = DefaultConsumerRetryBackoff
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = DefaultConsumerMaxRetries
	}
	if config.AutoOffsetReset == "" {
		config.AutoOffsetReset = "latest"
	}
	if config.AutoCommitInterval == 0 {
		config.AutoCommitInterval = 5 * time.Second
	}

	return config
}

// NewConsumer creates a new Kafka consumer
func NewConsumer(config *ConsumerConfig) (*Consumer, error) {
	if config == nil {
		config = NewConsumerConfigFromViper()
	}

	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("at least one broker is required")
	}

	if len(config.Topics) == 0 {
		return nil, fmt.Errorf("at least one topic is required")
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	saramaConfig.Consumer.Offsets.Initial = getInitialOffset(config.AutoOffsetReset)
	saramaConfig.Consumer.Offsets.AutoCommit.Enable = config.EnableAutoCommit
	saramaConfig.Consumer.Offsets.AutoCommit.Interval = config.AutoCommitInterval
	saramaConfig.Consumer.MaxWaitTime = config.MaxWaitTime
	saramaConfig.Consumer.MaxProcessingTime = config.MaxProcessingTime
	saramaConfig.Consumer.Fetch.Min = config.FetchMin
	saramaConfig.Consumer.Fetch.Default = config.FetchDefault
	saramaConfig.Consumer.Fetch.Max = config.FetchMax
	saramaConfig.Consumer.Retry.Backoff = config.RetryBackoff

	consumer, err := sarama.NewConsumerGroup(config.Brokers, config.GroupID, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	return &Consumer{
		consumer: consumer,
		topics:   config.Topics,
		config:   config,
	}, nil
}

// Close closes the Kafka consumer
func (c *Consumer) Close() error {
	if c.consumer != nil {
		return c.consumer.Close()
	}
	return nil
}

// ConsumeMessages starts consuming messages from Kafka using our abstracted Message type
func (c *Consumer) ConsumeMessages(ctx context.Context, handler func(*Message) error) error {
	consumerHandler := &consumerGroupHandler{handler: handler}
	return c.consumer.Consume(ctx, c.topics, consumerHandler)
}

// consumerGroupHandler implements sarama.ConsumerGroupHandler
type consumerGroupHandler struct {
	handler func(*Message) error
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for saramaMsg := range claim.Messages() {
		msg := &Message{
			Topic:     saramaMsg.Topic,
			Partition: saramaMsg.Partition,
			Offset:    saramaMsg.Offset,
			Key:       saramaMsg.Key,
			Value:     saramaMsg.Value,
			Timestamp: saramaMsg.Timestamp,
		}

		err := h.handler(msg)
		if err != nil {
			return err
		}
	}
	return nil
}

// Helper functions
func getInitialOffset(value string) int64 {
	switch value {
	case "earliest":
		return sarama.OffsetOldest
	case "latest":
		return sarama.OffsetNewest
	default:
		return sarama.OffsetNewest
	}
}
