# Go Kafka Library

A shared library for Kafka operations across FileConvert microservices.

## Features

- Kafka producer configuration
- Kafka consumer configuration
- Message serialization/deserialization
- Error handling
- Connection management
- Viper configuration integration
- Default values with overrides

## Configuration

The library uses Viper for configuration management. Configuration can be provided through environment variables, configuration files, or direct configuration structs.

### Configuration Keys

#### Producer Configuration
- `kafka.brokers`: List of Kafka brokers (default: ["localhost:9092"])
- `kafka.client_id`: Client ID for the producer (default: "file-convert-producer")
- `kafka.topic`: Topic to produce messages to
- `kafka.producer.timeout`: Producer timeout (default: "5s")
- `kafka.producer.retries`: Number of retries (default: 3)
- `kafka.producer.max_bytes`: Maximum message size (default: 1000000)
- `kafka.producer.compression`: Compression codec (none, gzip, snappy, lz4, zstd)
- `kafka.producer.batch_size`: Batch size in bytes (default: 16384)
- `kafka.producer.batch_wait`: Batch wait time (default: "100ms")
- `kafka.producer.required_acks`: Required acknowledgments (none, leader, all)
- `kafka.producer.max_message_bytes`: Maximum message size (default: 1000000)
- `kafka.producer.retry_backoff`: Retry backoff time (default: "100ms")
- `kafka.producer.retry_max`: Maximum number of retries (default: 3)
- `kafka.producer.enable_idempotence`: Enable idempotent producer (default: true)

#### Consumer Configuration
- `kafka.brokers`: List of Kafka brokers (default: ["localhost:9092"])
- `kafka.consumer.group_id`: Consumer group ID (default: "file-convert-group")
- `kafka.consumer.topics`: List of topics to consume
- `kafka.consumer.max_wait_time`: Maximum wait time (default: "250ms")
- `kafka.consumer.max_processing_time`: Maximum processing time (default: "100ms")
- `kafka.consumer.fetch_min`: Minimum fetch size (default: 1)
- `kafka.consumer.fetch_default`: Default fetch size (default: 1048576)
- `kafka.consumer.fetch_max`: Maximum fetch size (default: 0)
- `kafka.consumer.retry_backoff`: Retry backoff time (default: "2s")
- `kafka.consumer.max_retries`: Maximum number of retries (default: 3)
- `kafka.consumer.auto_offset_reset`: Auto offset reset (earliest, latest)
- `kafka.consumer.enable_auto_commit`: Enable auto commit (default: true)
- `kafka.consumer.auto_commit_interval`: Auto commit interval (default: "5s")

## Usage

### Basic Usage

```go
import (
    "github.com/file-convert/go-kafka"
    "github.com/spf13/viper"
)

func main() {
    // Initialize Viper
    viper.SetConfigName("config")
    viper.SetConfigType("yaml")
    viper.AddConfigPath(".")
    viper.AutomaticEnv()
    
    if err := viper.ReadInConfig(); err != nil {
        if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
            panic(fmt.Errorf("fatal error config file: %w", err))
        }
    }

    // Create a new Kafka producer with default configuration
    producer, err := gokafka.NewProducer(nil)
    if err != nil {
        log.Fatal(err)
    }
    defer producer.Close()

    // Send a message
    err = producer.SendMessage("key", map[string]interface{}{
        "field1": "value1",
        "field2": "value2",
    })
    if err != nil {
        log.Fatal(err)
    }

    // Create a new Kafka consumer with default configuration
    consumer, err := gokafka.NewConsumer(nil)
    if err != nil {
        log.Fatal(err)
    }
    defer consumer.Close()

    // Start consuming messages
    ctx := context.Background()
    err = consumer.ConsumeMessages(ctx, func(msg *sarama.ConsumerMessage) error {
        // Process message
        var data map[string]interface{}
        if err := json.Unmarshal(msg.Value, &data); err != nil {
            return err
        }
        
        // Handle message
        fmt.Printf("Received message: %+v\n", data)
        return nil
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

### Custom Configuration

You can also provide custom configuration instead of using Viper:

```go
// Producer configuration
producerConfig := &gokafka.ProducerConfig{
    Brokers:  []string{"localhost:9092"},
    ClientID: "custom-producer",
    Topic:    "my-topic",
    Timeout:  5 * time.Second,
}

producer, err := gokafka.NewProducer(producerConfig)

// Consumer configuration
consumerConfig := &gokafka.ConsumerConfig{
    Brokers:         []string{"localhost:9092"},
    GroupID:         "custom-group",
    Topics:          []string{"my-topic"},
    MaxWaitTime:     250 * time.Millisecond,
    AutoOffsetReset: "latest",
}

consumer, err := gokafka.NewConsumer(consumerConfig)
```

## Configuration File Example

```yaml
kafka:
  brokers:
    - localhost:9092
    - localhost:9093
  client_id: file-convert-producer
  topic: my-topic
  
  producer:
    timeout: 5s
    retries: 3
    max_bytes: 1000000
    compression: none
    batch_size: 16384
    batch_wait: 100ms
    required_acks: all
    max_message_bytes: 1000000
    retry_backoff: 100ms
    retry_max: 3
    enable_idempotence: true
  
  consumer:
    group_id: file-convert-group
    topics:
      - my-topic
    max_wait_time: 250ms
    max_processing_time: 100ms
    fetch_min: 1
    fetch_default: 1048576
    fetch_max: 0
    retry_backoff: 2s
    max_retries: 3
    auto_offset_reset: latest
    enable_auto_commit: true
    auto_commit_interval: 5s
``` 