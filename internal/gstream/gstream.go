package gstream

import "fmt"

// Stream defines a generic message streaming interface.
// It provides persistent key-value registry, message routing, and delivery acknowledgments.
type Stream interface {
	Register(key, target string) error
	Publish(key, action string, data []byte) (Ack, error)
	Subscribe(target string, handler MessageHandler) error
	SubscribeAction(target string, action string, handler MessageHandler) error
	Close() error
}

// producer interface for Kafka message publishing.
type producer interface {
	Publish(topic string, key string, action string, data []byte) (Ack, error)
	Close() error
}

// consumer interface for Kafka message consumption.
type consumer interface {
	Subscribe(topic string, handler MessageHandler) error
	Close() error
}

// registry interface for key-to-target mapping storage.
type registry interface {
	Set(key, target string) error
	Get(key string) (string, error)
	Delete(key string) error
	Close() error
}

// MessageHandler is called when a message is received.
type MessageHandler func(msg Message) error

// ActionMatcher is an internal handler that filters by action.
type actionMatcher struct {
	action  string
	handler MessageHandler
}

func (am *actionMatcher) handle(msg Message) error {
	if am.action == "*" || am.action == msg.Action {
		return am.handler(msg)
	}
	return nil
}

// stream is the concrete implementation using Kafka and Redis.
type stream struct {
	producer producer
	consumer consumer
	registry registry
}

// New creates a new Stream instance.
func New(cfg Config) (Stream, error) {
	producer, err := newKafkaProducer(cfg.Kafka)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	consumer, err := newKafkaConsumer(cfg.Kafka)
	if err != nil {
		producer.Close()
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}

	registry, err := newRedisRegistry(cfg.Redis)
	if err != nil {
		producer.Close()
		consumer.Close()
		return nil, fmt.Errorf("failed to create redis registry: %w", err)
	}

	return &stream{
		producer: producer,
		consumer: consumer,
		registry: registry,
	}, nil
}

func (s *stream) Register(key, target string) error {
	if err := s.registry.Set(key, target); err != nil {
		return fmt.Errorf("failed to register key %s to target %s: %w", key, target, err)
	}
	return nil
}

func (s *stream) Publish(key, action string, data []byte) (Ack, error) {
	// Lookup target topic based on the call_id
	target, err := s.registry.Get(key)
	if err != nil {
		return Ack{}, fmt.Errorf("failed to lookup target for key %s: %w", key, err)
	}

	// Publish to the target-specific Kafka topic
	// The action is embedded in the Message for the consumer to handle
	ack, err := s.producer.Publish(target, key, action, data)
	if err != nil {
		return Ack{}, fmt.Errorf("failed to publish message to target %s: %w", target, err)
	}

	return ack, nil
}

func (s *stream) Subscribe(target string, handler MessageHandler) error {
	if err := s.consumer.Subscribe(target, handler); err != nil {
		return fmt.Errorf("failed to subscribe to target %s: %w", target, err)
	}
	return nil
}

func (s *stream) SubscribeAction(target string, action string, handler MessageHandler) error {
	matcher := &actionMatcher{
		action:  action,
		handler: handler,
	}

	if err := s.consumer.Subscribe(target, matcher.handle); err != nil {
		return fmt.Errorf("failed to subscribe to target %s with action %s: %w", target, action, err)
	}
	return nil
}

func (s *stream) Close() error {
	var errs []error

	if err := s.consumer.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close consumer: %w", err))
	}

	if err := s.producer.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close producer: %w", err))
	}

	if err := s.registry.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close registry: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors during close: %v", errs)
	}

	return nil
}

// Config holds configuration for the Stream.
type Config struct {
	Kafka KafkaConfig
	Redis RedisConfig
}

// KafkaConfig contains Kafka connection settings.
type KafkaConfig struct {
	Brokers []string
	Topic   string
	GroupID string
}

// RedisConfig contains Redis connection settings.
type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

// Message represents a message to be published.
type Message struct {
	Key    string `json:"key"`
	Action string `json:"action"`
	Data   []byte `json:"data"`
}

// Ack represents acknowledgment of message delivery.
type Ack struct {
	Key       string
	Offset    int64
	Partition int32
	Delivered bool
}
