package gstream

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
)

// kafkaConsumer implements the consumer interface using Sarama.
type kafkaConsumer struct {
	consumerGroup sarama.ConsumerGroup
	config        KafkaConfig
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

func newKafkaConsumer(cfg KafkaConfig) (*kafkaConsumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Version = sarama.V2_6_0_0

	consumerGroup, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &kafkaConsumer{
		consumerGroup: consumerGroup,
		config:        cfg,
		ctx:           ctx,
		cancel:        cancel,
	}, nil
}

func (c *kafkaConsumer) Subscribe(topic string, handler MessageHandler) error {
	h := &consumerGroupHandler{
		handler: handler,
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			if err := c.consumerGroup.Consume(c.ctx, []string{topic}, h); err != nil {
				log.Error().
					Err(err).
					Msg("consumer error")
			}

			if c.ctx.Err() != nil {
				return
			}
		}
	}()

	return nil
}

func (c *kafkaConsumer) Close() error {
	c.cancel()
	c.wg.Wait()
	return c.consumerGroup.Close()
}

// consumerGroupHandler implements sarama.ConsumerGroupHandler.
type consumerGroupHandler struct {
	handler MessageHandler
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var message Message

		if err := json.Unmarshal(msg.Value, &message); err != nil {
			log.Error().
				Err(err).
				Msg("failed to unmarshal message")
			session.MarkMessage(msg, "")
			continue
		}

		// Use Key from Kafka message key (more authoritative)
		message.Key = string(msg.Key)

		if err := h.handler(message); err != nil {
			log.Error().
				Err(err).
				Str("key", message.Key).
				Str("action", message.Action).
				Msg("handler error")
		}

		session.MarkMessage(msg, "")
	}
	return nil
}

// kafkaProducer implements the producer interface using Sarama.
type kafkaProducer struct {
	producer sarama.SyncProducer
	config   KafkaConfig
}

func newKafkaProducer(cfg KafkaConfig) (*kafkaProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all replicas
	config.Producer.Retry.Max = 3
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(cfg.Brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	return &kafkaProducer{
		producer: producer,
		config:   cfg,
	}, nil
}

func (p *kafkaProducer) Publish(topic string, key string, action string, data []byte) (Ack, error) {
	msg := Message{
		Key:    key,
		Action: action,
		Data:   data,
	}

	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return Ack{}, fmt.Errorf("failed to marshal message: %w", err)
	}

	message := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(msgBytes),
	}

	partition, offset, err := p.producer.SendMessage(message)
	if err != nil {
		return Ack{}, fmt.Errorf("failed to publish message: %w", err)
	}

	return Ack{
		Key:       key,
		Offset:    offset,
		Partition: partition,
		Delivered: true,
	}, nil
}

func (p *kafkaProducer) Close() error {
	return p.producer.Close()
}
