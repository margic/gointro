package tmkafka

import (
	"fmt"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/spf13/viper"
)

// Producer sends messages a kafka topic
type Producer struct {
	MessageProducer sarama.SyncProducer
}

// ProducerConfig wraps the config variables so they can be provided outside
// of the producer itself.
type ProducerConfig struct {
	BrokerList []string
	Config     *sarama.Config
}

// NewProducer creates a new producer
func NewProducer(config ProducerConfig) (*Producer, error) {
	log.Debug("new producer")

	producer, err := sarama.NewSyncProducer(config.BrokerList, config.Config)
	if err != nil {
		return nil, err
	}

	return &Producer{
		MessageProducer: producer,
	}, nil
}

// Send a message to the topic
func (p *Producer) Send(msg *StringMessage) error {
	pmsg := &sarama.ProducerMessage{
		Value: msg,
		Topic: viper.GetString("kafka.topic"),
	}

	partition, offset, err := p.MessageProducer.SendMessage(pmsg)
	if err != nil {
		return err
	}
	log.WithFields(log.Fields{
		"value":     msg.Value,
		"partition": fmt.Sprintf("%v", partition),
		"offset":    fmt.Sprintf("%v", offset),
	}).Debug("MessageSent")
	return nil
}
