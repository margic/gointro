package tmkafka

import (
	log "github.com/Sirupsen/logrus"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Consumer reads from a kafka topic
type Consumer struct {
	kc  *kafka.Consumer
	run bool
}

// NewConsumer create a new Consumer type with provided kafka config
func NewConsumer(cfg *kafka.ConfigMap) (*Consumer, error) {
	c, err := kafka.NewConsumer(cfg)
	if err != nil {
		return nil, err
	}
	return &Consumer{
		kc:  c,
		run: true,
	}, nil
}

// Start starts the consumer
func (c *Consumer) Start() {
	for c.run {
		select {
		case ev := <-c.kc.Events():

			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				log.Infof("%% %v\n", e)
				c.kc.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				log.Infof("%% %v\n", e)
				c.kc.Unassign()
			case *kafka.Message:
				log.Infof("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
			case kafka.PartitionEOF:
				log.Infof("%% Reached %v\n", e)
			case kafka.Error:
				log.Infof("%% Error: %v\n", e)
				c.run = false
			}
		}
	}
}

// Stop stops the consumer
func (c *Consumer) Stop() {
	// set run to false and the loop in start will end
	log.Info("Stopping app")
	c.run = false
	c.kc.Close()
}
