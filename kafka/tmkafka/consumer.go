package tmkafka

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/spf13/viper"
)

// Program control
var (
	closing = make(chan struct{})
	wg      sync.WaitGroup
)

// Consumer reads from a kafka topic
type Consumer struct {
	MessageConsumer sarama.Consumer
	MessageDecoder  func([]byte) interface{}
}

// ConsumerConfig contains the config
type ConsumerConfig struct {
	BrokerList     []string
	Config         *sarama.Config
	MessageDecoder func([]byte) interface{}
}

// NewConsumer create a new consumer to read from topic
func NewConsumer(config ConsumerConfig) (*Consumer, error) {
	log.Debug("new consumer")
	c, err := sarama.NewConsumer(config.BrokerList, config.Config)
	if err != nil {
		log.WithError(err).Error("ConsumerError")
	}
	mc := &Consumer{
		MessageConsumer: c,
		MessageDecoder:  config.MessageDecoder,
	}
	return mc, nil
}

// Start start consuming from topic
func (c *Consumer) Start() error {
	log.Info("ConsumerStarting")
	t := viper.GetString("kafka.topic")
	offset := viper.GetString("kafka.initialOffset")
	var initialOffset int64
	switch offset {
	case "oldest":
		initialOffset = sarama.OffsetOldest
	case "newest":
		initialOffset = sarama.OffsetNewest
	default:
		return errors.New("kafka.initiaOffset should be `oldest` or `newest`")
	}

	log.WithField("topic", t).Info("ConsumerSettings")
	partitionList, err := getPartitions(c.MessageConsumer)
	if err != nil {
		log.WithError(err).Error("ConsumerError")
	}

	var (
		closing = make(chan struct{})
		wg      sync.WaitGroup
	)

	for _, partition := range partitionList {
		log.WithFields(log.Fields{
			"partition": partition,
			"state":     "starting",
		}).Info("ConsumerState")
		pc, err := c.MessageConsumer.ConsumePartition(t, partition, initialOffset)
		if err != nil {
			return err
		}

		go func(pc sarama.PartitionConsumer) {
			<-closing
			pc.AsyncClose()
		}(pc)

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for message := range pc.Messages() {
				log.WithFields(log.Fields{
					"offset":    message.Offset,
					"partition": message.Partition,
					"topic":     message.Topic,
					"value":     fmt.Sprintf("%s", message.Value),
				}).Debug("MessageReceived")
				decoded := c.MessageDecoder(message.Value)
				log.WithField("message", fmt.Sprintf("%v", decoded)).Debug("MessageDecoded")
			}
		}(pc)
	}

	wg.Wait()
	log.WithField("state", "done consuming").Info("ConsumerState")
	//close(messages)

	if err := c.MessageConsumer.Close(); err != nil {
		log.WithError(err).Error("ConsumerError")
	}

	return nil
}

func getPartitions(c sarama.Consumer) ([]int32, error) {
	log.Debug("Getting partions")

	p := viper.GetString("kafka.partitions")
	t := viper.GetString("kafka.topic")
	if strings.ToUpper(p) == "ALL" {
		return c.Partitions(t)
	}

	log.WithFields(log.Fields{
		"kafka.partitions": p,
		"kafka.topic":      t,
	}).Info("ConsumerStarting")
	pSlice := strings.Split(p, ",")
	var pList []int32
	for pIndex := range pSlice {
		val, err := strconv.ParseInt(pSlice[pIndex], 10, 32)
		if err != nil {
			return nil, err
		}
		pList = append(pList, int32(val))
	}
	return pList, nil
}

// Stop stop consuming
func (c *Consumer) Stop() {}

// // NewConsumer create a new Consumer type with provided kafka config
// func NewConsumer(cfg *kafka.ConfigMap) (*Consumer, error) {
// 	c, err := kafka.NewConsumer(cfg)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &Consumer{
// 		kc:  c,
// 		run: true,
// 	}, nil
// }

// // Start starts the consumer
// func (c *Consumer) Start() {
// 	for c.run {
// 		select {
// 		case ev := <-c.kc.Events():

// 			switch e := ev.(type) {
// 			case kafka.AssignedPartitions:
// 				log.Infof("%% %v\n", e)
// 				c.kc.Assign(e.Partitions)
// 			case kafka.RevokedPartitions:
// 				log.Infof("%% %v\n", e)
// 				c.kc.Unassign()
// 			case *kafka.Message:
// 				log.Infof("%% Message on %s:\n%s\n",
// 					e.TopicPartition, string(e.Value))
// 			case kafka.PartitionEOF:
// 				log.Infof("%% Reached %v\n", e)
// 			case kafka.Error:
// 				log.Infof("%% Error: %v\n", e)
// 				c.run = false
// 			}
// 		}
// 	}
// }

// // Stop stops the consumer
// func (c *Consumer) Stop() {
// 	// set run to false and the loop in start will end
// 	log.Info("Stopping app")
// 	c.run = false
// 	c.kc.Close()
// }
