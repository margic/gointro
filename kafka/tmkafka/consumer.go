package tmkafka

import (
	"errors"
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
	ReceiverChannel chan interface{}
}

// ConsumerConfig contains the config
type ConsumerConfig struct {
	BrokerList      []string
	SaramaConfig    *sarama.Config
	MessageDecoder  func([]byte) interface{}
	ConsumerChannel chan interface{}
}

// NewConsumer create a new consumer to read from topic
func NewConsumer(config ConsumerConfig) (*Consumer, error) {
	log.Debug("new consumer")
	if config.MessageDecoder == nil || config.ConsumerChannel == nil {
		return nil, errors.New("MessageDecoder and ReceiverChannel must be provided")
	}
	c, err := sarama.NewConsumer(config.BrokerList, config.SaramaConfig)
	if err != nil {
		log.WithError(err).Error("error creating new consumer")
	}
	mc := &Consumer{
		MessageConsumer: c,
		MessageDecoder:  config.MessageDecoder,
		ReceiverChannel: config.ConsumerChannel,
	}
	return mc, nil
}

// Start start consuming from topic
func (c *Consumer) Start() error {
	log.Info("starting consumer")
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

	log.WithField("topic", t).Info("consumer topic")
	partitionList, err := getPartitions(c.MessageConsumer)
	if err != nil {
		log.WithError(err).Error("error getting kafka partitions")
	}

	var (
		closing = make(chan struct{})
		wg      sync.WaitGroup
	)

	for _, partition := range partitionList {
		log.WithField("partition", partition).Info("start consuming")
		pc, err := c.MessageConsumer.ConsumePartition(t, partition, initialOffset)
		if err != nil {
			return err
		}

		go func(pc sarama.PartitionConsumer) {
			// will wait for a message on closing or until closing is closed
			<-closing
			if err := pc.Close(); err != nil {
				log.WithError(err).Error("error closing consumer")
			}
		}(pc)

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for message := range pc.Messages() {
				decoded := c.MessageDecoder(message.Value)
				c.ReceiverChannel <- decoded
			}
		}(pc)
	}

	wg.Wait()
	log.WithField("state", "done consuming").Info("ConsumerState")

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
func (c *Consumer) Stop() {
	log.Info("consumer stopping")
	close(closing)
	log.Info("consumer stopped")
}
