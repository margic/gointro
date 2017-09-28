package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/margic/gointro/kafka/counter"
	tmkafka "github.com/margic/gointro/kafka/tmkafka"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// consumeCmd represents the consume command
var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "Consumes messages from Kafka",
	Long: `This command uses the settings in the config.yml file
to connect to a Kafka topic and consume messages from the topic.
Messages are received on the message channel. `,
	Run: func(cmd *cobra.Command, args []string) {
		consume()
	},
}

var consumer *tmkafka.Consumer
var messages chan interface{}

func consume() {
	log.Info("consume called")
	log.WithField("env", fmt.Sprintf("%v", os.Environ())).Info("environment")
	log.WithField("kafkaConfig", viper.Get("kafka")).Debug("config")
	handleTerm(cleanup)

	bufSize := viper.GetInt("kafka.consumer.bufferSize")
	messages = make(chan interface{}, bufSize)
	countClient, err := counter.NewClient()
	if err != nil {
		log.WithError(err).Error("error creating counter client")
	}

	go func(msgChannel chan interface{}, counter *counter.Client) {
		for message := range messages {
			log.WithFields(log.Fields{
				"messageType":    fmt.Sprintf("%T", message),
				"messageContent": fmt.Sprintf("%v", message),
			}).Debug("consumed")
			decoded := message.(tmkafka.StringMessage)
			counter.CountEvent(decoded.Value)
		}
		log.Info("finished reading messages")
	}(messages, countClient)

	config := sarama.NewConfig()
	config.Net.DialTimeout = 10 * time.Second

	cConfig := tmkafka.ConsumerConfig{
		BrokerList:      viper.GetStringSlice("kafka.broker"),
		SaramaConfig:    config,
		MessageDecoder:  tmkafka.StringMessageDecoder,
		ConsumerChannel: messages,
	}

	cons, err := tmkafka.NewConsumer(cConfig)
	if err != nil {
		log.WithError(err).Fatal("ConsumerError")

	}
	consumer = cons
	if err := consumer.Start(); err != nil {
		log.WithError(err).Error("ConsumerError")
	}
}

func init() {
	RootCmd.AddCommand(consumeCmd)
}

func cleanup() {
	log.Info("cleanup called")
	log.Info("stopping consumer")
	consumer.Stop()
	log.Info("closing consumer channel")
	close(messages)
}
