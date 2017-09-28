package cmd

import (
	"fmt"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
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
	log.WithField("kafkaConfig", viper.Get("kafka")).Debug("config")
	handleTerm(cleanup)

	bufSize := viper.GetInt("kafka.consumer.bufferSize")
	messages = make(chan interface{}, bufSize)

	go func(msgChannel chan interface{}) {
		for message := range messages {
			log.WithFields(log.Fields{
				"messageType":    fmt.Sprintf("%T", message),
				"messageContent": fmt.Sprintf("%v", message),
			}).Debug("consumed")
		}
		log.Info("finished reading messages")
	}(messages)

	config := sarama.NewConfig()

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
