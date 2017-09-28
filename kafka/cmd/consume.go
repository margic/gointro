package cmd

import (
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
to connect to a Kafka topic and consume messages from the topic.`,
	Run: func(cmd *cobra.Command, args []string) {
		consume()
	},
}

var consumer *tmkafka.Consumer

func consume() {
	log.Info("consume called")
	log.WithField("kafkaConfig", viper.Get("kafka")).Debug("config")
	handleTerm(cleanup)

	config := sarama.NewConfig()

	cConfig := tmkafka.ConsumerConfig{
		BrokerList:     viper.GetStringSlice("kafka.broker"),
		Config:         config,
		MessageDecoder: tmkafka.StringMessageDecoder,
	}

	cons, err := tmkafka.NewConsumer(cConfig)
	if err != nil {
		log.WithError(err).Error("ConsumerError")
	}
	consumer = cons
	consumer.Start()
}

func init() {
	RootCmd.AddCommand(consumeCmd)
}

func cleanup() {
	log.Info("cleanup called")
	consumer.Stop()
}
