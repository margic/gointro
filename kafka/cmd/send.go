package cmd

import (
	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/margic/gointro/kafka/tmkafka"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// sendCmd represents the send command
var sendCmd = &cobra.Command{
	Use:   "send",
	Short: "Send a message to the broker",
	Long: `Will send a default message from the config.yml file or 
		a custom message if the --message flag is used.`,
	Run: func(cmd *cobra.Command, args []string) {
		send()
	},
}

func init() {
	RootCmd.AddCommand(sendCmd)

}

func send() {
	log.Debug("send called")
	config := sarama.NewConfig()

	config.Producer.Return.Successes = true
	config.Producer.Compression = sarama.CompressionSnappy

	pConfig := tmkafka.ProducerConfig{
		BrokerList: viper.GetStringSlice("kafka.broker"),
		Config:     config,
	}

	producer, err := tmkafka.NewProducer(pConfig)
	if err != nil {
		log.WithError(err).Error("ProducerError")
	}

	msg := &tmkafka.StringMessage{
		Value: "TEST",
	}
	err = producer.Send(msg)
	if err != nil {
		log.WithError(err).Error("SendError")
	}
}
