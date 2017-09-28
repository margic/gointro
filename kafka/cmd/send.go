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
		producer, err := createProducer()
		if err != nil {
			log.WithError(err).Error("error creatint producer")
		}
		defer producer.MessageProducer.Close()
		send(producer)
	},
}

func init() {
	RootCmd.AddCommand(sendCmd)

	sendCmd.Flags().StringP("message", "m", "Default Test Message", "message to send to the topic override default in kafka.message")
	viper.BindPFlag("kafka.message", sendCmd.Flags().Lookup("message"))
}

func createProducer() (*tmkafka.Producer, error) {
	log.Debug("creating producer")
	config := sarama.NewConfig()

	config.Producer.Return.Successes = true
	config.Producer.Compression = sarama.CompressionSnappy

	pConfig := tmkafka.ProducerConfig{
		BrokerList: viper.GetStringSlice("kafka.broker"),
		Config:     config,
	}

	producer, err := tmkafka.NewProducer(pConfig)
	if err != nil {
		return nil, err
	}
	return producer, nil
}

func send(producer *tmkafka.Producer) {
	log.Debug("send called")
	msg := &tmkafka.StringMessage{
		Value: viper.GetString("kafka.message"),
	}
	err := producer.Send(msg)
	if err != nil {
		log.WithError(err).Error("SendError")
	}
}
