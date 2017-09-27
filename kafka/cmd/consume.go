// Copyright © 2017 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	log "github.com/Sirupsen/logrus"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	tmkafka "github.com/margic/gointro/kafka/kafka"
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

	kcfg := &kafka.ConfigMap{
		"bootstrap.servers":               viper.GetString("kafka.broker"),
		"group.id":                        viper.GetString("kafka.group"),
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": "earliest"},
	}

	cons, err := tmkafka.NewConsumer(kcfg)
	if err != nil {
		log.WithError(err).Error("error creating kafka consumer")
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
