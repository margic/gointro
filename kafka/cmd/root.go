package cmd

import (
	"os"
	"os/signal"
	"strings"
	"syscall"

	log "github.com/Sirupsen/logrus"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "kafka",
	Short: "Example Go",
	Long:  `Sample go code.`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		log.WithError(err).Error("faild to run command")
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./config.yml)")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	}
	viper.AddConfigPath(".")
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.WithField("configFile", viper.ConfigFileUsed()).Info("config file used")

		// setup logging level
		ll := viper.GetString("logging.level")
		l, err := log.ParseLevel(strings.ToUpper(ll))
		if err == nil {
			log.SetLevel(l)
		} else {
			log.WithError(err).Error("error parsing logging level")
		}
		log.Debug("debug logging enabled")
	}
}

func handleTerm(cleanup func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		for sig := range c {
			log.Infof("captured %v, stopping application", sig)
			cleanup()
			os.Exit(1)
		}
	}()
}
