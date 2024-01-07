package cli

import (
	"errors"
	"github.com/charmbracelet/log"
	"github.com/spf13/cobra"
	"github.com/usedatabrew/blink/public/server"
	"github.com/usedatabrew/blink/public/stream"
	"os"
)

var configFileLocation string
var enableHttpServer bool

var cmdStart = &cobra.Command{
	Use:   "start",
	Short: "Starts blink instance reading with default blink.yaml config",
	Long:  `Provide --config config.yaml to specify the location of the config file before starting`,
	Run: func(cmd *cobra.Command, args []string) {
		if _, err := os.Stat(configFileLocation); errors.Is(err, os.ErrNotExist) {
			log.WithPrefix("blink-cli").Fatal("Config file doesn't exist", "file", configFileLocation)
		}

		configFile, err := os.ReadFile(configFileLocation)
		if err != nil {
			panic(err)
		}

		var streamService *stream.Stream
		serviceConfiguration, err := stream.ReadInitConfigFromYaml(configFile)
		streamService, err = stream.InitFromConfig(serviceConfiguration)
		if err != nil {
			panic(err)
		}

		if enableHttpServer {
			go server.CreateAndStartHttpServer(streamService.GetContext(), true)
		}

		if err = streamService.Start(); err != nil {
			panic(err)
		}
	},
}

var rootCmd = &cobra.Command{}

func init() {
	cmdStart.Flags().StringVarP(&configFileLocation, "config", "c", "blink.yaml", "Specify the location of the configuration file")
	cmdStart.Flags().BoolVarP(&enableHttpServer, "http-server", "s", true, "Define if you need blink to start http server with prometheus metrics exporter")
}

func Start() {

	rootCmd.AddCommand(cmdStart)
	err := rootCmd.Execute()
	if err != nil {
		panic(err)
	}
}
