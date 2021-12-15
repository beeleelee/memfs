package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/beeleelee/memfs/kvdbfs"
	"github.com/urfave/cli"
)

var fs *kvdbfs.FileSystem

//===========================================================================
// OS Signal Handlers
//===========================================================================

func signalHandler() {
	// Make signal channel and register notifiers for Interrupt and Terminate
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)
	signal.Notify(sigchan, syscall.SIGTERM)

	// Block until we receive a signal on the channel
	<-sigchan

	// Defer the clean exit until the end of the function
	defer os.Exit(0)

	// Shutdown now that we've received the signal
	err := fs.Shutdown()
	if err != nil {
		msg := fmt.Sprintf("shutdown error: %s", err.Error())
		fmt.Println(msg)
		os.Exit(1)
	}
}

func main() {

	app := cli.NewApp()
	app.Name = "kvdbfs"
	app.ArgsUsage = "mount point"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "config, c",
			Usage: "specify a path to the configuration `FILE`",
		},
		cli.StringFlag{
			Name:  "name, N",
			Usage: "specify name of host, uses os hostname by default",
		},
		cli.StringFlag{
			Name:  "level, L",
			Usage: "specify minimum log level, INFO by default",
		},
		cli.BoolFlag{
			Name:  "readonly, R",
			Usage: "set the fs to read only mode, false by default",
		},
	}

	app.Action = runfs
	app.Run(os.Args)

}

func runfs(c *cli.Context) error {

	var err error
	var mountPath string
	var config *kvdbfs.Config

	// Validate the arguments
	if c.NArg() != 1 {
		return cli.NewExitError("please supply the path to the mount point", 1)
	}

	// Get the mount path from the arguments
	mountPath = c.Args()[0]

	// Create the configuration from the passed in file or with defaults
	cpath := c.String("config")
	if config, err = makeConfig(cpath); err != nil {
		return cli.NewExitError(err.Error(), 1)
	}
	fmt.Printf("%v\n", *config)

	// Update the configuration with command line options
	if c.String("name") != "" {
		config.Name = c.String("name")
	}

	if c.String("level") != "" {
		config.LogLevel = c.String("level")
	}

	if c.Bool("readonly") {
		config.ReadOnly = c.Bool("readonly")
	}

	// Create the new file system
	fs, err = kvdbfs.New(mountPath, config)
	if err != nil {
		return err
	}

	// Handle interrupts
	go signalHandler()

	// Run the file system
	if err := fs.Run(); err != nil {
		return cli.NewExitError(err.Error(), 1)
	}

	return nil
}

// Helper function to make the configuration.
func makeConfig(cpath string) (*kvdbfs.Config, error) {
	// Construct configuration from command line options or JSON file.
	config := new(kvdbfs.Config)

	// Load the configuration if a path was passed in.

	if cpath != "" {
		if err := config.Load(cpath); err != nil {
			return nil, err
		}
	} else {
		name, err := os.Hostname()
		if err != nil {
			name = "terp"
		}

		// Add reasonable defaults to the configuration
		config.Name = name
		config.LogLevel = "info"
		config.ReadOnly = false
	}

	return config, nil
}
