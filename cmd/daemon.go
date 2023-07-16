package cmd

import (
	"context"
	"fmt"
	"github.com/application-research/edge-ur/api"
	"github.com/application-research/edge-ur/config"
	"github.com/application-research/edge-ur/core"
	"github.com/application-research/edge-ur/jobs"
	"github.com/application-research/edge-ur/utils"
	"github.com/urfave/cli/v2"
	"runtime"
	"strconv"
)

func DaemonCmd(cfg *config.EdgeConfig) []*cli.Command {
	// add a command to run API node
	var daemonCommands []*cli.Command

	daemonCmd := &cli.Command{
		Name:  "daemon",
		Usage: "EdgeURID gateway daemon that allows users to upload and download data to/from the Filecoin network.",

		Flags: []cli.Flag{
			&cli.StringFlag{
				Name: "repo",
			},
			&cli.StringFlag{
				Name: "port",
			},
		},

		Action: func(c *cli.Context) error {

			fmt.Println("OS:", runtime.GOOS)
			fmt.Println("Architecture:", runtime.GOARCH)
			fmt.Println("Hostname:", core.GetHostname())

			ip, err := core.GetPublicIP()
			if err != nil {
				fmt.Println("Error getting public IP:", err)
			}
			fmt.Println("Public IP:", ip)
			fmt.Println(utils.Blue + "Starting Edge daemon..." + utils.Reset)

			repo := c.String("repo")
			port := c.String("port")
			if repo != "" {
				cfg.Node.Repo = repo
			}
			if port != "" {
				portInt, err := strconv.Atoi(port)
				if err != nil {
					fmt.Println("Error parsing port:", err)
				}
				cfg.Node.Port = portInt
			}

			fmt.Println(utils.Blue + "Setting up the Edge node... " + utils.Reset)
			ln, err := core.NewEdgeNode(context.Background(), *cfg)
			if err != nil {
				return err
			}
			fmt.Println(utils.Blue + "Setting up the Edge node... Done" + utils.Reset)

			core.ScanHostComputeResources(ln, cfg.Node.Repo)

			//	launch the jobs
			go runProcessors(ln)

			// launch the API node
			fmt.Printf(`
 _______    ________   ________   _______                    ___  ___   ________     
|\  ___ \  |\   ___ \ |\   ____\ |\  ___ \                  |\  \|\  \ |\   __  \    
\ \   __/| \ \  \_|\ \\ \  \___| \ \   __/|    ____________ \ \  \\\  \\ \  \|\  \   
 \ \  \_|/__\ \  \ \\ \\ \  \  ___\ \  \_|/__ |\____________\\ \  \\\  \\ \   _  _\  
  \ \  \_|\ \\ \  \_\\ \\ \  \|\  \\ \  \_|\ \\|____________| \ \  \\\  \\ \  \\  \| 
   \ \_______\\ \_______\\ \_______\\ \_______\                \ \_______\\ \__\\ _\ 
    \|_______| \|_______| \|_______| \|_______|                 \|_______| \|__|\|__|
`)

			// default tagging.
			api.GetDefaultTagPolicy(ln)
			fmt.Println("Starting API server")
			api.InitializeEchoRouterConfig(ln)
			api.LoopForever()

			return nil
		},
	}

	// add commands.
	daemonCommands = append(daemonCommands, daemonCmd)

	return daemonCommands

}

func runProcessors(ln *core.LightNode) {
	job := jobs.CreateNewDispatcher()
	job.AddJob(jobs.NewBucketAggregator(ln))
	job.Start(5)
}
