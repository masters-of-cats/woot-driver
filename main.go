package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"

	"github.com/codegangsta/cli"
	"github.com/containers/image/types"
	"github.com/julz/wooter"
	"github.com/williammartin/woot/puller"
)

func main() {
	wootfs := cli.NewApp()
	wootfs.Name = "wootfs"
	wootfs.Version = "0.0.1"
	wootfs.Usage = "I am Woot!"

	wootfs.Commands = []cli.Command{
		CreateCommand,
	}

	if err := wootfs.Run(os.Args); err != nil {
		os.Exit(1)
	}
}

var CreateCommand = cli.Command{
	Name:        "create",
	Usage:       "create [options] <image> <id>",
	Description: "Creates a root filesystem for the provided image.",
	Action: func(ctx *cli.Context) error {
		wootStore := "/tmp/woot-store"
		if err := os.MkdirAll(wootStore, 0777); err != nil {
			return err
		}

		cpDriver := wooter.Cp{
			BaseDir: wootStore,
		}
		puller := puller.Puller{
			Driver:        cpDriver,
			SystemContext: createSystemContext(),
		}

		image := ctx.Args().Get(0)
		imageURL, err := url.Parse(image)
		if err != nil {
			return cli.NewExitError(err.Error(), 1)
		}

		id := ctx.Args().Get(1)
		bundle, err := puller.Pull(imageURL, id)
		if err != nil {
			return cli.NewExitError(err.Error(), 1)
		}

		bundleJSON, err := json.Marshal(bundle)
		if err != nil {
			return cli.NewExitError(err.Error(), 1)
		}

		fmt.Println(string(bundleJSON))

		return nil
	},
}

func createSystemContext() *types.SystemContext {
	return &types.SystemContext{
		DockerInsecureSkipTLSVerify: true,
	}
}
