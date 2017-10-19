package wooter

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

const VolumesDir string = "volumes"
const DiffsDir string = "diffs"

type Cp struct {
	BaseDir string
}

func (c Cp) Unpack(id, parentID string, tar io.Reader) (size int, err error) {
	dest := filepath.Join(c.BaseDir, VolumesDir, id)
	if err := os.MkdirAll(dest, 0700); err != nil {
		return 0, err
	}

	if parentID != "" {
		cpCmd := exec.Command("sh", "-c", fmt.Sprintf("cp -r %s/* %s", filepath.Join(c.BaseDir, VolumesDir, parentID), dest+"/"))
		if out, err := cpCmd.CombinedOutput(); err != nil {
			return 0, fmt.Errorf("%s: %s", string(out), err)
		}

		fmt.Printf(c.BaseDir)
	}

	tarCmd := exec.Command("tar", "-x", "-C", dest)
	tarCmd.Stdin = tar
	if err := tarCmd.Run(); err != nil {
		return 0, err
	}

	return 0, nil
}

func (c Cp) Bundle(id string, parentIds []string) (specs.Spec, error) {
	dest := filepath.Join(c.BaseDir, DiffsDir, id)
	if err := os.MkdirAll(dest, 0700); err != nil {
		return specs.Spec{}, err
	}

	cpCmd := exec.Command("sh", "-c", fmt.Sprintf("cp -r %s/* %s", filepath.Join(c.BaseDir, VolumesDir, parentIds[len(parentIds)-1]), dest+"/"))
	if out, err := cpCmd.CombinedOutput(); err != nil {
		return specs.Spec{}, fmt.Errorf("%s: %s", string(out), err)
	}

	return specs.Spec{
		Root: &specs.Root{
			Path: filepath.Join(c.BaseDir, id),
		},
	}, nil
}

func (c Cp) Exists(id string) bool {
	_, err := os.Stat(filepath.Join(c.BaseDir, id))
	return err == nil
}
