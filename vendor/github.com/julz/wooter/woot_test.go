package wooter_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/julz/wooter"
)

func TestWootASingleLayer(t *testing.T) {
	mytar, err := os.Open("mytar.tar")
	if err != nil {
		t.Fatal("open mytar", err)
	}

	dir, err := ioutil.TempDir("", "woot")
	if err != nil {
		t.Fatal("tmpdir", err)
	}

	w := wooter.Cp{
		BaseDir: dir,
	}

	if _, err := w.Unpack("my-id", "", mytar); err != nil {
		t.Errorf("expected unpack to succeed but got error %s", err)
	}

	bundle, err := w.Bundle("my-id", []string{""})
	if err != nil {
		t.Errorf("expected creating bundle to succeed but got error %s", err)
	}

	if _, err = os.Stat(filepath.Join(bundle.Root.Path)); err != nil {
		t.Errorf("expected root path to inside the returned bundle to exist")
	}

	if _, err = os.Stat(filepath.Join(bundle.Root.Path, "foo", "bar")); err != nil {
		t.Errorf("expected foo/bar to exist inside the returned root")
	}
}

func TestExistingAWoot(t *testing.T) {
	mytar, err := os.Open("mytar.tar")
	if err != nil {
		t.Fatal("open mytar", err)
	}

	dir, err := ioutil.TempDir("", "woot")
	if err != nil {
		t.Fatal("tmpdir", err)
	}

	w := wooter.Cp{
		BaseDir: dir,
	}

	if w.Exists("my-id") {
		t.Error("expected my-id not to exist before unpacking")
	}

	if _, err := w.Unpack("my-id", "", mytar); err != nil {
		t.Errorf("expected unpack to succeed but got error %s", err)
	}

	if w.Exists("my-id") != true {
		t.Error("expected my-id to exist after unpacking")
	}
}

func TestWootingWithAParentWoot(t *testing.T) {
	mytar, err := os.Open("mytar.tar")
	if err != nil {
		t.Fatal("open mytar", err)
	}

	myparenttar, err := os.Open("myparent.tar")
	if err != nil {
		t.Fatal("open parent tar", err)
	}

	dir, err := ioutil.TempDir("", "woot")
	if err != nil {
		t.Fatal("tmpdir", err)
	}

	w := wooter.Cp{
		BaseDir: dir,
	}

	if _, err := w.Unpack("my-parent-id", "", myparenttar); err != nil {
		t.Errorf("expected unpack to succeed but got error %s", err)
	}

	if _, err := w.Unpack("my-id", "my-parent-id", mytar); err != nil {
		t.Errorf("expected unpack to succeed but got error %s", err)
	}

	bundle, err := w.Bundle("my-id", []string{""})
	if err != nil {
		t.Errorf("expected creating bundle to succeed but got error %s", err)
	}

	if _, err = os.Stat(filepath.Join(bundle.Root.Path)); err != nil {
		t.Errorf("expected root path to inside the returned bundle to exist")
	}

	if _, err = os.Stat(filepath.Join(bundle.Root.Path, "foo", "bar")); err != nil {
		t.Errorf("expected foo/bar to exist inside the returned root")
	}

	if _, err = os.Stat(filepath.Join(bundle.Root.Path, "i", "am", "parent")); err != nil {
		t.Errorf("expected i/am/parent to exist inside the returned root")
	}
}
