package puller

import (
	"io"
	"net/url"
	"strings"

	_ "github.com/containers/image/docker"
	"github.com/containers/image/image"

	"github.com/containers/image/transports"
	"github.com/containers/image/types"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

type Driver interface {
	Unpack(id, parentID string, blobStream io.Reader) (size int, err error)
	Bundle(id string, parentIDs []string) (spec specs.Spec, err error)
}

type Puller struct {
	Driver        Driver
	SystemContext *types.SystemContext
}

func (p *Puller) Pull(imageURL *url.URL, id string) (specs.Spec, error) {
	imageSource, sourcedImage, err := getSourceAndImage(imageURL, p.SystemContext)
	if err != nil {
		return specs.Spec{}, nil
	}
	defer func() {
		imageSource.Close()
		sourcedImage.Close()
	}()

	digests, err := p.UnpackLayers(imageSource, sourcedImage.LayerInfos())
	if err != nil {
		return specs.Spec{}, err
	}

	return p.Driver.Bundle(id, digests)
}

func (p *Puller) UnpackLayers(imageSource types.ImageSource, layers []types.BlobInfo) ([]string, error) {
	digests := []string{}
	for _, layer := range layers {
		blobStream, err := getBlobStream(imageSource, layer)
		if err != nil {
			return []string{}, err
		}
		defer blobStream.Close()

		parsedDigest := strings.Split(layer.Digest.String(), ":")[1]
		_, err = p.Driver.Unpack(parsedDigest, last(digests), blobStream)
		if err != nil {
			return []string{}, err
		}

		digests = append(digests, parsedDigest)
	}

	return digests, nil
}

func getSourceAndImage(imageURL *url.URL, systemContext *types.SystemContext) (types.ImageSource, types.Image, error) {
	ref, err := reference(imageURL)
	if err != nil {
		return nil, nil, err
	}

	imageSource, err := ref.NewImageSource(systemContext)
	if err != nil {
		return nil, nil, err
	}

	sourcedImage, err := image.FromSource(imageSource)
	if err != nil {
		imageSource.Close()
		return nil, nil, err
	}

	return imageSource, sourcedImage, nil
}

func getBlobStream(imageSource types.ImageSource, layer types.BlobInfo) (io.ReadCloser, error) {
	blobStream, _, err := imageSource.GetBlob(layer)
	if err != nil {
		return nil, err
	}

	return blobStream, err
}

func reference(imageURL *url.URL) (types.ImageReference, error) {
	transport := transports.Get(imageURL.Scheme)

	refString := "/"
	if imageURL.Host != "" {
		refString += "/" + imageURL.Host
	}
	refString += imageURL.Path

	ref, err := transport.ParseReference(refString)
	if err != nil {
		return nil, err
	}

	return ref, nil
}

func last(digests []string) string {
	return digests[len(digests)-1]
}
