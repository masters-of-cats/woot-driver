package puller

import (
	"io"
	"net/url"
	"strings"

	_ "github.com/containers/image/docker"

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
	ref, err := reference(imageURL)
	if err != nil {
		return specs.Spec{}, err
	}

	imageWithCachedManifest, err := p.getImageWithCachedManifest(ref)
	if err != nil {
		return specs.Spec{}, err
	}
	defer imageWithCachedManifest.Close()

	imageSource, err := ref.NewImageSource(p.SystemContext)
	if err != nil {
		return specs.Spec{}, err
	}
	defer imageSource.Close()

	var digests []string = []string{}
	var previousDigest string
	for _, layer := range imageWithCachedManifest.LayerInfos() {
		blobStream, err := getBlobStream(imageSource, layer)
		defer blobStream.Close()
		if err != nil {
			return specs.Spec{}, err
		}

		parsedDigest := strings.Split(layer.Digest.String(), ":")[1]
		_, err = p.Driver.Unpack(parsedDigest, previousDigest, blobStream)
		if err != nil {
			return specs.Spec{}, err
		}

		digests = append(digests, parsedDigest)
		previousDigest = parsedDigest
	}

	return p.Driver.Bundle(id, digests)
}

func getBlobStream(imageSource types.ImageSource, layer types.BlobInfo) (io.ReadCloser, error) {
	blobStream, _, err := imageSource.GetBlob(layer)
	if err != nil {
		return nil, err
	}

	return blobStream, err
}

func (p Puller) getImageWithCachedManifest(ref types.ImageReference) (types.Image, error) {
	img, err := ref.NewImage(p.SystemContext)
	if err != nil {
		return nil, err
	}

	_, _, err = img.Manifest()
	if err != nil {
		return nil, err
	}

	return img, nil
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
