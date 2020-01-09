package serialize

import (
	"github.com/lawrencejones/pg2sink/pkg/changelog"
)

// DefaultSerializer is the serializer of choice, unless it has been overriden
var DefaultSerializer = &JSON{Pretty: true}

// Serializer defines the required interface for all changelog serializers.
type Serializer interface {
	Register(*changelog.Schema) []byte
	Marshal(*changelog.Modification) ([]byte, error)
}
