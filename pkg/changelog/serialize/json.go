package serialize

import (
	"encoding/json"

	"github.com/lawrencejones/pg2sink/pkg/changelog"
)

var _ Serializer = &JSON{}

type JSON struct {
	Pretty bool // whether to pretty-print the output
}

func (s *JSON) Register(schema *changelog.Schema) []byte {
	bytes, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		panic("could not marshal json schema, this should never happen")
	}

	return bytes
}

func (s *JSON) Marshal(modification *changelog.Modification) ([]byte, error) {
	if s.Pretty {
		return json.MarshalIndent(modification, "", "  ")
	}

	return json.Marshal(modification)
}
