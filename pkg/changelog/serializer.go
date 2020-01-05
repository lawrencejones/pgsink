package changelog

import "encoding/json"

// DefaultSerializer is the serializer of choice, unless it has been overriden
var DefaultSerializer = &JSONSerializer{Pretty: true}

// Serializer defines the required interface for all changelog serializers.
type Serializer interface {
	Register(*Schema) []byte
	Marshal(*Modification) ([]byte, error)
}

var _ Serializer = &JSONSerializer{}

type JSONSerializer struct {
	Pretty bool // whether to pretty-print the output
}

func (s *JSONSerializer) Register(schema *Schema) []byte {
	bytes, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		panic("could not marshal json schema, this should never happen")
	}

	return bytes
}

func (s *JSONSerializer) Marshal(modification *Modification) ([]byte, error) {
	if s.Pretty {
		return json.MarshalIndent(modification, "", "  ")
	}

	return json.Marshal(modification)
}
