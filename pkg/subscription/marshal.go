package subscription

import (
	"reflect"

	"github.com/lawrencejones/pgsink/pkg/decode"
	"github.com/lawrencejones/pgsink/pkg/logical"
)

// MarshalTuple generates a map[string]interface{} from a tuple, using the column type
// information in the relation to find appropriate scanners in the decoder.
//
// This is the function that defines how a logical message is encoded into a changelog
// entry. It's important that it matches the behaviour of the import code, to prevent
// imported rows from being represented differently to those that came via the logical
// stream.
func MarshalTuple(decoder decode.Decoder, relation *logical.Relation, tuple []logical.Element) (map[string]interface{}, error) {
	// TODO: Should we be ignoring this? I haven't investigated why this happens, though it
	// appears to have little effect on correctness.
	if len(relation.Columns) != len(tuple) {
		return nil, nil
	}

	row := map[string]interface{}{}
	for idx, column := range relation.Columns {
		scanner, dest, err := decoder.ScannerFor(column.Type)
		if err != nil {
			return nil, err
		}

		if tuple[idx].Type != 'n' {
			if err := scanner.Scan(tuple[idx].Value); err != nil {
				return nil, err
			}
		}

		if err := scanner.AssignTo(dest); err != nil {
			return nil, err
		}

		// Dereference the value we get from our destination, to remove double pointer-ing
		row[column.Name] = reflect.ValueOf(dest).Elem().Interface()
	}

	return row, nil
}
