package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig"
	"github.com/jackc/pgtype"
	"github.com/lawrencejones/pgsink/pkg/decode"
)

/*
pgconn and pgtype have a good list of Postgres type parsers, and map them against name and
OID. This file uses the pgx implementation to generate a list of supported mappings for
pgsink.

If a type is supported, that means we can parse it into a native format. Postgres text
arrays, for example, are parsed into Golang []string.

Decoders can be configured to a type as a fallback mechanism, such as parsing the plain
text format of a Postgres column in the situation where it would otherwise be unreadable.
Whether this is desirable will depend on how pgsink is being used, and is why it should be
configurable outside of the mapping package.

Run go generate ./... to invoke this file, and modify the output by adjusting the
configuration struct.
*/

type TemplatedTypeMapping struct {
	Name    string
	OID     uint32
	Scanner string
	Empty   string
}

type Config struct {
	Unsupported []string
	Templates   map[string]TemplatedTypeMapping
}

var cfg = Config{
	Unsupported: []string{
		"tid",
		"point",
		"lseg",
		"path",
		"box",
		"polygon",
		"line",
		"_cidr",
		"circle",
		"macaddr",
		"inet",
		"_bytea",
		"_bpchar",
		"aclitem",
		"_aclitem",
		"_inet",
		"bpchar",
		"date",
		"interval",
		"_numeric",
		"bit",
		"varbit",
		"numeric",
		"uuid",
		"_uuid",
		"int4range",
		"numrange",
		"tsrange",
		"tstzrange",
		"daterange",
		"int8range",
	},
	Templates: map[string]TemplatedTypeMapping{
		"bool": {
			Empty: "new(bool)",
		},
		"bytea": {
			Empty: "new([]uint8)",
		},
		"name": {
			Empty: "new(string)",
		},
		"int8": {
			Empty: "new(int64)",
		},
		"int2": {
			Empty: "new(int16)",
		},
		"int4": {
			Empty: "new(int32)",
		},
		"text": {
			Empty: "new(string)",
		},
		"oid": {
			Empty: "new(uint32)",
		},
		"xid": {
			Empty: "new(uint32)",
		},
		"cid": {
			Empty: "new(uint32)",
		},
		"json": {
			Empty: "new([]byte)",
		},
		"float4": {
			Empty: "new(float32)",
		},
		"float8": {
			Empty: "new(float64)",
		},
		"_bool": {
			Empty: "new([]bool)",
		},
		"_int2": {
			Empty: "new([]int16)",
		},
		"_int4": {
			Empty: "new([]int64)",
		},
		"_text": {
			Empty: "new([]string)",
		},
		"_varchar": {
			Empty: "new([]string)",
		},
		"_int8": {
			Empty: "new([]int64)",
		},
		"_float4": {
			Empty: "new([]float32)",
		},
		"_float8": {
			Empty: "new([]float64)",
		},
		"varchar": {
			Empty: "new(string)",
		},
		"date": {
			Empty: "new(time.Time)",
		},
		"time": {
			Empty: "new(int64)",
		},
		"timestamp": {
			Empty: "new(time.Time)",
		},
		"_timestamp": {
			Empty: "new([]time.Time)",
		},
		"_date": {
			Empty: "new([]time.Time)",
		},
		"timestamptz": {
			Empty: "new(time.Time)",
		},
		"_timestamptz": {
			Empty: "new([]time.Time)",
		},
		"jsonb": {
			Empty: "new(string)",
		},
	},
}

var sourceTemplate = template.Must(template.New("mappings.go").Funcs(sprig.TxtFuncMap()).Parse(`package mappings

import (
	"github.com/lawrencejones/pgsink/pkg/decode"
)

// Auto-generated by pkg/decode/cmd/generate_mappings.go

func Each(do func(name string, mapping decode.TypeMapping)) {
	for _, mapping := range Mappings {
		do(mapping.Name, mapping)
	}
}

// Mappings are the types we support natively from Postgres, with proper parsing
var Mappings = []decode.TypeMapping{
{{- range .Supported }}
	{
		Name: {{ .Name | quote }},
		OID: {{ .OID }},
		Scanner: {{ .Scanner }},
		Empty: {{ .Empty }},
	},
{{- end }}
}

// Unsupported is a list of types that Postgres normally supports that we choose not to,
// as we can't parse them into a sensibly common Golang types.
var Unsupported = []decode.TypeMapping{
{{- range .Unsupported }}
	{
		Name: {{ .Name | quote }},
		OID: {{ .OID }},
	},
{{- end }}
}
`))

func buildMappings(cfg Config) (supported, unsupported []TemplatedTypeMapping) {
	ci := pgtype.NewConnInfo()

nextDataType:
	for oid := uint32(1); oid < 4096; oid++ {
		dataType, found := ci.DataTypeForOID(oid)
		if !found {
			continue // no type registered for oid
		}

		valueScanner, ok := dataType.Value.(decode.ValueScanner)
		if !ok {
			continue // we only want scanners that can encode text
		}

		for _, name := range cfg.Unsupported {
			if name == dataType.Name {
				unsupported = append(unsupported, TemplatedTypeMapping{
					Name: dataType.Name,
					OID:  dataType.OID,
				})

				continue nextDataType
			}
		}

		template, found := cfg.Templates[dataType.Name]
		if !found {
			log.Fatalf("no template set for %s", dataType.Name)
		}

		supported = append(supported, TemplatedTypeMapping{
			Name:    dataType.Name,
			OID:     dataType.OID,
			Scanner: fmt.Sprintf("&%s{}", reflect.TypeOf(valueScanner).Elem().String()),
			Empty:   template.Empty,
		})
	}

	return
}

func main() {
	supported, unsupported := buildMappings(cfg)

	var buffer = bytes.NewBuffer([]byte{})
	err := sourceTemplate.Execute(buffer, struct {
		Supported, Unsupported []TemplatedTypeMapping
	}{supported, unsupported})
	if err != nil {
		panic(err.Error())
	}

	cmd := exec.Command("goimports")
	cmd.Stderr = os.Stderr
	cmd.Stdin = strings.NewReader(buffer.String())

	output, err := cmd.Output()
	if err != nil {
		log.Fatalf(err.Error())
	}

	if len(os.Args) > 2 {
		log.Printf("outputting to %s", os.Args[2])
		if err := ioutil.WriteFile(os.Args[2], output, 0644); err != nil {
			log.Fatalf(err.Error())
		}
	} else {
		log.Printf("outputting to stdout")
		fmt.Println(string(output))
	}
}
