package decode_test

import (
	"context"
	"fmt"
	"time"

	"github.com/lawrencejones/pgsink/internal/dbtest"
	"github.com/lawrencejones/pgsink/pkg/decode"
	"github.com/lawrencejones/pgsink/pkg/decode/gen/mappings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/types"
)

// The aim of these tests is to exercise all the registered type mappings, confirming that
// we can decode what Postgres gives us into the type of our choosing.
var _ = Describe("Decoder", func() {
	var (
		decoder decode.Decoder
	)

	JustBeforeEach(func() {
		decoder = decode.NewDecoder(mappings.Mappings)
	})

	mustParseTimestamp := func(val string) time.Time {
		ts, err := time.Parse(time.RFC3339, val)
		if err != nil {
			panic(err.Error())
		}

		return ts
	}

	type testCase struct {
		value string
		match GomegaMatcher
	}
	testCasesByTypeName := map[string][]testCase{
		"bool": {
			{
				value: "true",
				match: Equal(true),
			},
			{
				value: "false",
				match: Equal(false),
			},
		},
		"bytea": {
			{
				value: "decode('DEADBEEF', 'hex')",
				match: Equal([]byte{0xDE, 0xAD, 0xBE, 0xEF}),
			},
		},
		"name": {
			{
				value: "'spartacus'",
				match: Equal("spartacus"),
			},
		},
		"int8": {
			{
				value: "0",
				match: Equal(int64(0)),
			},
			{
				value: "3",
				match: Equal(int64(3)),
			},
			{
				value: "-7",
				match: Equal(int64(-7)),
			},
		},
		"int2": {
			{
				value: "0",
				match: Equal(int16(0)),
			},
			{
				value: "3",
				match: Equal(int16(3)),
			},
			{
				value: "-7",
				match: Equal(int16(-7)),
			},
		},
		"int4": {
			{
				value: "0",
				match: Equal(int32(0)),
			},
			{
				value: "3",
				match: Equal(int32(3)),
			},
			{
				value: "-7",
				match: Equal(int32(-7)),
			},
		},
		"text": {
			{
				value: "'But soft, what light through yonder window breaks?'",
				match: Equal("But soft, what light through yonder window breaks?"),
			},
		},
		"oid": {
			{
				value: "3",
				match: Equal(uint32(3)),
			},
		},
		"xid": {
			{
				value: "'7'::xid",
				match: Equal(uint32(7)),
			},
		},
		"cid": {
			{
				value: "'11'::cid",
				match: Equal(uint32(11)),
			},
		},
		// Both JSON types are taken as strings, for now. We might want to parse it properly
		// in future, but we need support in the sinks to decide how best to handle them.
		"json": {
			{
				value: `'{"It is the east": "and Juliet is the sun"}'`,
				match: MatchJSON(`{
					"It is the east": "and Juliet is the sun"
				}`),
			},
		},
		"jsonb": {
			{
				value: `'{"O happy dagger!": "This is thy sheath"}'`,
				match: MatchJSON(`{
					"O happy dagger!": "This is thy sheath"
				}`),
			},
		},
		"float4": {
			{
				value: "0.0",
				match: Equal(float32(0.0)),
			},
			{
				value: "1.33",
				match: Equal(float32(1.33)),
			},
			{
				value: "-7.1",
				match: Equal(float32(-7.1)),
			},
		},
		"float8": {
			{
				value: "0.0",
				match: Equal(float64(0.0)),
			},
			{
				value: "1.33",
				match: Equal(float64(1.33)),
			},
			{
				value: "-7.1",
				match: Equal(float64(-7.1)),
			},
		},
		"_bool": {
			{
				value: "'{}'",
				match: Equal([]bool{}),
			},
			{
				value: "'{true}'",
				match: Equal([]bool{true}),
			},
			{
				value: "'{false}'",
				match: Equal([]bool{false}),
			},
		},
		"_int2": {
			{
				value: "'{}'",
				match: Equal([]int16{}),
			},
			{
				value: "'{3}'",
				match: Equal([]int16{3}),
			},
			{
				value: "'{-7}'",
				match: Equal([]int16{-7}),
			},
		},
		"_int4": {
			{
				value: "'{}'",
				match: Equal([]int32{}),
			},
			{
				value: "'{3}'",
				match: Equal([]int32{3}),
			},
			{
				value: "'{-7}'",
				match: Equal([]int32{-7}),
			},
		},
		"_int8": {
			{
				value: "'{}'",
				match: Equal([]int64{}),
			},
			{
				value: "'{3}'",
				match: Equal([]int64{3}),
			},
			{
				value: "'{-7}'",
				match: Equal([]int64{-7}),
			},
		},
		"_text": {
			{
				value: "'{}'",
				match: Equal([]string{}),
			},
			{
				value: `'{"I defy you, stars!"}'`,
				match: Equal([]string{
					"I defy you, stars!",
				}),
			},
		},
		"_varchar": {
			{
				value: "'{}'",
				match: Equal([]string{}),
			},
			{
				value: `'{"O me, what fray was here?"}'`,
				match: Equal([]string{
					"O me, what fray was here?",
				}),
			},
		},
		"_float4": {
			{
				value: "'{}'",
				match: Equal([]float32{}),
			},
			{
				value: "'{1.33}'",
				match: Equal([]float32{1.33}),
			},
			{
				value: "'{-7.9}'",
				match: Equal([]float32{-7.9}),
			},
		},
		"_float8": {
			{
				value: "'{}'",
				match: Equal([]float64{}),
			},
			{
				value: "'{1.33}'",
				match: Equal([]float64{1.33}),
			},
			{
				value: "'{-7.9}'",
				match: Equal([]float64{-7.9}),
			},
		},
		"varchar": {
			{
				value: "''",
				match: Equal(""),
			},
			{
				value: "'It seems she hangs upon the cheek of night'",
				match: Equal("It seems she hangs upon the cheek of night"),
			},
		},
		"time": {
			{
				value: "'04:05:06'::time",
				// Time types are weird, as they are based on the turn of the mellenia, but are
				// really an 8 byte microsecond count in PG.
				match: Equal(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Add(
					4*time.Hour + 5*time.Minute + 6*time.Second,
				)),
			},
		},
		"date": {
			{
				value: "'2017-01-08'",
				match: Equal(mustParseTimestamp("2017-01-08T00:00:00Z")),
			},
		},
		"timestamp": {
			{
				value: "'2017-01-08 04:05:06'",
				match: Equal(mustParseTimestamp("2017-01-08T04:05:06Z")),
			},
		},
		"timestamptz": {
			{
				value: "'2017-01-08 04:05:06 +01:00'",
				match: BeTemporally("==", mustParseTimestamp("2017-01-08T04:05:06+01:00")),
			},
		},
		"_timestamptz": {
			{
				value: "'{}'",
				match: Equal([]time.Time{}),
			},
			{
				value: `'{"2017-01-08 04:05:06 +01:00"}'`,
				match: ConsistOf(
					BeTemporally("==", mustParseTimestamp("2017-01-08T04:05:06+01:00")),
				),
			},
		},
		"_timestamp": {
			{
				value: "'{}'",
				match: Equal([]time.Time{}),
			},
			{
				value: `'{"2017-01-08 04:05:06"}'`,
				match: Equal([]time.Time{
					mustParseTimestamp("2017-01-08T04:05:06Z"),
				}),
			},
		},
		"_date": {
			{
				value: "'{}'",
				match: Equal([]time.Time{}),
			},
		},
	}
	mappings.Each(func(name string, mapping decode.TypeMapping) {
		Describe(fmt.Sprintf("%s (%v)", name, mapping.OID), func() {
			var (
				ctx    context.Context
				cancel func()
			)

			var (
				schema           = "decode_test"
				tableExampleName = "example"
				tableExample     = fmt.Sprintf("%s.%s", schema, tableExampleName)
			)

			db := dbtest.Configure(
				dbtest.WithSchema(schema),
				dbtest.WithTable(schema, tableExampleName, fmt.Sprintf("field %s", mapping.Name)),
			)

			BeforeEach(func() {
				ctx, cancel = db.Setup(context.Background(), 10*time.Second)
			})

			AfterEach(func() {
				cancel()
			})

			// For every type, we need a test case that validates we can decode a null value
			// into a Golang nil.
			entries := []TableEntry{
				Entry("NULL", "NULL", BeNil()),
			}
			for _, testCase := range testCasesByTypeName[mapping.Name] {
				entries = append(entries, Entry(testCase.value, testCase.value, testCase.match))
			}

			// For any types that we don't have non-NULL test cases, create a failing test. This
			// ensures we have test cases for all of our valid mappings.
			if _, ok := testCasesByTypeName[mapping.Name]; !ok {
				It("has non-NULL values", func() {
					Fail("need to provide non-NULL examples for this type")
				})
			}

			DescribeTable("it decodes correct value for",
				func(value string, match GomegaMatcher) {
					db.MustExec(ctx, fmt.Sprintf("insert into %s (field) values (%s);", tableExample, value))

					scanner, dest, err := decoder.ScannerFor(mapping.OID)
					Expect(err).NotTo(HaveOccurred())

					rows, err := db.GetDB().QueryContext(ctx, fmt.Sprintf("select field from %s;", tableExample))
					Expect(err).NotTo(HaveOccurred())

					for rows.Next() {
						err := rows.Scan(scanner)
						Expect(err).NotTo(HaveOccurred())

						err = scanner.AssignTo(dest)
						Expect(err).NotTo(HaveOccurred())

						Expect(decode.Unpack(dest)).To(match)
					}
				},
				entries...,
			)
		})
	})
})
