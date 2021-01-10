package subscription

import (
	"fmt"

	"github.com/lawrencejones/pgsink/pkg/changelog"
	"github.com/lawrencejones/pgsink/pkg/decode"
	"github.com/lawrencejones/pgsink/pkg/logical"

	kitlog "github.com/go-kit/kit/log"
)

// BuildChangelog produces a stream of changelog entries from raw logical messages
// produced by a subscription.
//
// This function is where we translate Postgres data into Golang types, via the decoder,
// and marshal the results into changelog entries.
//
// It currently lacks error reporting, which should be added whenever possible as we can
// fail in many ways. Handling those errors will be key to availability and data
// durability.
func BuildChangelog(logger kitlog.Logger, decoder decode.Decoder, stream *Stream) changelog.Changelog {
	output := make(changelog.Changelog)

	// Logical messages about row changes (insert, update, delete) don't come with schema
	// information. This means we need to lookup the relation to understand how to decode
	// the column value, as the relation has type information for each of the columns.
	//
	// The stream produced by a subscription is ordered, and we are guaranteed to receive a
	// relation message before any row messages for that relation. This allows us to track
	// the most recent relation for a given oid (the unique identifier of a relation) in the
	// relations map, and use it to marshal a row message.
	//
	// This function doesn't do any parallel marshalling, which means we can omit locks
	// around accesses into the relation cache.
	relations := make(map[uint32]*logical.Relation)

	// NB: If this is ever modified to marshal entries in parallel, this will complicate any
	// acknowledgement pipeline. Double check assumptions about acknowledgement order before
	// removing ordering.
	go func() {
		// Sequencing the messages annotates each entry with a timestamp and LSN. Both are
		// important for sinks that support deduplication, or any usecase that requires
		// defining the authoriative row version.
		for msg := range Sequence(stream.Messages()) {
			timestamp, lsn := msg.Begin.Timestamp, msg.Begin.LSN
			switch entry := msg.Entry.(type) {
			case *logical.Relation:
				logger.Log("event", "cache_relation",
					"relation_id", entry.ID,
					"relation", fmt.Sprintf("%s.%s", entry.Namespace, entry.Name),
					"msg", "subscription received a relation, storing in relation cache")
				relations[entry.ID] = entry

				// Build the changelog.Schema and pass it down the channel
				schema := changelog.SchemaFromRelation(timestamp, &lsn, entry)
				output <- changelog.Entry{Schema: &schema}

			case logical.Modification:
				reloid, before, after := entry.GetModification()
				relation, ok := relations[reloid]
				if !ok {
					panic(fmt.Sprintf("no relation found for oid '%v'", reloid))
				}

				modification := &changelog.Modification{
					Timestamp: timestamp,
					Namespace: relation.Namespace,
					Name:      relation.Name,
					LSN:       &lsn,
				}

				// Marshalling can fail in a few ways:
				//
				// 1. Cached relation doesn't match the format of the tuple
				// 2. Invalid data in the tuple
				// 3. Decoder cannot parse a valid column type
				// 4. Lack of support for a column type in the decoder
				//
				// (1) should never happen, as we have assurances that relations will be provided
				// before a tuple of the same type. (2) is a bug in the pgoutput format on the
				// origin server, or corruption over the replication link: retrying the stream is
				// our best bet.
				//
				// For (3), our decoder might be out-of-date for the given Postgres type. Perhaps
				// the binary format changed, or the implementation has a bug? This is not
				// transient, and is unrecoverable.
				//
				// (4) implies we need to tell the decoder to fallback to text for unknown types,
				// or extend the mappings to support this unrecognised type. Not recoverable
				// in-process, but easily added to this project.
				//
				// TODO: Right now we panic. This is a crutch- we should pass this error back, and
				// handle it appropriately: eg, dropping this table from the publication rather
				// than killing the entire process.
				{
					var err error
					if before != nil {
						modification.Before, err = MarshalTuple(decoder, relation, before)
						if err != nil {
							panic(err.Error())
						}
					}
					if after != nil {
						modification.After, err = MarshalTuple(decoder, relation, after)
						if err != nil {
							panic(err.Error())
						}
					}
				}

				// Send it down the pipe!
				output <- changelog.Entry{Modification: modification}

			default:
				// ignore this message type
			}
		}

		close(output)
	}()

	return output
}
