package sinks

import (
	"context"

	"github.com/lawrencejones/pg2sink/pkg/changelog"
)

// AckCallback will acknowledge successful publication of up-to this message. It is not
// guaranteed to be called for any intermediate messages.
type AckCallback func(changelog.Entry)

// Sink is a generic sink destination for a changelog. It will consume entries until
// either an error, or the entries run out.
//
// If the process producing the changelog is long-running, then the AckCallback is used to
// acknowledge successfully writes into the sync. If you to wait for all writes to be
// completely processed to the sync, then wait for Consume to return.
type Sink interface {
	Consume(context.Context, changelog.Changelog, AckCallback) error
}
