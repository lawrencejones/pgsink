package subscription

import "github.com/lawrencejones/pgsink/pkg/logical"

// SequencedMessage wraps logical messages with the begin message associated with the
// transaction that the message was contained within.
type SequencedMessage struct {
	Begin    logical.Begin
	Sequence uint64
	Entry    logical.Message // the original message
}

// Sequence receives a channel containing logical replication messages and produces
// a channel which annotates each message with commit information. Sequenced structs can
// be tracked back to a specific LSN, and logically ordered by sequence number, ensuring
// we can detect the authoriative row value even if the same row is updated many times
// within the same transaction.
//
// This will almost always be used like so:
//
//     Sequence(stream.Messages())
//
// Where stream is an active Stream.
func Sequence(messages <-chan interface{}) <-chan SequencedMessage {
	output := make(chan SequencedMessage)

	go func() {
		var currentTransaction *logical.Begin
		var sequence uint64

		for msg := range messages {
			switch msg := msg.(type) {
			case *logical.Begin:
				currentTransaction = msg
				sequence = 0
			case *logical.Commit:
				currentTransaction = nil
			default:
				sequence++
				output <- SequencedMessage{
					Begin:    *currentTransaction,
					Sequence: sequence,
					Entry:    msg,
				}
			}
		}

		close(output)
	}()

	return output
}
