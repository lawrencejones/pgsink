package logical

// Sequenced wraps logical messages with the begin message associated with the transaction
// that the message was contained within, along with a sequence number that can be used to
// order operations within the same transaction.
type Sequenced struct {
	Begin
	Sequence uint64
	Entry    interface{}
}

// Sequence receives a channel containing logical replication messages and produces
// a channel which annotates each message with commit information. Sequenced structs can
// be tracked back to a specific LSN, and logically ordered by sequence number, ensuring
// we can detect the authoriative row value even if the same row is updated many times
// within the same transaction.
//
// This will almost always be used like so:
//
//     Sequence(sub.Received())
//
// Where sub is a Subscription.
func Sequence(operations <-chan interface{}) <-chan Sequenced {
	output := make(chan Sequenced)

	go func() {
		var currentTransaction *Begin
		var sequence uint64

		for msg := range operations {
			switch msg := msg.(type) {
			case *Begin:
				currentTransaction = msg
				sequence = 0
			case *Commit:
				currentTransaction = nil
			default:
				sequence++
				output <- Sequenced{
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
