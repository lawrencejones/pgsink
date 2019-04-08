package pg2pubsub

import "errors"

// Message is a parsed pgoutput message received from the replication stream
type Message struct{}

func DecodePGOutput(raw []byte) (Message, error) {
	return Message{}, errors.New("decoding not implemented")
}
