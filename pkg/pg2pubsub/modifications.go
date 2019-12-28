package pg2pubsub

import (
	"sync"
	"time"

	kitlog "github.com/go-kit/kit/log"
)

// BuildModifications streams committed messages and marshals them into Modifications,
// making use of the provided schema registry. Each Modification has Before/After fields
// populated with the native Golang representation of the type- serialization takes place
// later.
//
// Marshalling modifications can be expensive, as it requires decoding the textual
// representation of each tuple into Golang types. Specify a higher workerCount to
// parallelise the work.
//
// NB. We expect only Insert, Update or Delete messages down the commits channel.
func BuildModifications(logger kitlog.Logger, registry *Registry, commits <-chan Committed, workerCount int) <-chan Modification {
	modifications := make(chan Modification)
	var wg sync.WaitGroup

	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for committed := range commits {
				modifications <- registry.Marshal(committed)
			}

		}()
	}

	go func() { wg.Wait(); close(modifications) }()

	return modifications
}

type Modification struct {
	Timestamp time.Time   `json:"timestamp"`
	LSN       uint64      `json:"lsn"`
	Sequence  uint64      `json:"sequence"`
	Namespace string      `json:"namespace"`
	Name      string      `json:"name"`
	Before    interface{} `json:"before"`
	After     interface{} `json:"after"`
}
