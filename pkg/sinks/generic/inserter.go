package generic

import (
	"context"
	"sync"

	"github.com/lawrencejones/pgsink/pkg/changelog"
)

// Inserter provides a synchronous interface around inserting data into a sink
type Inserter interface {
	// Insert receives changelog modifications to insert into a table. It returns the count
	// of rows inserted, and the highest non-nil LSN from the batch of modification
	// confirmed to be written to the sink
	Insert(context.Context, []*changelog.Modification) (count int, lsn *uint64, err error)
}

// MemoryInserter is a reference implementation of an inserter, storing modifications in
// an in-memory buffer. It satisfies all requirements of an inserter, including
// race-safety.
//
// Beyond offering a useful reference implementation, this can be used for testing generic
// inserter logic without being coupled to an actual backend.
type MemoryInserter struct {
	batches [][]*changelog.Modification
	sync.Mutex
}

func NewMemoryInserter() *MemoryInserter {
	return &MemoryInserter{
		batches: [][]*changelog.Modification{},
	}
}

func (i *MemoryInserter) Insert(ctx context.Context, modifications []*changelog.Modification) (count int, lsn *uint64, err error) {
	i.Lock()
	defer i.Unlock()

	for _, m := range modifications {
		select {
		case <-ctx.Done():
			return count, lsn, ctx.Err()
		default:
		}

		count += 1

		// Update the greatest LSN, if this modification is marked with one
		if m.LSN != nil {
			if lsn == nil || *m.LSN > *lsn {
				lsn = m.LSN
			}
		}
	}

	i.batches = append(i.batches, modifications)

	return
}

func (i *MemoryInserter) Batches() [][]*changelog.Modification {
	i.Lock()
	defer i.Unlock()

	return append([][]*changelog.Modification(nil), i.batches...)
}

func (i *MemoryInserter) Store() []*changelog.Modification {
	i.Lock()
	defer i.Unlock()

	all := []*changelog.Modification{}
	for _, batch := range i.batches {
		all = append(all, batch...)
	}

	return all
}
