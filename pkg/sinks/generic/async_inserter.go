package generic

import (
	"context"
	"sync"

	"github.com/lawrencejones/pg2sink/pkg/changelog"
)

type AsyncInserter interface {
	// Insert has the same signature as the synchronous Inserter, but returns an
	// InsertResult that will be fulfilled at some later time.
	Insert(context.Context, []*changelog.Modification) InsertResult

	// Flush is called to force a write of buffered chanelog modifications to the underlying
	// inserter. The returned result resolves with the sum of rows inserted, and the highest
	// LSN successfully written, if any that were flushed had an associated LSN.
	Flush(context.Context) InsertResult
}

type asyncInserter struct {
	inserter Inserter
	result   InsertResult
	sync.Mutex
}

// WrapAsync converts a synchronous inserter to the async contract
func WrapAsync(i Inserter) AsyncInserter {
	return &asyncInserter{
		inserter: i,
		result:   EmptyInsertResult,
	}
}

// Insert wraps our synchronous inserter to make it compatible with the async contract.
// Whenever we create the result, we fold it against the pending result of other inserts,
// ensure Flush() takes this insertion into account.
//
// The insert result returned will resolve when this specific insertion is complete, not
// including all pending operations.
func (i *asyncInserter) Insert(ctx context.Context, modifications []*changelog.Modification) InsertResult {
	i.Lock()
	defer i.Unlock()

	result := NewInsertResult()
	i.result = i.result.Fold(result) // combine this insert with all pending
	go func() {
		result.Resolve(i.inserter.Insert(ctx, modifications))
	}()

	return result
}

// Flush returns the current in-flight insertion results, resetting the in-flight result
func (i *asyncInserter) Flush(ctx context.Context) InsertResult {
	i.Lock()
	defer i.Unlock()

	result := i.result
	i.result = EmptyInsertResult

	return result
}
