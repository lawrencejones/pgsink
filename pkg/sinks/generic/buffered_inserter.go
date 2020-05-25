package generic

import (
	"context"
	"sync"

	"github.com/lawrencejones/pgsink/pkg/changelog"
)

type bufferedInserter struct {
	inserter     AsyncInserter
	buffer       []*changelog.Modification
	bufferResult *insertResult
	bufferSize   int
	sync.Mutex
}

// NewBufferedInserter wraps any async inserter with a buffer. When an insertion overflows
// the buffer, the insert is triggered is passed to the underlying inserter. Calling Flush
// will also push buffered inserts into the underlying system.
func NewBufferedInserter(i AsyncInserter, bufferSize int) AsyncInserter {
	return &bufferedInserter{
		inserter:     i,
		buffer:       make([]*changelog.Modification, 0, bufferSize),
		bufferResult: NewInsertResult(),
		bufferSize:   bufferSize,
	}
}

// Insert adds the given modifications to the buffer, with any overflow being chunked into
// BufferSize batches and pushed into the underlying inserter. The result resolves only
// once all modifications in the given batch have been inserted.
func (i *bufferedInserter) Insert(ctx context.Context, modifications []*changelog.Modification) InsertResult {
	return i.insert(ctx, modifications, false)
}

func (i *bufferedInserter) insert(ctx context.Context, modifications []*changelog.Modification, overflowAll bool) InsertResult {
	i.Lock()
	defer i.Unlock()

	hadPreviouslyBuffered := len(i.buffer) > 0
	overflowBatches := i.enqueueAndOverflow(modifications, overflowAll)
	if len(overflowBatches) == 0 {
		return i.bufferResult
	}

	// This is the result we've been returning for previously buffered inserts. We need to
	// resolve this result when our first overflow batch is inserted, as that guarantees the
	// previous caller buffered inserts have finally been processed.
	bufferResult := i.bufferResult

	// Insert all overflow batches. This will include the insertion of modifications we had
	// previously buffered, and may not take into account any modifications we've buffered
	// during this call.
	result := EmptyInsertResult
	for idx, batch := range overflowBatches {
		result = result.Fold(i.inserter.Insert(ctx, batch))

		if idx == 0 && hadPreviouslyBuffered {
			bufferResult.ResolveFrom(ctx, result)
		}
	}

	// Reset the bufferResult to an as-yet unfulfilled result, preparing for the next call
	// to Insert
	i.bufferResult = NewInsertResult()

	// If some of the modifications given in this call have been buffered, we may not return
	// a resolved promise until they have been flushed. Instead, return the currently
	// processing result folded with what we have pending
	if len(i.buffer) > 0 {
		return result.Fold(i.bufferResult)
	}

	// Otherwise we've buffered nothing (the modifications perfectly split by bufferSize)
	// and we should return the folded result
	return result
}

// Flush triggers an insert for the currently buffered elements, then calls Flush on the
// underlying inserter. The semantics of an AsyncInserter mean Flush will return a result
// that tracks all in-flight insertions, removing the need for the bufferedInserter to do
// any accounting of in-flight inserts.
func (i *bufferedInserter) Flush(ctx context.Context) InsertResult {
	i.insert(ctx, []*changelog.Modification{}, true)
	return i.inserter.Flush(ctx)
}

// enqueueAndOverflow inserts modifications into the buffer, returning chunks of
// bufferSize that have overflowed, requiring insertion. If overflowAll is set, we clear
// everything out of the buffer and into the overflow batches.
func (i *bufferedInserter) enqueueAndOverflow(modifications []*changelog.Modification, overflowAll bool) [][]*changelog.Modification {
	i.buffer = append(i.buffer, modifications...)
	overflowBatches := make([][]*changelog.Modification, 0, (len(i.buffer)+i.bufferSize-1)/i.bufferSize)

	for i.bufferSize <= len(i.buffer) {
		i.buffer, overflowBatches = i.buffer[i.bufferSize:], append(overflowBatches, i.buffer[0:i.bufferSize:i.bufferSize])
	}

	if overflowAll && len(i.buffer) > 0 {
		overflowBatches = append(overflowBatches, i.buffer)
		i.buffer = make([]*changelog.Modification, 0, i.bufferSize)
	}

	return overflowBatches
}
