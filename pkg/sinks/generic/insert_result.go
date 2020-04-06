package generic

import "context"

// InsertResult is a promise interface for async insert operations. Get() will block until
// a value is fulfilled, while Fold() will merge the result of another InsertResult,
// taking the higher of the two lsns (if provided)
type InsertResult interface {
	Get(context.Context) (count int, lsn *uint64, err error)
	Fold(InsertResult) InsertResult
}

// EmptyInsertResult represents an insertion that did no work. It can be used as the base
// element when recursively folding insertions together.
var EmptyInsertResult = NewInsertResult().Resolve(0, nil, nil)

// chainedInsertResult gathers together many InsertResults, resolving only once all child
// results have been resolved.
type chainedInsertResult struct {
	results []InsertResult
}

func (r *chainedInsertResult) Get(ctx context.Context) (count int, lsn *uint64, err error) {
	for _, result := range r.results {
		otherCount, otherLSN, err := result.Get(ctx)
		if err != nil {
			return -1, nil, err
		}

		count += otherCount
		if otherLSN != nil {
			if lsn == nil || *lsn < *otherLSN {
				lsn = otherLSN
			}
		}
	}

	return
}

func (r *chainedInsertResult) Fold(other InsertResult) InsertResult {
	r.results = append(r.results, other)
	return r
}

func NewInsertResult() *insertResult {
	return &insertResult{ready: make(chan struct{})}
}

// insertResult is a single insertion result, or promise
type insertResult struct {
	ready chan struct{}
	count int
	lsn   *uint64
	err   error
}

func (r *insertResult) Get(ctx context.Context) (int, *uint64, error) {
	// If the result is ready, we should return the error even if the context is done
	select {
	case <-r.ready:
		return r.count, r.lsn, r.err
	default:
	}
	select {
	case <-ctx.Done():
		return -1, nil, ctx.Err()
	case <-r.ready:
		return r.count, r.lsn, r.err
	}
}

func (r *insertResult) Fold(other InsertResult) InsertResult {
	return &chainedInsertResult{[]InsertResult{r, other}}
}

// Resolve fulfills the promise. We return an InsertResult, as it is not correct to
// resolve a promise twice, so we don't care if the caller no longer has access to this
// method.
func (r *insertResult) Resolve(count int, lsn *uint64, err error) InsertResult {
	r.count = count
	r.lsn = lsn
	r.err = err
	close(r.ready)

	return r
}

// ResolveFrom causes the subject to be resolved with the value of the given result
func (r *insertResult) ResolveFrom(ctx context.Context, result InsertResult) InsertResult {
	go func() {
		r.Resolve(result.Get(ctx))
	}()

	return r
}
