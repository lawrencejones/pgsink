package generic_test

import (
	"context"

	"github.com/lawrencejones/pg2sink/pkg/changelog"
	"github.com/lawrencejones/pg2sink/pkg/sinks/generic"
)

var (
	// insert into example (id, msg) values (1, 'original');
	fixtureExample1Insert = changelog.ModificationBuilder(
		changelog.ModificationBuilder.WithTimestampNow(),
		changelog.ModificationBuilder.WithNamespace("public.example"),
		changelog.ModificationBuilder.WithLSN(1),
		changelog.ModificationBuilder.WithAfter(map[string]interface{}{"id": 1, "msg": "original"}),
	)
	// update example set id = 1, msg = 'first' where id = 1;
	fixtureExample1UpdateFirst = changelog.ModificationBuilder(
		changelog.ModificationBuilder.WithBase(fixtureExample1Insert),
		changelog.ModificationBuilder.WithLSN(2),
		changelog.ModificationBuilder.WithUpdate(map[string]interface{}{"msg": "first"}),
	)
	// update example set id = 1, msg = 'second' where id = 1;
	fixtureExample1UpdateSecond = changelog.ModificationBuilder(
		changelog.ModificationBuilder.WithBase(fixtureExample1UpdateFirst),
		changelog.ModificationBuilder.WithLSN(3),
		changelog.ModificationBuilder.WithUpdate(map[string]interface{}{"msg": "second"}),
	)
	// insert into dog (id, name) values (1, 'scooby');
	fixtureDog1Insert = changelog.ModificationBuilder(
		changelog.ModificationBuilder.WithTimestampNow(),
		changelog.ModificationBuilder.WithNamespace("public.dog"),
		changelog.ModificationBuilder.WithLSN(4),
		changelog.ModificationBuilder.WithAfter(map[string]interface{}{"id": 1, "name": "scooby"}),
	)
	// insert into dog (id, name) values (2, 'clifford');
	fixtureDog2Insert = changelog.ModificationBuilder(
		changelog.ModificationBuilder.WithBase(fixtureDog1Insert),
		changelog.ModificationBuilder.WithLSN(5),
		changelog.ModificationBuilder.WithUpdate(map[string]interface{}{"id": 2, "name": "clifford"}),
	)
	// insert into cat (id, name, lives) values (1, 'tom', 9);
	fixtureCat1Insert = changelog.ModificationBuilder(
		changelog.ModificationBuilder.WithTimestampNow(),
		changelog.ModificationBuilder.WithNamespace("public.cat"),
		changelog.ModificationBuilder.WithLSN(6),
		changelog.ModificationBuilder.WithAfter(map[string]interface{}{"id": 1, "name": "tom", "lives": 9}),
	)
)

// fakeBackend aggregates many fakeBackend, providing an interface that can be used to
// inspect what has been inserted, as well as manipulating each inserter to simulate
// different types of failure.
type fakeBackend []*fakeInserter

func newFakeBackend(inserters ...*fakeInserter) fakeBackend {
	return append([]*fakeInserter{}, inserters...)
}

// Store returns all modifications successfully written to the backend, in a flattened
// slice
func (f fakeBackend) Store() []*changelog.Modification {
	store := []*changelog.Modification{}
	for _, inserter := range f {
		store = append(store, inserter.Store()...)
	}

	return store
}

// Batches returns a slice of all individual batches inserted into each backend
func (f fakeBackend) Batches() [][]*changelog.Modification {
	batches := [][]*changelog.Modification{}
	for _, inserter := range f {
		batches = append(batches, inserter.Batches()...)
	}

	return batches
}

// Pause causes all inserters to hang until the returned channel is closed. While most
// backends will contain just one inserter, we support one or many by returning a slice
// of channels.
func (f fakeBackend) Pause() (resumes []chan struct{}) {
	resumes = []chan struct{}{}
	for _, inserter := range f {
		resumes = append(resumes, inserter.Pause())
	}

	return resumes
}

// Fail is like pause, causing all backend inserters to return the given error whenever
// they attempt inserts. The returns slice of succeeds functions can be used to undo
// this change, and return the inserters to normal functionality.
func (f fakeBackend) Fail(err error) (succeeds []func()) {
	succeeds = []func(){}
	for _, inserter := range f {
		succeeds = append(succeeds, inserter.Fail(err))
	}

	return succeeds
}

// fakeInserter wraps an in-memory inserter, providing the ability to hook before and
// after functions into the insertion process.
type fakeInserter struct {
	*generic.MemoryInserter
	BeforeFunc func(context.Context, []*changelog.Modification) error
	AfterFunc  func(context.Context, []*changelog.Modification)
}

func (f *fakeInserter) Pause() (resume chan struct{}) {
	resume = make(chan struct{})

	f.BeforeFunc = func(ctx context.Context, _ []*changelog.Modification) error {
		select {
		case <-ctx.Done():
			logger.Log("event", "inserter.expired", "msg", "context expired before inserters were resumed")
		case <-resume:
			logger.Log("event", "inserter.resume", "msg", "resumed, inserts will now proceed")
		}

		return nil
	}

	return resume
}

func (f *fakeInserter) Fail(err error) func() {
	f.BeforeFunc = func(ctx context.Context, _ []*changelog.Modification) error {
		return err
	}

	return func() { f.BeforeFunc = nil }
}

func (f *fakeInserter) Insert(ctx context.Context, modifications []*changelog.Modification) (int, *uint64, error) {
	if f.BeforeFunc != nil {
		if err := f.BeforeFunc(ctx, modifications); err != nil {
			return -1, nil, err
		}
	}

	if f.AfterFunc != nil {
		defer f.AfterFunc(ctx, modifications)
	}

	return f.MemoryInserter.Insert(ctx, modifications)
}
