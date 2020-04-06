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
)

// fakeInserter wraps an in-memory inserter, providing the ability to hook before and
// after functions into the insertion process.
type fakeInserter struct {
	*generic.MemoryInserter
	BeforeFunc func(context.Context, []*changelog.Modification)
	AfterFunc  func(context.Context, []*changelog.Modification)
}

func (f *fakeInserter) Insert(ctx context.Context, modifications []*changelog.Modification) (int, *uint64, error) {
	if f.BeforeFunc != nil {
		f.BeforeFunc(ctx, modifications)
	}

	if f.AfterFunc != nil {
		defer f.AfterFunc(ctx, modifications)
	}

	return f.MemoryInserter.Insert(ctx, modifications)
}
