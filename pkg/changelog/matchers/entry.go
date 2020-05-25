package matchers

import (
	"fmt"
	"strings"

	"github.com/lawrencejones/pgsink/pkg/changelog"

	"github.com/davecgh/go-spew/spew"

	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
	. "github.com/onsi/gomega/types"
)

// ChangelogMatcher tracks the entries that come through a changelog so on failure, we can
// print each entry that came through the changelog next to the reason it failed the
// matcher.
func ChangelogMatcher(matcher GomegaMatcher) *changelogMatcher {
	switch matcher.(type) {
	case schemaMatcher:
		matcher = MatchFields(IgnoreExtras, Fields{"Schema": PointTo(matcher)})
	case modificationMatcher:
		matcher = MatchFields(IgnoreExtras, Fields{"Modification": PointTo(matcher)})
	default:
		panic("unsupported matcher type")
	}

	return &changelogMatcher{
		matcher: matcher,
	}
}

type changelogMatcher struct {
	matcher  GomegaMatcher
	failures []string
}

func (m *changelogMatcher) Match(actual interface{}) (success bool, err error) {
	entry, ok := actual.(changelog.Entry)
	if !ok {
		return false, fmt.Errorf("was not given changelog.Entry")
	}

	success, err = m.matcher.Match(actual)
	if success {
		return
	}

	template := `
Changelog[%d] ==> %s
%s`
	m.failures = append(m.failures, fmt.Sprintf(
		template, len(m.failures), spew.Sdump(entry), m.matcher.FailureMessage(entry),
	))

	return false, err
}

func (m *changelogMatcher) FailureMessage(actual interface{}) (message string) {
	return strings.Join(m.failures, "\n")
}

func (m *changelogMatcher) NegatedFailureMessage(actual interface{}) (message string) {
	return m.matcher.NegatedFailureMessage(actual)
}

func SchemaMatcher(namespace interface{}) schemaMatcher {
	return schemaMatcher{
		MatchFields(IgnoreExtras, Fields{
			"Spec": MatchFields(IgnoreExtras, Fields{
				"Namespace": BeEquivalentTo(namespace),
			}),
		}),
	}
}

type schemaMatcher struct {
	GomegaMatcher
}

func (m schemaMatcher) With(matcher interface{}) schemaMatcher {
	return schemaMatcher{SatisfyAll(m, match(matcher))}
}

func (m schemaMatcher) WithSpec(matcher interface{}) schemaMatcher {
	return m.With(MatchFields(IgnoreExtras, Fields{"Spec": match(matcher)}))
}

func (m schemaMatcher) WithFields(fields ...interface{}) schemaMatcher {
	return m.WithSpec(MatchFields(IgnoreExtras, Fields{"Fields": ContainElements(fields...)}))
}

func ModificationMatcher(namespace interface{}) modificationMatcher {
	return modificationMatcher{
		MatchFields(IgnoreExtras, Fields{
			"Namespace": BeEquivalentTo(namespace),
		}),
	}
}

type modificationMatcher struct {
	GomegaMatcher
}

func (m modificationMatcher) With(matcher interface{}) modificationMatcher {
	return modificationMatcher{SatisfyAll(m, match(matcher))}
}

func (m modificationMatcher) WithBefore(matcher interface{}) modificationMatcher {
	return m.With(MatchFields(IgnoreExtras, Fields{"Before": match(matcher)}))
}

func (m modificationMatcher) WithAfter(matcher interface{}) modificationMatcher {
	return m.With(MatchFields(IgnoreExtras, Fields{"After": match(matcher)}))
}

func match(thing interface{}) GomegaMatcher {
	if matcher, ok := thing.(GomegaMatcher); ok {
		return matcher
	}

	return Equal(thing)
}
