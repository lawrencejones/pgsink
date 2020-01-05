package util

import (
	"reflect"
	"sync"
)

func Diff(s1 []string, s2 []string) []string {
	result := make([]string, 0)
	for _, s := range s1 {
		if !Includes(s2, s) {
			result = append(result, s)
		}
	}

	return result
}

func Includes(ss []string, s string) bool {
	for _, existing := range ss {
		if existing == s {
			return true
		}
	}

	return false
}

// SyncSet provides a race-safe interface for building a set of unknown type
type SyncSet struct {
	elements []interface{}
	sync.RWMutex
}

func (s *SyncSet) Add(candidate interface{}) {
	s.Lock()
	defer s.Unlock()

	if s.indexOf(candidate) == -1 {
		s.elements = append(s.elements, candidate)
	}
}

// All generates a slice of set elements. Optionally provide a single typed slice paramter
// to coerce the result elements into that type of slice.
func (s *SyncSet) All(outputs ...interface{}) interface{} {
	var output interface{} = []interface{}{}
	if len(outputs) == 1 {
		output = outputs[0]
	}

	s.RLock()
	defer s.RUnlock()

	t := reflect.TypeOf(output)
	if t.Kind() != reflect.Slice {
		panic("unsupported type")
	}

	v := reflect.ValueOf(output)
	for _, element := range s.elements {
		v = reflect.Append(v, reflect.ValueOf(element))
	}

	return v.Interface()
}

func (s *SyncSet) Remove(candidate interface{}) {
	s.Lock()
	defer s.Unlock()

	if idx := s.indexOf(candidate); idx > -1 {
		s.elements = append(s.elements[:idx], s.elements[idx+1:]...)
	}
}

func (s *SyncSet) IndexOf(candidate interface{}) int {
	s.RLock()
	defer s.RUnlock()

	return s.indexOf(candidate)
}

func (s *SyncSet) indexOf(candidate interface{}) int {
	for idx, element := range s.elements {
		if reflect.DeepEqual(element, candidate) {
			return idx
		}
	}

	return -1
}
