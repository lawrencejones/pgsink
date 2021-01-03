package changelog

import "fmt"

// Table uniquely identifies a Postgres table, by both schema and table name.
type Table struct {
	Schema    string `json:"schema"`
	TableName string `json:"table_name"`
}

func (t Table) String() string {
	return fmt.Sprintf("%s.%s", t.Schema, t.TableName)
}

type Tables []Table

func (s1 Tables) Diff(s2 Tables) Tables {
	result := make([]Table, 0)
	for _, s := range s1 {
		if !s2.Includes(s) {
			result = append(result, s)
		}
	}

	return result
}

func (ss Tables) Includes(s Table) bool {
	for _, existing := range ss {
		if existing.Schema == s.Schema && existing.TableName == s.TableName {
			return true
		}
	}

	return false
}
