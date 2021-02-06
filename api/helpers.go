package api

import "time"

// FormatDateTime generates an RFC3339 string of the given timestamp, or an empty string
// if the time is zero.
func FormatDateTime(ts time.Time) string {
	if ts.IsZero() {
		return ""
	}

	return ts.Format(time.RFC3339)
}

// FormatDateTimePointer generates an RFC3339 string of the given timestamp, or nil if it
// was empty/nil.
func FormatDateTimePointer(ts *time.Time) *string {
	if ts == nil {
		return nil
	}

	result := FormatDateTime(*ts)
	if result == "" {
		return nil
	}

	return &result
}
