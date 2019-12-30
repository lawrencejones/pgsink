package util

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
