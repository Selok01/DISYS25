package utils

func CompareClocks(vec1, vec2 []int32) []int32 {
	merged := make([]int32, 1000)

	for i := range len(vec1) {
		merged[i] = max(vec1[i], vec2[i])
	}

	return merged
}

func IncreaseClock(s []int32, id int32) {
	s[id]++
}