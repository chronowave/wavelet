package utils

import (
	"math"
	"testing"
)

func TestSelectConsistentHosts(t *testing.T) {
	ips := []string{"1", "2", "3", "4", "5"}
	counts := make(map[string]int64, len(ips))

	bound := int64(65537)
	for key := int64(1); key < bound; key++ {
		selections := SelectConsistentHosts(uint64(key), ips, 2)
		for _, s := range selections {
			counts[s]++
		}
	}

	mean := 2 * bound / int64(len(ips))

	for _, v := range counts {
		if math.Abs(float64(v-mean)) > 20 {
			t.Errorf("node distribution is uneven mean=%v, nodes=%v", mean, counts)
		}
	}
}
