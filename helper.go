package hotstuff

import "math"

// NumFaulty calculates 'f', which is the number of replicas that can be faulty for a configuration of size 'n'.
func NumFaulty(n int) int {
	return (n - 1) / 3
}

// QuorumSize calculates '2f + 1', which is the quorum size for a configuration of size 'n'.
func QuorumSize(n int) int {
	return n - NumFaulty(n)
}

// RapidFair:baseline 计算公平排序所需的fault replica数量和quorum size
func NumFaultyFair(n int, gamma float32) int {
	return int(math.Floor((float64(n-1) * float64(2*gamma-1)) / float64(4)))
}

func QuorumSizeFair(n int, gamma float32) int {
	return n - NumFaultyFair(n, gamma)
}
