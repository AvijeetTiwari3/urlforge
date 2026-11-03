package bloom

import (
	"hash/fnv"
	"math"
	"sync"
)

// Filter represents a concurrent, mathematically sound Bloom Filter
type Filter struct {
	mu        sync.RWMutex
	bitSet    []uint64
	size      uint64 // Total number of bits (m)
	hashCount uint32 // Number of hash functions (k)
}

// NewFilter computes optimal bit set size and hash count given expected elements and target false positive rate
func NewFilter(expectedElements uint64, falsePositiveRate float64) *Filter {
	if expectedElements == 0 {
		expectedElements = 1000
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		falsePositiveRate = 0.01 // default 1%
	}

	// m = - (n * ln(p)) / (ln(2)^2)
	m := math.Ceil(-float64(expectedElements) * math.Log(falsePositiveRate) / (math.Ln2 * math.Ln2))
	// k = (m / n) * ln(2)
	k := math.Round((m / float64(expectedElements)) * math.Ln2)

	bitSize := uint64(m)
	wordCount := (bitSize + 63) / 64

	return &Filter{
		bitSet:    make([]uint64, wordCount),
		size:      bitSize,
		hashCount: uint32(k),
	}
}

// Add inserts an element into the Bloom Filter
func (f *Filter) Add(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	h1, h2 := f.getHashes(key)
	for i := uint32(0); i < f.hashCount; i++ {
		// Kirsch-Mitzenmacher technique: gi(x) = h1(x) + i * h2(x)
		combined := (h1 + uint64(i)*h2) % f.size
		wordIndex := combined / 64
		bitOffset := combined % 64
		f.bitSet[wordIndex] |= (1 << bitOffset)
	}
}

// Test checks whether an element might be in the set (true) or definitely is not (false)
func (f *Filter) Test(key string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()

	h1, h2 := f.getHashes(key)
	for i := uint32(0); i < f.hashCount; i++ {
		combined := (h1 + uint64(i)*h2) % f.size
		wordIndex := combined / 64
		bitOffset := combined % 64
		if (f.bitSet[wordIndex] & (1 << bitOffset)) == 0 {
			return false // Definitely not present
		}
	}
	return true // Possibly present
}

// getHashes computes two distinct 64-bit hash values for double hashing
func (f *Filter) getHashes(key string) (uint64, uint64) {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(key))
	h1 := hasher.Sum64()

	// High quality secondary hash mixing (Murmur/SplitMix inspired)
	h2 := h1 ^ (h1 >> 30)
	h2 *= 0xbf58476d1ce4e5b9
	h2 ^= h2 >> 27
	h2 *= 0x94d049bb133111eb
	h2 ^= h2 >> 31

	if h2 == 0 {
		h2 = 1 // Ensure h2 is never zero to preserve variance across i
	}

	return h1, h2
}
