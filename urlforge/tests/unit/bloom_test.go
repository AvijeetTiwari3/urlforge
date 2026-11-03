package unit

import (
	"fmt"
	"testing"

	"github.com/sde/urlforge/internal/bloom"
)

func TestBloomFilterCorrectness(t *testing.T) {
	const n = 1000
	const targetFPR = 0.01 // 1%

	filter := bloom.NewFilter(n, targetFPR)

	// Add 1000 items
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("shortcode_%d", i)
		filter.Add(key)
	}

	// Verify all 1000 items definitely return true (Zero false negatives)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("shortcode_%d", i)
		if !filter.Test(key) {
			t.Fatalf("expected key '%s' to be present, but Test returned false", key)
		}
	}

	// Verify false positive rate against 5000 non-existent items
	falsePositives := 0
	const nonExistentSamples = 5000

	for i := 0; i < nonExistentSamples; i++ {
		key := fmt.Sprintf("nonexistent_code_%d", i)
		if filter.Test(key) {
			falsePositives++
		}
	}

	actualFPR := float64(falsePositives) / float64(nonExistentSamples)
	t.Logf("Measured False Positive Rate: %.4f (target was %.4f, %d/%d false positives)",
		actualFPR, targetFPR, falsePositives, nonExistentSamples)

	// In practice, FPR should be roughly around targetFPR (allow slight statistical variance up to 3%)
	if actualFPR > 0.03 {
		t.Errorf("false positive rate %.4f significantly exceeded threshold", actualFPR)
	}
}
