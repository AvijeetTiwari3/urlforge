package unit

import (
	"sync"
	"testing"

	"github.com/sde/urlforge/internal/idgen"
)

func TestSnowflakeGenerationAndUniqueness(t *testing.T) {
	gen, err := idgen.NewGenerator(1)
	if err != nil {
		t.Fatalf("failed to create generator: %v", err)
	}

	const numGoroutines = 50
	const idsPerGoroutine = 200
	const totalExpected = numGoroutines * idsPerGoroutine

	idMap := sync.Map{}
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < idsPerGoroutine; j++ {
				id, err := gen.NextID()
				if err != nil {
					t.Errorf("error generating ID: %v", err)
					return
				}
				if _, loaded := idMap.LoadOrStore(id, true); loaded {
					t.Errorf("duplicate ID detected: %d", id)
				}
			}
		}()
	}

	wg.Wait()

	count := 0
	idMap.Range(func(_, _ any) bool {
		count++
		return true
	})

	if count != totalExpected {
		t.Fatalf("expected %d unique IDs, got %d", totalExpected, count)
	}
}

func TestBase62RoundTrip(t *testing.T) {
	testCases := []int64{
		0,
		1,
		61,
		62,
		123456789,
		987654321012345,
	}

	for _, original := range testCases {
		encoded := idgen.EncodeBase62(original)
		decoded, err := idgen.DecodeBase62(encoded)
		if err != nil {
			t.Fatalf("failed to decode '%s': %v", encoded, err)
		}
		if decoded != original {
			t.Errorf("roundtrip mismatch for %d: encoded as %s, decoded as %d", original, encoded, decoded)
		}
	}
}
