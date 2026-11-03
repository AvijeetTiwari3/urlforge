package idgen

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	// Custom Epoch: 2026-01-01 00:00:00 UTC in milliseconds
	CustomEpoch int64 = 1767225600000

	WorkerIDBits     uint8 = 10
	SequenceBits     uint8 = 12
	MaxWorkerID      int64 = -1 ^ (-1 << WorkerIDBits) // 1023
	MaxSequence      int64 = -1 ^ (-1 << SequenceBits) // 4095

	WorkerIDShift    uint8 = SequenceBits                     // 12
	TimestampShift   uint8 = SequenceBits + WorkerIDBits      // 22

	Base62Alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	Base           = int64(len(Base62Alphabet))
)

// Generator defines the distributed Snowflake ID generator
type Generator struct {
	mu           sync.Mutex
	workerID     int64
	lastTimestamp int64
	sequence     int64
}

// NewGenerator initializes a new Snowflake generator with a validated worker ID
func NewGenerator(workerID int64) (*Generator, error) {
	if workerID < 0 || workerID > MaxWorkerID {
		return nil, fmt.Errorf("worker ID must be between 0 and %d, got %d", MaxWorkerID, workerID)
	}

	return &Generator{
		workerID:      workerID,
		lastTimestamp: -1,
		sequence:      0,
	}, nil
}

// NextID generates a unique, sortable, 64-bit snowflake ID
func (g *Generator) NextID() (int64, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	currentTimestamp := time.Now().UnixMilli()

	if currentTimestamp < g.lastTimestamp {
		// Clock drift detected (NTP adjustment or leap second)
		offset := g.lastTimestamp - currentTimestamp
		if offset <= 5 {
			// Small drift: wait it out
			time.Sleep(time.Duration(offset) * time.Millisecond)
			currentTimestamp = time.Now().UnixMilli()
		} else {
			return 0, fmt.Errorf("clock moved backwards by %d ms; refusing to generate ID", offset)
		}
	}

	if currentTimestamp == g.lastTimestamp {
		g.sequence = (g.sequence + 1) & MaxSequence
		if g.sequence == 0 {
			// Sequence exhausted in current millisecond, wait for next ms
			for currentTimestamp <= g.lastTimestamp {
				currentTimestamp = time.Now().UnixMilli()
			}
		}
	} else {
		g.sequence = 0
	}

	g.lastTimestamp = currentTimestamp

	id := ((currentTimestamp - CustomEpoch) << TimestampShift) |
		(g.workerID << WorkerIDShift) |
		g.sequence

	return id, nil
}

// EncodeBase62 converts a 64-bit positive integer into a compact Base62 string
func EncodeBase62(num int64) string {
	if num == 0 {
		return string(Base62Alphabet[0])
	}

	var sb strings.Builder
	for num > 0 {
		remainder := num % Base
		sb.WriteByte(Base62Alphabet[remainder])
		num /= Base
	}

	// Reverse to preserve big-endian order
	bytes := []byte(sb.String())
	for i, j := 0, len(bytes)-1; i < j; i, j = i+1, j-1 {
		bytes[i], bytes[j] = bytes[j], bytes[i]
	}

	return string(bytes)
}

// DecodeBase62 converts a Base62 string back into its original 64-bit integer
func DecodeBase62(str string) (int64, error) {
	var num int64 = 0

	for i := 0; i < len(str); i++ {
		char := str[i]
		idx := strings.IndexByte(Base62Alphabet, char)
		if idx == -1 {
			return 0, errors.New("invalid character in Base62 string")
		}
		num = num*Base + int64(idx)
	}

	return num, nil
}
