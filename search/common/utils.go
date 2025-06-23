package common

import (
	"context"
	"fmt"
	"time"
)

var noopFn = func() {}

// contextTimeout returns either a Context with the given timeout
// or the original Context if timeout ≤ 0. Always returns a non-nil CancelFunc
// that the caller should defer, even if it's a no-op.
func ContextTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout > 0 {
		return context.WithTimeout(parent, timeout)
	}
	return parent, noopFn
}

type SliceAlias[T any] interface {
	~[]T
}

func SplitInChunks[SliceT SliceAlias[ElemT], ElemT any](
	input SliceT,
	batchSize int,
) []SliceT {
	n := len(input)
	if n == 0 {
		return nil
	}
	if batchSize <= 0 || batchSize >= n {
		return []SliceT{input}
	}
	numChunks := (n + batchSize - 1) / batchSize
	chunks := make([]SliceT, numChunks)
	for i := range numChunks {
		start := i * batchSize
		end := min(start+batchSize, n)
		chunks[i] = input[start:end]
	}
	return chunks
}

func MergeSlices[T any](a, b []T) []T {
	lenA, lenB := len(a), len(b)

	switch {
	case lenA == 0:
		return b
	case lenB == 0:
		return a
	default:
		result := make([]T, lenA+lenB)
		copy(result, a)
		copy(result[lenA:], b)
		return result
	}
}

func CheckMaxSearches(cfg *Config, count int) (err error) {
	if cfg.MaxSearchesPerRequest != 0 && count > cfg.MaxSearchesPerRequest {
		err = &ValidationError{
			Rule: "MaxSearchesPerBundle",
			Err:  fmt.Errorf("found %d searches in bundle, but the maximum allowed is %d", count, cfg.MaxSearchesPerRequest),
		}
	}
	return
}

func CheckMaxAggregates(cfg *Config, count int) (err error) {
	if cfg.MaxAggregatesPerRequest != 0 && count > cfg.MaxAggregatesPerRequest {
		err = &ValidationError{
			Rule: "MaxAggregatesPerBundle",
			Err:  fmt.Errorf("found %d aggregates in bundle, but the maximum allowed is %d", count, cfg.MaxAggregatesPerRequest),
		}
	}
	return
}
