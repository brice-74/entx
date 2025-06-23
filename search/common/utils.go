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

func SplitInChunks[SliceT SliceAlias[ElemT], ElemT any](input SliceT, batchSize int) []SliceT {
	if batchSize <= 0 || batchSize >= len(input) {
		if len(input) == 0 {
			return nil
		}
		return []SliceT{input}
	}
	var chunks []SliceT
	for i := 0; i < len(input); i += batchSize {
		end := min(i+batchSize, len(input))
		chunks = append(chunks, input[i:end])
	}
	return chunks
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
