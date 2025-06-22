package common

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
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

func GoExecBatch[T any](
	ctx context.Context,
	wg *errgroup.Group,
	timeout time.Duration,
	funcs []func(context.Context) (T, error),
) []T {
	results := make([]T, len(funcs))
	var mu sync.Mutex

	for i := range funcs {
		if ctx.Err() != nil {
			break
		}

		i := i
		wg.Go(func() error {
			ctx, cancel := ContextTimeout(ctx, timeout)
			defer cancel()

			res, err := funcs[i](ctx)
			if err != nil {
				return err
			}

			mu.Lock()
			results[i] = res
			mu.Unlock()
			return nil
		})
	}

	return results
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

func AttachPagination(
	res *GroupResponse,
	pagMap map[string]*PaginateInfos,
) error {
	for key, p := range pagMap {
		raw, ok := res.Meta.Aggregates[key]
		if !ok {
			return &ExecError{
				Op:  "attachPagination",
				Err: fmt.Errorf("missing paginate count for '%s'", key),
			}
		}
		cnt, ok := raw.(int64)
		if !ok {
			return &ExecError{
				Op:  "attachPagination",
				Err: fmt.Errorf("paginate count wrong type for '%s': %T", key, raw),
			}

		}
		sr, exist := res.Searches[key]
		if !exist {
			return &ExecError{
				Op:  "attachPagination",
				Err: fmt.Errorf("search response not found for paginate on key '%s'", key),
			}
		}
		sr.Meta.Paginate = p.Calculate(int(cnt), sr.Meta.Count)
		delete(res.Meta.Aggregates, key)
	}
	return nil
}
