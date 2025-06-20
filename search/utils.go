package search

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"entgo.io/ent"
	"entgo.io/ent/dialect/sql"
	"golang.org/x/sync/errgroup"
)

type EntityHandler func(entities []Entity) error

func AddAggregatesFromValues(fields ...string) EntityHandler {
	return func(entities []Entity) error {
		for _, e := range entities {
			for _, f := range fields {
				v, err := e.Value(f)
				if err != nil {
					return err
				}
				m := e.Metadatas()
				if m.Aggregates == nil {
					m.Aggregates = make(map[string]any, len(fields))
				}
				m.Aggregates[f] = v
			}
		}
		return nil
	}
}

func ToInterceptor[T Entity](handlers ...EntityHandler) ent.Interceptor {
	return ent.InterceptFunc(func(next ent.Querier) ent.Querier {
		return ent.QuerierFunc(func(ctx context.Context, query ent.Query) (ent.Value, error) {
			value, err := next.Query(ctx, query)
			if err != nil {
				return nil, err
			}

			entities, hasMeta := value.([]T)
			if !hasMeta {
				return nil, fmt.Errorf("query result value (%T) cannot be cast to []%T", value, new(T))
			}

			casted := ToEntitySlice(entities)

			for _, handler := range handlers {
				if err := handler(casted); err != nil {
					return nil, err
				}
			}

			return value, nil
		})
	})
}

// TODO: create slice pool to avoid re allocation ?
func ToEntitySlice[T Entity](in []T) []Entity {
	var out = make([]Entity, len(in))
	for i, v := range in {
		out[i] = v
	}
	return out
}

func CombinePredicates[T ~func(*sql.Selector)](preds ...T) T {
	return func(s *sql.Selector) {
		for _, f := range preds {
			f(s)
		}
	}
}

// resolveChain traverses a list of segments starting from a start node.
// Returns the final node, the name of the terminal field (or empty if no field) and
// the slice of bridges traversed.
func resolveChain(start Node, parts []string) (current Node, field string, bridges []Bridge, err error) {
	current = start
	bridges = make([]Bridge, 0, len(parts))
	for i, seg := range parts {
		if b := current.Bridge(seg); b != nil {
			bridges = append(bridges, b)
			current = b.Child()
		} else if f := current.FieldByName(seg); f != nil {
			// Si c'est un champ et pas le dernier segment, c'est une erreur
			if i != len(parts)-1 {
				err = fmt.Errorf("chain broken: the %q field cannot be in the middle of the chain", seg)
				return
			}
			field = seg
		} else {
			err = fmt.Errorf("%q isn't field or bridge of %q", seg, current.Table())
			return
		}
	}
	return
}

func splitChain(s string) (parts []string, invalidAt int, ok bool) {
	parts = strings.Split(s, ".")
	pos := 0
	for _, part := range parts {
		if part == "" {
			return nil, pos, false
		}
		pos += len(part) + 1
	}
	return parts, -1, true
}

var noopFn = func() {}

// contextTimeout returns either a Context with the given timeout
// or the original Context if timeout ≤ 0. Always returns a non-nil CancelFunc
// that the caller should defer, even if it's a no-op.
func contextTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout > 0 {
		return context.WithTimeout(parent, timeout)
	}
	return parent, noopFn
}

type SliceAlias[T any] interface {
	~[]T
}

func splitInChunks[SliceT SliceAlias[ElemT], ElemT any](input SliceT, batchSize int) []SliceT {
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
			ctx, cancel := contextTimeout(ctx, timeout)
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
