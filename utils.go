package entx

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"entgo.io/ent"
	"entgo.io/ent/dialect/sql"
)

func CombinePredicates[T ~func(*sql.Selector)](preds ...T) T {
	return func(s *sql.Selector) {
		for _, f := range preds {
			f(s)
		}
	}
}

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
				m.Aggregates[f] = NormalizeAggregateValue(v)
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

			casted := AsEntities(entities)

			for _, handler := range handlers {
				if err := handler(casted); err != nil {
					return nil, err
				}
			}

			return value, nil
		})
	})
}

func NormalizeAggregateValue(v any) any {
	switch val := v.(type) {
	case nil:
		return nil
	case int, int64, int32, uint, uint64:
		return val
	case float32, float64:
		return val
	case []uint8:
		s := string(val)
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		}
		return s
	case string:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
		return val
	case time.Time:
		return val.Format(time.RFC3339)
	default:
		return fmt.Sprintf("%v", val)
	}
}

// TODO: create slice pool to avoid re allocation ?
func AsEntities[T Entity](in []T) []Entity {
	var out = make([]Entity, len(in))
	for i, v := range in {
		out[i] = v
	}
	return out
}

// Transforms a slice of Entity into a slice of T (where T implements Entity)
// ⚠️ This conversion often requires a safe type assertion or verification
func AsTypedEntities[T Entity](in []Entity) []T {
	var out = make([]T, len(in))
	for i, v := range in {
		out[i] = v.(T)
	}
	return out
}
