package entx

import (
	"context"
	"fmt"

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
