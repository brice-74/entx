package common

import (
	"context"
	"database/sql/driver"
	"fmt"
	"maps"
	"sync"

	"entgo.io/ent/dialect/sql"
	"github.com/brice-74/entx"
	"golang.org/x/sync/errgroup"
)

type ScalarQuery struct {
	Selector *sql.Selector // subquery (must already be a SELECT returning 1 column)
	Key      string        // unique SQL alias for this sub-select
	Dest     any           // destination pointer, destinations that implement the driver.Valuer are processed.
}

func ExecuteScalar(ctx context.Context, client entx.Client, scalar *ScalarQuery) (any, error) {
	query, args := sql.SelectExpr(scalar.Selector).Query()
	rows, err := client.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("ExecuteScalars: no rows returned")
	}
	if err := rows.Scan(scalar.Dest); err != nil {
		return nil, err
	}
	v, ok := scalar.Dest.(driver.Valuer)
	if ok {
		val, err := v.Value()
		if err != nil {
			return nil, err
		}
		return val, nil
	}
	return scalar.Dest, nil
}

// Execute constructs and executes a query such as :
//
//	SELECT
//	  (…subquery1…) AS alias1,
//	  (…subquery2…) AS alias2,
//	  …
//
// and scans directly into Dest.
func ExecuteScalars(ctx context.Context, client entx.Client, scalars ...*ScalarQuery) (map[string]any, error) {
	if length := len(scalars); length <= 0 {
		return nil, nil
	} else if length == 1 {
		res, err := ExecuteScalar(ctx, client, scalars[0])
		if err != nil {
			return nil, err
		}
		return map[string]any{scalars[0].Key: res}, nil
	}

	sel := sql.Select()
	lenScalars := len(scalars)
	dests := make([]any, lenScalars)
	for i, q := range scalars {
		sel.AppendSelectExprAs(q.Selector, q.Key)
		dests[i] = q.Dest
	}

	query, args := sel.Query()
	rows, err := client.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("ExecuteScalars: no rows returned")
	}
	if err := rows.Scan(dests...); err != nil {
		return nil, err
	}

	res := make(map[string]any, lenScalars)
	for _, q := range scalars {
		if v, ok := q.Dest.(driver.Valuer); ok {
			val, err := v.Value()
			if err != nil {
				return nil, err
			}
			res[q.Key] = val
		} else {
			res[q.Key] = q.Dest
		}
	}
	return res, nil
}

func ExecuteScalarGroupsAsync(
	ctx context.Context,
	wg *errgroup.Group,
	client entx.Client,
	cfg *Config,
	responseSize int,
	scalarGroups ...[]*ScalarQuery,
) map[string]any {
	if len(scalarGroups) == 0 {
		return nil
	}

	var mu sync.Mutex
	finalRes := make(map[string]any, responseSize)
	for _, group := range scalarGroups {
		switch len(group) {
		case 0:
		case 1:
			wg.Go(func() error {
				ctx, cancel := ContextTimeout(ctx, cfg.AggregateTimeout)
				defer cancel()
				res, err := ExecuteScalar(ctx, client, group[0])
				if err != nil {
					return err
				}
				mu.Lock()
				finalRes[group[0].Key] = res
				mu.Unlock()
				return nil
			})
		default:
			wg.Go(func() error {
				ctx, cancel := ContextTimeout(ctx, cfg.AggregateTimeout)
				defer cancel()
				res, err := ExecuteScalars(ctx, client, group...)
				if err != nil {
					return err
				}
				mu.Lock()
				maps.Copy(finalRes, res)
				mu.Unlock()
				return nil
			})
		}
	}
	return finalRes
}

func ExecuteScalarGroupsSync(
	ctx context.Context,
	client entx.Client,
	cfg *Config,
	responseSize int,
	scalarGroups ...[]*ScalarQuery,
) (map[string]any, error) {
	if len(scalarGroups) == 0 {
		return nil, nil
	}

	finalRes := make(map[string]any, responseSize)
	for _, group := range scalarGroups {
		switch len(group) {
		case 0:
		case 1:
			ctx, cancel := ContextTimeout(ctx, cfg.AggregateTimeout)
			defer cancel()

			res, err := ExecuteScalar(ctx, client, group[0])
			if err != nil {
				return nil, err
			}

			finalRes[group[0].Key] = res
		default:
			ctx, cancel := ContextTimeout(ctx, cfg.AggregateTimeout)
			defer cancel()

			res, err := ExecuteScalars(ctx, client, group...)
			if err != nil {
				return nil, err
			}

			maps.Copy(finalRes, res)
		}
	}

	return finalRes, nil
}
