package search

import (
	"context"
	"database/sql/driver"
	"fmt"

	"entgo.io/ent/dialect/sql"
)

type ScalarQuery struct {
	Selector *sql.Selector // subquery (must already be a SELECT returning 1 column)
	Key      string        // unique SQL alias for this sub-select
	Dest     any           // destination pointer, destinations that implement the driver.Valuer are processed.
}

func ExecuteScalar(ctx context.Context, client Client, scalar *ScalarQuery) (any, error) {
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
func ExecuteScalars(ctx context.Context, client Client, scalars ...*ScalarQuery) (map[string]any, error) {
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
