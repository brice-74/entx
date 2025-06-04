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

type ScalarExecutor struct {
	Queries []ScalarQuery
}

func NewScalarExecutor(len int) *ScalarExecutor {
	return &ScalarExecutor{Queries: make([]ScalarQuery, 0, len)}
}

func (e *ScalarExecutor) Add(sel *sql.Selector, key string, dest any) *ScalarExecutor {
	e.Queries = append(e.Queries, ScalarQuery{Selector: sel, Key: key, Dest: dest})
	return e
}

func (e *ScalarExecutor) AddQ(queries ...ScalarQuery) *ScalarExecutor {
	e.Queries = append(e.Queries, queries...)
	return e
}

// Execute constructs and executes a query such as :
//
//	SELECT
//	  (…subquery1…) AS alias1,
//	  (…subquery2…) AS alias2,
//	  …
//
// and scans directly into Dest.
func (e *ScalarExecutor) Execute(ctx context.Context, client Client) (map[string]any, error) {
	if len(e.Queries) == 0 {
		return nil, nil
	}

	sel := sql.Select()
	dests := make([]any, len(e.Queries))
	for i, q := range e.Queries {
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
		return nil, fmt.Errorf("no rows returned")
	}
	if err := rows.Scan(dests...); err != nil {
		return nil, err
	}

	res := make(map[string]any, len(e.Queries))
	for _, q := range e.Queries {
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
