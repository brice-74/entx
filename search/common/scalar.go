package common

import (
	"context"
	"database/sql/driver"
	"errors"

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
		return nil, &ExecError{
			Op:  "ExecuteScalar",
			Err: errors.New("no rows returned"),
		}
	}
	if err := rows.Scan(scalar.Dest); err != nil {
		return nil, err
	}
	return handleValuer(scalar.Dest)
}

// executeScalars constructs and executes a query such as :
//
//	SELECT
//	  (…subquery1…) AS alias1,
//	  (…subquery2…) AS alias2,
//	  …
//
// and scans directly into Dest.
func executeScalars(
	ctx context.Context,
	client entx.Client,
	scalars ...*ScalarQuery,
) ([]any, error) {
	if length := len(scalars); length <= 0 {
		return nil, nil
	} else if length == 1 {
		scalarRes, err := ExecuteScalar(ctx, client, scalars[0])
		if err != nil {
			return nil, err
		}
		return []any{scalarRes}, nil
	}

	sel := sql.Select()
	dests := make([]any, len(scalars))
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
		return nil, &ExecError{
			Op:  "executeScalars",
			Err: errors.New("no rows returned"),
		}
	}
	if err := rows.Scan(dests...); err != nil {
		return nil, err
	}

	for i, q := range scalars {
		value, err := handleValuer(q.Dest)
		if err != nil {
			return nil, err
		}
		dests[i] = value
	}
	return dests, nil
}

func handleValuer(dest any) (any, error) {
	v, ok := dest.(driver.Valuer)
	if ok {
		val, err := v.Value()
		if err != nil {
			return nil, &ExecError{
				Op:  "handleValuer",
				Err: err,
			}
		}
		return val, nil
	}
	return v, nil
}

func ExecuteScalars(ctx context.Context, client entx.Client, resp map[string]any, scalars ...*ScalarQuery) error {
	vals, err := executeScalars(ctx, client, scalars...)
	if err != nil {
		return err
	}
	for i, q := range scalars {
		resp[q.Key] = vals[i]
	}
	return nil
}

func ExecuteScalarsAsync(ctx context.Context, client entx.Client, resp *MapSync[string, any], scalars ...*ScalarQuery) error {
	vals, err := executeScalars(ctx, client, scalars...)
	if err != nil {
		return err
	}
	resp.Lock()
	defer resp.Unlock()
	for i, q := range scalars {
		resp.UnsafeSet(q.Key, vals[i])
	}
	return nil
}

func ExecuteScalarGroupsAsync(
	ctx context.Context,
	wg *errgroup.Group,
	client entx.Client,
	cfg *Config,
	response *MapSync[string, any],
	scalarGroups ...[]*ScalarQuery,
) {
	if len(scalarGroups) == 0 {
		return
	}

	for _, group := range scalarGroups {
		switch len(group) {
		case 0:
		case 1:
			wg.Go(func() error {
				res, err := ExecuteScalar(ctx, client, group[0])
				if err != nil {
					return err
				}
				response.Set(group[0].Key, res)
				return nil
			})
		default:
			wg.Go(func() error {
				return ExecuteScalarsAsync(ctx, client, response, group...)
			})
		}
	}
}

func ExecuteScalarGroups(
	ctx context.Context,
	client entx.Client,
	cfg *Config,
	response map[string]any,
	scalarGroups ...[]*ScalarQuery,
) (err error) {
	if len(scalarGroups) == 0 {
		return
	}

	for _, group := range scalarGroups {
		switch len(group) {
		case 0:
		case 1:
			response[group[0].Key], err = ExecuteScalar(ctx, client, group[0])
			return
		default:
			return ExecuteScalars(ctx, client, response, group...)
		}
	}

	return nil
}
