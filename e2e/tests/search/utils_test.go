package e2e_search_test

import (
	"context"
	"e2e/ent/entx"
	"testing"

	entxstd "github.com/brice-74/entx"
	"github.com/brice-74/entx/search"
	"github.com/stretchr/testify/require"
)

type Executable[T any] interface {
	Execute(ctx context.Context, client entxstd.Client, graph entxstd.Graph, cfg *search.Config) (T, error)
}

func runExecutable[Res any](t *testing.T, q Executable[Res], cfg *search.Config) Res {
	t.Helper()
	res, err := q.Execute(context.Background(), client, entx.Graph, cfg)
	require.NoError(t, err)
	return res
}

func runExecutableErr[Res any](t *testing.T, q Executable[Res], cfg *search.Config) error {
	t.Helper()
	_, err := q.Execute(context.Background(), client, entx.Graph, cfg)
	require.Error(t, err)
	return err
}

func runTargetedQuery[T entxstd.Entity](t *testing.T, q *search.TargetedQuery, cfg *search.Config) []T {
	t.Helper()
	res := runExecutable(t, q, cfg)
	return entxstd.AsTypedEntities[T](res.Data.([]entxstd.Entity))
}
