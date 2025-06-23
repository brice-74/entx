package search

import (
	"context"

	"github.com/brice-74/entx"
	"github.com/brice-74/entx/search/common"
	"golang.org/x/sync/errgroup"
)

// ClassifiedBuilds is used to execute constructs already organized by category at once.
// Transactions are omitted as they can be executed independently.
type ClassifiedBuilds struct {
	Transactions      TxQueryGroupBuilds
	Searches          []*NamedQueryBuild
	Aggregates        []*common.ScalarQuery
	GroupedAggregates [][]*common.ScalarQuery
}

func (builds *ClassifiedBuilds) Execute(
	ctx context.Context,
	client entx.Client,
	cfg *Config,
) (*GroupResponse, error) {
	nSearch := len(builds.Searches)
	for _, t := range builds.Transactions {
		nSearch += len(t.Searches)
	}
	nPaginate := 0
	for _, s := range builds.Searches {
		if s.IsPaginated() {
			nPaginate++
		}
	}
	nAgg := len(builds.Aggregates)
	nGroupAgg := 0
	for _, grp := range builds.GroupedAggregates {
		nGroupAgg += len(grp)
	}
	totalScalars := nAgg + nPaginate + nGroupAgg

	var (
		paginations   map[string]*common.PaginateInfos
		response      common.GroupResponseSync
		scalarQueries = make([]*common.ScalarQuery, totalScalars)
	)
	if nPaginate > 0 {
		paginations = make(map[string]*common.PaginateInfos, nPaginate)
	}
	if totalScalars > 0 {
		response.Aggregates = *common.NewMapSync(make(map[string]any, totalScalars))
	}
	if nSearch > 0 {
		response.Searches = *common.NewMapSync(make(map[string]*SearchResponse, nSearch))
	}

	errg, wgctx := errgroup.WithContext(ctx)
	errg.SetLimit(cfg.MaxParallelWorkersPerRequest)

	idx := 0
	for _, build := range builds.Searches {
		key := build.Key
		errg.Go(func() error {
			res, err := build.ExecuteSearchOnly(wgctx, client, cfg)
			if err != nil {
				return err
			}
			response.Searches.Set(key, res)
			return nil
		})

		if build.IsPaginated() {
			paginations[key] = build.Paginate
			scalarQueries[idx] = build.Paginate.ToScalarQuery(key)
			idx++
		}
	}

	for i := 0; i < nAgg; i++ {
		scalarQueries[idx] = builds.Aggregates[i]
		idx++
	}

	for _, grp := range builds.GroupedAggregates {
		for _, q := range grp {
			scalarQueries[idx] = q
			idx++
		}
	}

	if totalScalars > 0 {
		chunked := common.SplitInChunks(scalarQueries, cfg.ScalarQueriesChunkSize)
		groups := mergeSlices(chunked, builds.GroupedAggregates)
		common.ExecuteScalarGroupsAsync(wgctx, errg, client, cfg, &response.Aggregates, groups...)
	}

	if err := errg.Wait(); err != nil {
		return nil, err
	}

	if err := common.AttachPaginationAndCleanSync(&response.Searches, &response.Aggregates, paginations); err != nil {
		return nil, err
	}

	return (&response).UnsafeResponse(), nil
}

func mergeSlices[T any](a, b []T) []T {
	lenA, lenB := len(a), len(b)

	switch {
	case lenA == 0:
		return b
	case lenB == 0:
		return a
	default:
		result := make([]T, lenA+lenB)
		copy(result, a)
		copy(result[lenA:], b)
		return result
	}
}
