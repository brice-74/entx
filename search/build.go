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
	Searches          []*NamedQueryBuild
	Aggregates        []*common.ScalarQuery
	GroupedAggregates [][]*common.ScalarQuery
}

func (builds *ClassifiedBuilds) Execute(
	ctx context.Context,
	client entx.Client,
	cfg *Config,
	errg *errgroup.Group,
) (SearchesResponse, map[string]any) {
	nSearch := len(builds.Searches)
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
		scalarSync    *common.MapSync[string, any]
		searchesSync  *common.MapSync[string, *SearchResponse]
		scalarQueries = make([]*common.ScalarQuery, totalScalars)
	)
	if nPaginate > 0 {
		paginations = make(map[string]*common.PaginateInfos, nPaginate)
	}
	if totalScalars > 0 {
		scalarSync = common.NewMapSync(make(map[string]any, totalScalars))
	}
	if nSearch > 0 {
		searchesSync = common.NewMapSync(make(map[string]*SearchResponse, nSearch))
	}

	idx := 0
	for _, build := range builds.Searches {
		key := build.Key
		errg.Go(func() error {
			res, err := build.ExecuteSearchOnly(ctx, client, cfg)
			if err != nil {
				return err
			}
			searchesSync.Set(key, res)
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
		groups := common.MergeSlices(chunked, builds.GroupedAggregates)
		common.ExecuteScalarGroupsAsync(ctx, errg, client, cfg, scalarSync, groups...)
	}

	return searchesSync.UnsafeRaw(), scalarSync.UnsafeRaw()
}
