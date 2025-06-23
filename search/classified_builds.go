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
	s := builds.calculateSizes()

	var (
		paginations   map[string]*common.PaginateInfos
		response      common.GroupResponseSync
		scalarQueries = make([]*common.ScalarQuery, s.TotalScalarQueries)
	)
	if s.NumPaginated > 0 {
		paginations = make(map[string]*common.PaginateInfos, s.NumPaginated)
	}
	if s.TotalScalarQueries > 0 {
		response.Aggregates = *common.NewMapSync(make(map[string]any, s.TotalScalarQueries))
	}
	if s.NumSearches > 0 {
		response.Searches = *common.NewMapSync(make(map[string]*SearchResponse, s.NumSearches))
	}

	errg, wgctx := errgroup.WithContext(ctx)
	errg.SetLimit(cfg.MaxParallelWorkersPerRequest)

	idx := 0
	for _, build := range builds.Searches {
		errg.Go(func() error {
			res, err := build.ExecuteSearchOnly(wgctx, client, cfg)
			if err != nil {
				return err
			}
			response.Searches.Set(build.Key, res)
			return nil
		})

		if build.IsPaginated() {
			paginations[build.Key] = build.Paginate
			scalarQueries[idx] = build.Paginate.ToScalarQuery(build.Key)
			idx++
		}
	}

	for i := range s.NumAggregates {
		scalarQueries[idx] = builds.Aggregates[i]
		idx++
	}

	for i := range s.NumGroupedAggs {
		grp := builds.GroupedAggregates[i]
		for j := range grp {
			scalarQueries[idx] = grp[j]
			idx++
		}
	}

	if s.TotalScalarQueries > 0 {
		chunked := common.SplitInChunks(scalarQueries, cfg.ScalarQueriesChunkSize)
		groups := common.MergeSlices(chunked, builds.GroupedAggregates)
		common.ExecuteScalarGroupsAsync(wgctx, errg, client, cfg, &response.Aggregates, groups...)
	}

	builds.Transactions.Execute(wgctx, client, cfg, errg, &response)

	if err := errg.Wait(); err != nil {
		return nil, err
	}

	if err := common.AttachPaginationAndCleanSync(&response.Searches, &response.Aggregates, paginations); err != nil {
		return nil, err
	}

	return (&response).UnsafeResponse(), nil
}

type BuildSizes struct {
	NumSearches        int
	NumPaginated       int
	NumAggregates      int
	NumGroupedAggs     int
	TotalScalarQueries int
}

func (b *ClassifiedBuilds) calculateSizes() *BuildSizes {
	m := BuildSizes{}
	m.NumSearches = len(b.Searches)
	for _, tx := range b.Transactions {
		m.NumSearches += len(tx.Searches)
	}
	for _, s := range b.Searches {
		if s.IsPaginated() {
			m.NumPaginated++
		}
	}
	m.NumAggregates = len(b.Aggregates)

	for _, grp := range b.GroupedAggregates {
		m.NumGroupedAggs += len(grp)
	}

	m.TotalScalarQueries = m.NumPaginated + m.NumAggregates + m.NumGroupedAggs
	return &m
}
