package search

import (
	"context"
)

type QueryBundle struct {
	Transactions   TxQueryGroups       `json:"transactions,omitempty"`
	ParallelGroups []OverallAggregates `json:"parallel_aggregates_groups,omitempty"`
	QueryGroup
}

type AggregatesMeta struct {
	Aggregates AggregatesResponse `json:"aggregates,omitempty"`
}

type GroupResponse struct {
	Searches SearchesResponse `json:"searches,omitempty"`
	Meta     *AggregatesMeta  `json:"meta,omitempty"`
}

func (r *QueryBundle) Execute(
	ctx context.Context,
	client Client,
	graph Graph,
	cfg *Config,
) (*GroupResponse, error) {
	// TODO
	return nil, nil
}

func (r *QueryBundle) ValidateAndPreprocessFinal(cfg *Config) (countAggregates, countSearches int, err error) {
	searches, aggregates, err := r.QueryGroup.ValidateAndPreprocess(cfg)
	if err != nil {
		return 0, 0, err
	}
	countSearches += searches
	countAggregates += aggregates

	searches, aggregates, err = r.Transactions.ValidateAndPreprocess(cfg)
	if err != nil {
		return 0, 0, err
	}
	countSearches += searches
	countAggregates += aggregates

	for _, aggregates := range r.ParallelGroups {
		count, err := aggregates.ValidateAndPreprocess(cfg)
		if err != nil {
			return 0, 0, err
		}
		countAggregates += count
	}
	return
}

type QueryGroup struct {
	Searches   NamedQueries      `json:"searches,omitempty"`
	Aggregates OverallAggregates `json:"aggregates,omitempty"`
}

func (r *QueryGroup) Execute(
	ctx context.Context,
	client Client,
	graph Graph,
	cfg *Config,
) (*GroupResponse, error) {
	// TODO
	return nil, nil
}

type QueryGroupBuildClassified struct {
	PaginatedWithTx    []*NamedQueryBuild
	PaginatedWithoutTx []*NamedQueryBuild
	SearchOnly         []*NamedQueryBuild
	Aggregates         []*ScalarQuery
}

func (r *QueryGroup) BuildClassified(cfg *Config, graph Graph) (
	build *QueryGroupBuildClassified,
	err error,
) {
	build = new(QueryGroupBuildClassified)
	if build.SearchOnly,
		build.PaginatedWithTx,
		build.PaginatedWithoutTx,
		err = r.Searches.BuildClassified(cfg, graph); err != nil {
		return
	}
	if build.Aggregates, err = r.Aggregates.BuildScalars(graph); err != nil {
		return
	}
	return
}

type QueryGroupBuild struct {
	Searches   []*NamedQueryBuild
	Aggregates []*ScalarQuery
}

func (build *QueryGroupBuild) CountPaginations() (count int) {
	for _, search := range build.Searches {
		if search.IsPaginated() {
			count++
		}
	}
	return
}

func (r *QueryGroup) Build(cfg *Config, graph Graph) (
	build *QueryGroupBuild,
	err error,
) {
	build = new(QueryGroupBuild)
	if build.Searches, err = r.Searches.Build(cfg, graph); err != nil {
		return
	}
	if build.Aggregates, err = r.Aggregates.BuildScalars(graph); err != nil {
		return
	}
	return
}

func (sr *QueryGroup) ValidateAndPreprocessFinal(cfg *Config) (err error) {
	var countSearches, countAggregates int
	if countAggregates, err = sr.Aggregates.ValidateAndPreprocess(cfg); err != nil {
		return
	}
	if countSearches, err = sr.Searches.ValidateAndPreprocess(cfg); err != nil {
		return
	}
	if err = checkMaxAggregates(cfg, countAggregates); err != nil {
		return
	}
	if err = checkMaxSearches(cfg, countSearches); err != nil {
		return
	}
	return
}

func (sr *QueryGroup) ValidateAndPreprocess(cfg *Config) (countSearches, countAggregates int, err error) {
	if countAggregates, err = sr.Aggregates.ValidateAndPreprocess(cfg); err != nil {
		return
	}
	if countSearches, err = sr.Searches.ValidateAndPreprocess(cfg); err != nil {
		return
	}
	return
}
