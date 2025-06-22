package search

import (
	"context"

	"github.com/brice-74/entx"
	"github.com/brice-74/entx/search/common"
	"github.com/brice-74/entx/search/dsl"
)

type QueryGroup struct {
	Searches   NamedQueries          `json:"searches,omitempty"`
	Aggregates dsl.OverallAggregates `json:"aggregates,omitempty"`
}

func (group *QueryGroup) Execute(
	ctx context.Context,
	client entx.Client,
	graph entx.Graph,
	cfg *Config,
) (*GroupResponse, error) {
	ctx, cancel := common.ContextTimeout(ctx, cfg.RequestTimeout)
	defer cancel()

	if err := group.ValidateAndPreprocessFinal(cfg); err != nil {
		return nil, err
	}

	build, err := group.BuildClassified(cfg, graph)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

type QueryGroupBuildClassified struct {
	PaginatedWithTx    []*NamedQueryBuild
	PaginatedWithoutTx []*NamedQueryBuild
	SearchOnly         []*NamedQueryBuild
	Aggregates         []*common.ScalarQuery
}

func (build *QueryGroupBuildClassified) Execute(
	ctx context.Context,
	client entx.Client,
	cfg *Config,
) {
	// coupled with NamedQueries, execution is the same, with the addition of aggregates
}

func (r *QueryGroup) BuildClassified(cfg *Config, graph entx.Graph) (
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
	Aggregates []*common.ScalarQuery
}

func (build *QueryGroupBuild) CountPaginations() (count int) {
	for _, search := range build.Searches {
		if search.IsPaginated() {
			count++
		}
	}
	return
}

func (r *QueryGroup) Build(cfg *Config, graph entx.Graph) (
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
	if err = common.CheckMaxAggregates(cfg, countAggregates); err != nil {
		return
	}
	if err = common.CheckMaxSearches(cfg, countSearches); err != nil {
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
