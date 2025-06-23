package search

import (
	"context"

	"github.com/brice-74/entx"
	"github.com/brice-74/entx/search/common"
	"github.com/brice-74/entx/search/dsl"
)

type QueryBundle struct {
	Transactions   TxQueryGroups           `json:"transactions,omitempty"`
	ParallelGroups []dsl.OverallAggregates `json:"parallel_aggregates_groups,omitempty"`
	QueryGroup
}

func (q *QueryBundle) Execute(
	ctx context.Context,
	client entx.Client,
	graph entx.Graph,
	cfg *Config,
) (*GroupResponse, error) {
	ctx, cancel := common.ContextTimeout(ctx, cfg.RequestTimeout)
	defer cancel()

	if err := q.ValidateAndPreprocessFinal(cfg); err != nil {
		return nil, err
	}

	build, err := q.BuildClassified(cfg, graph)
	if err != nil {
		return nil, err
	}

	res, err := build.Execute(ctx, client, cfg)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (q *QueryBundle) BuildClassified(
	cfg *Config,
	graph entx.Graph,
) (*ClassifiedBuilds, error) {
	build, err := q.QueryGroup.BuildClassified(cfg, graph)
	if err != nil {
		return nil, err
	}

	txs, err := q.Transactions.Build(cfg, graph)
	if err != nil {
		return nil, err
	}
	build.Transactions = common.MergeSlices(build.Transactions, txs)

	if ng := len(q.ParallelGroups); ng > 0 {
		grouped := make([][]*common.ScalarQuery, ng)
		for i := range ng {
			var grp []*common.ScalarQuery
			grp, err = q.ParallelGroups[i].BuildScalars(graph)
			if err != nil {
				return nil, err
			}
			grouped[i] = grp
		}
		build.GroupedAggregates = grouped
	}

	return build, nil
}

func (r *QueryBundle) ValidateAndPreprocessFinal(cfg *Config) error {
	countSearches, countAggregates, err := r.QueryGroup.ValidateAndPreprocess(cfg)
	if err != nil {
		return err
	}

	searches, aggregates, err := r.Transactions.ValidateAndPreprocess(cfg)
	if err != nil {
		return err
	}
	countSearches += searches
	countAggregates += aggregates

	for _, aggregates := range r.ParallelGroups {
		count, err := aggregates.ValidateAndPreprocess(cfg)
		if err != nil {
			return err
		}
		countAggregates += count
	}

	if err := common.CheckMaxSearches(cfg, countSearches); err != nil {
		return err
	}
	if err := common.CheckMaxAggregates(cfg, countAggregates); err != nil {
		return err
	}
	return nil
}
