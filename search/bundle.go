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

func (r *QueryBundle) Execute(
	ctx context.Context,
	client entx.Client,
	graph entx.Graph,
	cfg *Config,
) (*GroupResponse, error) {
	ctx, cancel := common.ContextTimeout(ctx, cfg.RequestTimeout)
	defer cancel()

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
