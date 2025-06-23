package search

import (
	"context"

	"github.com/brice-74/entx"
	"github.com/brice-74/entx/search/common"
)

type NamedQueries []*NamedQuery

func (queries NamedQueries) Execute(
	ctx context.Context,
	client entx.Client,
	graph entx.Graph,
	cfg *Config,
) (SearchesResponse, error) {
	ctx, cancel := common.ContextTimeout(ctx, cfg.RequestTimeout)
	defer cancel()

	if err := queries.ValidateAndPreprocessFinal(cfg); err != nil {
		return nil, err
	}

	build, err := queries.BuildClassified(cfg, graph)
	if err != nil {
		return nil, err
	}

	res, err := build.Execute(ctx, client, cfg)
	if err != nil {
		return nil, err
	}

	return res.Searches, nil
}

func (queries NamedQueries) BuildClassified(
	cfg *Config,
	graph entx.Graph,
) (*ClassifiedBuilds, error) {
	builds := new(ClassifiedBuilds)
	for i, q := range queries {
		build, err := q.Build(i, cfg, graph)
		if err != nil {
			return nil, err
		}
		if build.IsPaginatedWithTx() {
			builds.Transactions = append(builds.Transactions, build.ToTxQueryGroupBuild())
			continue
		}

		builds.Searches = append(builds.Searches, build)
	}
	return builds, nil
}

func (queries NamedQueries) Build(conf *Config, graph entx.Graph) ([]*NamedQueryBuild, error) {
	var builds = make([]*NamedQueryBuild, 0, len(queries))
	for i, q := range queries {
		build, err := q.Build(i, conf, graph)
		if err != nil {
			return nil, err
		}
		builds[i] = build
	}
	return builds, nil
}

func (queries NamedQueries) ValidateAndPreprocessFinal(cfg *Config) error {
	count, err := queries.ValidateAndPreprocess(cfg)
	if err != nil {
		return err
	}

	return common.CheckMaxSearches(cfg, count)
}

func (queries NamedQueries) ValidateAndPreprocess(cfg *Config) (count int, err error) {
	for _, q := range queries {
		if err = q.ValidateAndPreprocess(cfg); err != nil {
			return
		}
		count++
	}
	return
}
