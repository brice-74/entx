package search

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
)

func WithTx[T any](
	ctx context.Context,
	client Client,
	txOpts *sql.TxOptions,
	fn func(ctx context.Context, client Client) (T, error),
) (T, error) {
	var zero T

	tx, clientTx, err := client.Tx(ctx, txOpts)
	if err != nil {
		return zero, err
	}
	defer func() {
		if v := recover(); v != nil {
			panic(rollback(tx, err))
		}
	}()
	res, err := fn(ctx, clientTx)
	if err != nil {
		return zero, rollback(tx, err)
	}
	if err := tx.Commit(); err != nil {
		return zero, fmt.Errorf("committing transaction: %w", err)
	}
	return res, nil
}

func rollback(tx Transaction, err error) error {
	if rerr := tx.Rollback(); rerr != nil {
		err = fmt.Errorf("%w: rolling back transaction: %v", err, rerr)
	}
	return err
}

type TxQueryGroupBuild struct {
	IsolationLevel sql.IsolationLevel
	QueryGroupBuild
}

func (build *TxQueryGroupBuild) execute(
	ctx context.Context,
	client Client,
	cfg *Config,
) (*GroupResponse, error) {
	scalars, paginations := build.prepareScalars()

	res, err := WithTx(ctx,
		client,
		&sql.TxOptions{
			ReadOnly:  true,
			Isolation: build.IsolationLevel,
		}, func(ctx context.Context, tx Client) (*GroupResponse, error) {
			res := GroupResponse{}

			if length := len(build.Searches); length > 0 {
				res.Searches = make(map[string]*SearchResponse, length)
			}

			for _, s := range build.Searches {
				ctx, cancel := contextTimeout(ctx, cfg.QueryTimeout)
				defer cancel()

				data, count, err := s.ExecFn(ctx, tx)
				if err != nil {
					return nil, err
				}

				res.Searches[s.Key] = &SearchResponse{Data: data, Meta: &SearchMeta{Count: count}}
			}

			if countScalars := len(scalars); countScalars > 0 {
				scalarsRes, err := ExecuteScalarGroupsSync(ctx,
					client,
					cfg,
					len(scalars),
					splitInChunks(scalars, cfg.ScalarQueriesChunkSize)...,
				)
				if err != nil {
					return nil, err
				}

				res.Meta = &AggregatesMeta{Aggregates: scalarsRes}
			}

			return &res, nil
		})
	if err != nil {
		return nil, &ExecError{
			Op:  "TxQueryGroup.execute",
			Err: err,
		}
	}

	if err := attachPagination(res, paginations); err != nil {
		return nil, err
	}

	return res, nil
}

func (build *TxQueryGroupBuild) prepareScalars() (scalars []*ScalarQuery, pagMap map[string]*PaginateInfos) {
	pagCount := build.CountPaginations()
	aggCount := len(build.Aggregates)

	if pagCount > 0 {
		pagMap = make(map[string]*PaginateInfos, pagCount)
	}

	if scalarSize := pagCount + aggCount; scalarSize > 0 {
		scalars = make([]*ScalarQuery, scalarSize)
	}

	idx := 0
	for _, s := range build.Searches {
		if p := s.Paginate; p != nil {
			pagMap[s.Key] = p
			scalars[idx] = p.ToScalarQuery(s.Key)
			idx++
		}
	}

	copy(scalars[idx:], build.Aggregates)
	return
}

type TxQueryGroup struct {
	TransactionIsolationLevel *sql.IsolationLevel `json:"transaction_isolation_level,omitempty"`
	QueryGroup
}

func (group *TxQueryGroup) Execute(
	ctx context.Context,
	client Client,
	graph Graph,
	cfg *Config,
) (*GroupResponse, error) {
	ctx, cancel := contextTimeout(ctx, cfg.RequestTimeout)
	defer cancel()

	if err := group.ValidateAndPreprocessFinal(cfg); err != nil {
		return nil, err
	}

	build, err := group.Build(cfg, graph)
	if err != nil {
		return nil, err
	}

	return build.execute(ctx, client, cfg)
}

func (r *TxQueryGroup) Build(cfg *Config, graph Graph) (*TxQueryGroupBuild, error) {
	txBuild := new(TxQueryGroupBuild)
	if r.TransactionIsolationLevel != nil {
		txBuild.IsolationLevel = *r.TransactionIsolationLevel
	} else {
		txBuild.IsolationLevel = cfg.Transaction.IsolationLevel
	}

	build, err := r.QueryGroup.Build(cfg, graph)
	if err != nil {
		return nil, err
	}

	txBuild.QueryGroupBuild = *build

	return txBuild, nil
}

func (tr *TxQueryGroup) ValidateAndPreprocessFinal(cfg *Config) (err error) {
	var countAggregates, countSearches int
	if countAggregates, countSearches, err = tr.QueryGroup.ValidateAndPreprocess(cfg); err != nil {
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

func (tr *TxQueryGroup) ValidateAndPreprocess(c *Config) (countAggregates, countSearches int, err error) {
	if len(tr.Searches)+len(tr.Aggregates) <= 1 {
		return 0, 0, &ValidationError{
			Rule: "TransactionUnnecessary",
			Err:  errors.New("transaction with a single search or one aggregate is unnecessary"),
		}
	}
	if tr.TransactionIsolationLevel != nil && !c.Transaction.AllowClientIsolationLevel {
		return 0, 0, &ValidationError{
			Rule: "TransactionClientIsolationLevelDisallow",
			Err:  errors.New("transaction_isolation_level parameter is not allowed"),
		}
	}
	return tr.QueryGroup.ValidateAndPreprocess(c)
}

type TxQueryGroups []*TxQueryGroup

func (groups TxQueryGroups) Execute(
	ctx context.Context,
	client Client,
	graph Graph,
	cfg *Config,
) (*GroupResponse, error) {
	ctx, cancel := contextTimeout(ctx, cfg.RequestTimeout)
	defer cancel()

	if err := groups.ValidateAndPreprocessFinal(cfg); err != nil {
		return nil, err
	}

	builds, err := groups.Build(cfg, graph)
	if err != nil {
		return nil, err
	}

	return groups.execute(ctx, client, cfg, builds)
}

func (groups TxQueryGroups) execute(
	ctx context.Context,
	client Client,
	cfg *Config,
	builds []*TxQueryGroupBuild,
) (*GroupResponse, error) {
	var totalSearches, totalAggs int
	responses := make([]*GroupResponse, len(builds))
	for i, build := range builds {
		res, err := build.execute(ctx, client, cfg)
		if err != nil {
			return nil, err
		}
		responses[i] = res
		totalSearches += len(res.Searches)
		if res.Meta != nil {
			totalAggs += len(res.Meta.Aggregates)
		}
	}

	final := &GroupResponse{}
	if totalSearches > 0 {
		final.Searches = make(SearchesResponse, totalSearches)
	}
	if totalAggs > 0 {
		final.Meta = &AggregatesMeta{
			Aggregates: make(AggregatesResponse, totalAggs),
		}
	}

	for _, res := range responses {
		maps.Copy(final.Searches, res.Searches)
		if res.Meta != nil {
			maps.Copy(final.Meta.Aggregates, res.Meta.Aggregates)
		}
	}
	return final, nil
}

func (groups TxQueryGroups) Build(cfg *Config, graph Graph) ([]*TxQueryGroupBuild, error) {
	var builds = make([]*TxQueryGroupBuild, 0, len(groups))
	for i, group := range groups {
		build, err := group.Build(cfg, graph)
		if err != nil {
			return nil, err
		}
		builds[i] = build
	}
	return builds, nil
}

func (groups TxQueryGroups) ValidateAndPreprocessFinal(cfg *Config) (err error) {
	var countSearches, countAggregates int
	if countSearches, countAggregates, err = groups.ValidateAndPreprocess(cfg); err != nil {
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

func (groups TxQueryGroups) ValidateAndPreprocess(cfg *Config) (countSearches, countAggregates int, err error) {
	for _, group := range groups {
		agg, search, err := group.ValidateAndPreprocess(cfg)
		if err != nil {
			return 0, 0, err
		}
		countAggregates += agg
		countSearches += search
	}
	return
}
