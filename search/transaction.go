package search

import (
	"context"
	"database/sql"
	stdsql "database/sql"
	"errors"
	"fmt"
)

func WithTx[T any](
	ctx context.Context,
	client Client,
	txOpts *stdsql.TxOptions,
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

type TxQueryGroup struct {
	TransactionIsolationLevel *stdsql.IsolationLevel `json:"transaction_isolation_level,omitempty"`
	QueryGroup
}

func (group *TxQueryGroup) Execute(
	ctx context.Context,
	client Client,
	graph Graph,
	cfg *Config,
) (*GroupResponse, error) {
	if err := group.ValidateAndPreprocessFinal(cfg); err != nil {
		return nil, err
	}

	build, err := group.Build(cfg, graph)
	if err != nil {
		return nil, err
	}

	return group.execute(ctx, client, graph, cfg, build)
}

func (group *TxQueryGroup) execute(
	ctx context.Context,
	client Client,
	graph Graph,
	cfg *Config,
	build *TxQueryGroupBuild,
) (*GroupResponse, error) {
	scalars, paginations := group.prepareScalars(build)

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

func (group *TxQueryGroup) prepareScalars(build *TxQueryGroupBuild) (scalars []*ScalarQuery, pagMap map[string]*PaginateInfos) {
	pagCount := build.CountPaginations()
	aggCount := len(build.Aggregates)

	if pagCount > 0 {
		pagMap = make(map[string]*PaginateInfos, pagCount)
	}

	if scalarSize := pagCount + aggCount; scalarSize > 0 {
		scalars = make([]*ScalarQuery, 0, scalarSize)
	}

	for i, s := range build.Searches {
		if p := s.Paginate; p != nil {
			pagMap[s.Key] = p
			scalars[i] = p.ToScalarQuery(s.Key)
		}
	}

	if aggCount > 0 {
		scalars = append(scalars, build.Aggregates...)
	}
	return
}

type TxQueryGroupBuild struct {
	IsolationLevel sql.IsolationLevel
	QueryGroupBuild
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
	if err := groups.ValidateAndPreprocessFinal(cfg); err != nil {
		return nil, err
	}

	searches, aggregates, err := groups.execute(ctx, client, graph, cfg)
	if err != nil {
		return nil, err
	}

	response := new(GroupResponse)

	if len(searches) > 0 {
		response.Searches = searches
	}

	if len(aggregates) > 0 {
		response.Meta = &AggregatesMeta{
			Aggregates: aggregates,
		}
	}

	return response, nil
}

func (groups TxQueryGroups) execute(
	ctx context.Context,
	client Client,
	graph Graph,
	cfg *Config,
) (SearchesResponse, AggregatesResponse, error) {
	builds, err := groups.Build(cfg, graph)
	if err != nil {
		return nil, nil, err
	}

	return nil, nil, nil
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
