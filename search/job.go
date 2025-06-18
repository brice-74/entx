package search

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

type Jobifiable[R any] interface {
	ToJob(Client) Job[R]
}

type Job[R any] func(ctx context.Context) (R, error)

func RunJob[R any](
	ctx context.Context,
	job Job[R],
	wg *errgroup.Group,
	timeout time.Duration,
) (res R) {
	if ctx.Err() != nil {
		return
	}

	wg.Go(func() (err error) {
		ctx, cancel := contextTimeout(ctx, timeout)
		defer cancel()

		res, err = job(ctx)
		if err != nil {
			return
		}
		return
	})
	return
}

func RunJobs[R any](
	ctx context.Context,
	jobs []Job[R],
	wg *errgroup.Group,
	timeout time.Duration,
) []R {
	if len(jobs) <= 0 {
		return nil
	}

	results := make([]R, len(jobs))
	var mu sync.Mutex

	for i, exec := range jobs {
		if ctx.Err() != nil {
			break
		}

		wg.Go(func() error {
			ctx, cancel := contextTimeout(ctx, timeout)
			defer cancel()

			res, err := exec(ctx)
			if err != nil {
				return err
			}

			mu.Lock()
			results[i] = res
			mu.Unlock()
			return nil
		})
	}

	return results
}

func IterRunJobs[R any](
	ctx context.Context,
	jobifiables []Jobifiable[R],
	wg *errgroup.Group,
	timeout time.Duration,
	client Client,
) []R {
	if lenght := len(jobifiables); lenght > 0 {
		var jobs = make([]Job[R], 0, lenght)
		for i, g := range jobifiables {
			jobs[i] = g.ToJob(client)
		}

		return RunJobs(ctx, jobs, wg, timeout)
	}
	return nil
}

type ScalarGroup []*ScalarQuery
type ScalarGroupJob = Job[map[string]any]
type JobifiableScalarGroup = Jobifiable[map[string]any]

func (g ScalarGroup) ToJob(client Client) ScalarGroupJob {
	return func(ctx context.Context) (map[string]any, error) {
		return ExecuteScalars(ctx, client, g...)
	}
}

/* ------------------- */

type StandaloneSearchJob = Job[*SearchResponse]
type StandaloneSearch func(context.Context, Client) (any, int, error)

func (fn StandaloneSearch) ToJob(client Client) StandaloneSearchJob {
	return func(ctx context.Context) (*SearchResponse, error) {
		data, count, err := fn(ctx, client)
		if err != nil {
			return nil, err
		}
		return &SearchResponse{Data: data, Meta: &SearchMeta{Count: count}}, nil
	}
}

/* ------------------- */

type TxGroup struct {
	IsolationLevel sql.IsolationLevel
	Searches       []*NamedQueryBuild
	Aggregates     []*ScalarQuery
}
type TxGroupJobResponse struct {
	Searches   map[string]*SearchResponse
	Aggregates map[string]any
}
type TxGroupJob = Job[*TxGroupJobResponse]
type JobifiableTxGroup = Jobifiable[*TxGroupJobResponse]

func (g *TxGroup) ToJob(client Client) TxGroupJob {
	return func(ctx context.Context) (*TxGroupJobResponse, error) {
		scalarQ, pagMap := g.prepareScalars()

		res, err := g.executeTx(ctx, client, scalarQ)
		if err != nil {
			return nil, err
		}

		if err := g.attachPagination(res, pagMap); err != nil {
			return nil, err
		}

		return res, nil
	}
}

func (g *TxGroup) prepareScalars() ([]*ScalarQuery, map[string]*PaginateInfos) {
	var pagCount int
	for _, s := range g.Searches {
		if s.Paginate != nil {
			pagCount++
		}
	}
	scalars := make([]*ScalarQuery, 0, len(g.Aggregates)+pagCount)
	pagMap := make(map[string]*PaginateInfos, pagCount)
	for _, s := range g.Searches {
		if p := s.Paginate; p != nil {
			pagMap[s.Key] = p
			scalars = append(scalars, p.ToScalarQuery(s.Key))
		}
	}
	return scalars, pagMap
}

func (g *TxGroup) executeTx(
	ctx context.Context,
	client Client,
	scalars []*ScalarQuery,
) (*TxGroupJobResponse, error) {
	res, err := WithTx(ctx,
		client,
		&sql.TxOptions{
			ReadOnly:  true,
			Isolation: g.IsolationLevel,
		}, func(ctx context.Context, tx Client) (*TxGroupJobResponse, error) {
			res := TxGroupJobResponse{}

			if length := len(g.Searches); length > 0 {
				res.Searches = make(map[string]*SearchResponse, length)
			}
			if length := len(g.Aggregates); length > 0 {
				res.Aggregates = make(map[string]any, length)
			}

			for _, s := range g.Searches {
				data, count, err := s.ExecFn(ctx, tx)
				if err != nil {
					return nil, err
				}

				res.Searches[s.Key] = &SearchResponse{Data: data, Meta: &SearchMeta{Count: count}}
			}

			scalarsRes, err := ExecuteScalars(ctx, tx, scalars...)
			if err != nil {
				return nil, err
			}

			res.Aggregates = scalarsRes

			return nil, err
		})

	if err != nil {
		return nil, &ExecError{
			Op:  "TxGroup.executeTx",
			Err: err,
		}
	}
	return res, nil
}

func (g *TxGroup) attachPagination(
	res *TxGroupJobResponse,
	pagMap map[string]*PaginateInfos,
) error {
	for key, p := range pagMap {
		raw, ok := res.Aggregates[key]
		if !ok {
			return &ExecError{
				Op:  "TxGroup.attachPagination",
				Err: fmt.Errorf("missing paginate count for '%s'", key),
			}
		}
		cnt, ok := raw.(int64)
		if !ok {
			return &ExecError{
				Op:  "TxGroup.attachPagination",
				Err: fmt.Errorf("paginate count wrong type for '%s': %T", key, raw),
			}

		}
		sr, exist := res.Searches[key]
		if !exist {
			return &ExecError{
				Op:  "TxGroup.attachPagination",
				Err: fmt.Errorf("search response not found for paginate on key '%s'", key),
			}
		}
		sr.Meta.Paginate = p.Calculate(int(cnt), sr.Meta.Count)
		delete(res.Aggregates, key)
	}
	return nil
}
