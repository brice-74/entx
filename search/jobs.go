package search

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

type Job[R any] func(ctx context.Context) (R, error)

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
		exec := exec
		if ctx.Err() != nil {
			break
		}

		wg.Go(func() error {
			ctx, cancel := context.WithTimeout(ctx, timeout)
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

type ScalarGroup []*ScalarQuery

type ScalarJob = Job[map[string]any]

func (g ScalarGroup) ToJob(client Client) ScalarJob {
	return func(ctx context.Context) (map[string]any, error) {
		return ExecuteScalars(ctx, client, g...)
	}
}

type TransactionGroup struct {
	IsolationLevel sql.IsolationLevel
	Searches       []*CompositeSearch
	Aggregates     ScalarGroup
}

type TransactionGroupResponse struct {
	Searches   map[string]*SearchResponse
	Aggregates map[string]any
}

type TransactionJob = Job[*TransactionGroupResponse]

func (g *TransactionGroup) prepareScalars() ([]*ScalarQuery, map[string]*PaginateInfos) {
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

func (g *TransactionGroup) attachPagination(
	res *TransactionGroupResponse,
	pagMap map[string]*PaginateInfos,
) error {
	for key, p := range pagMap {
		raw, ok := res.Aggregates[key]
		if !ok {
			return fmt.Errorf("missing paginate count for '%s'", key)
		}
		cnt, ok := raw.(int64)
		if !ok {
			return fmt.Errorf("paginate count wrong type for '%s': %T", key, raw)
		}
		sr, exist := res.Searches[key]
		if !exist {
			return fmt.Errorf("search response not found for paginate on key '%s'", key)
		}
		sr.Meta.Paginate = p.Calculate(int(cnt), sr.Meta.Count)
		delete(res.Aggregates, key)
	}
	return nil
}

func (g *TransactionGroup) executeTx(
	ctx context.Context,
	client Client,
	scalars []*ScalarQuery,
) (*TransactionGroupResponse, error) {
	return WithTx(ctx,
		client,
		&sql.TxOptions{
			ReadOnly:  true,
			Isolation: g.IsolationLevel,
		}, func(ctx context.Context, tx Client) (*TransactionGroupResponse, error) {
			res := TransactionGroupResponse{}

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

				res.Searches[s.Key] = &SearchResponse{Data: data, Meta: SearchMeta{Count: count}}
			}

			scalarsRes, err := ExecuteScalars(ctx, tx, scalars...)
			if err != nil {
				return nil, err
			}

			res.Aggregates = scalarsRes

			return nil, err
		})
}

func (g *TransactionGroup) ToJob(client Client) TransactionJob {
	return func(ctx context.Context) (*TransactionGroupResponse, error) {
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
