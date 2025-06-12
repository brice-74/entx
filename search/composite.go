package search

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

type Composite struct {
	graph  Graph
	client Client
	conf   *Config
}

func NewComposite(
	graph Graph,
	client Client,
	conf *Config,
) *Composite {
	return &Composite{
		graph,
		client,
		conf,
	}
}

func (c *Composite) Exec(ctx context.Context, req *RequestBundle) (*CompositeResponse, error) {
	ctx, cancel := contextTimeout(ctx, c.conf.RequestTimeout)
	defer cancel()

	totalAggregates, totalSearches, err := req.ValidateAndPreprocess(c.conf)
	if err != nil {
		return nil, err
	}

	var txGroups []*TransactionGroup
	if txLen := len(req.Transactions) + len(req.Searches); txLen > 0 {
		txGroups = make([]*TransactionGroup, 0, txLen)

		for i, s := range req.Searches {
			composite, err := s.PrepareComposite(i, c.conf, c.graph)
			if err != nil {
				return nil, err
			}
			txGroups[i] = &TransactionGroup{
				Searches: []*CompositeSearch{composite},
			}
		}

		lenTxGroups := len(txGroups)
		for i, t := range req.Transactions {
			txGroup := &TransactionGroup{
				IsolationLevel: c.conf.Transaction.IsolationLevel,
			}

			if t.TransactionIsolationLevel != nil {
				txGroup.IsolationLevel = sql.IsolationLevel(*t.TransactionIsolationLevel)
			}

			if lenSearches := len(t.Searches); lenSearches > 0 {
				txGroup.Searches = make([]*CompositeSearch, 0, len(t.Searches))

				for i, s := range t.Searches {
					composite, err := s.PrepareComposite(i, c.conf, c.graph)
					if err != nil {
						return nil, err
					}
					txGroup.Searches[i] = composite
				}
			}

			if lenAggregates := len(t.Aggregates); lenAggregates > 0 {
				txGroup.Aggregates = make(ScalarGroup, 0, len(t.Aggregates))

				for i, a := range t.Aggregates {
					scalar, err := a.PrepareScalar(c.graph)
					if err != nil {
						return nil, err
					}
					txGroup.Aggregates[i] = scalar
				}
			}

			txGroups[i+lenTxGroups] = txGroup
		}
	}

	var aggChunkedGroups []ScalarGroup
	if lenAggGroups, lenAgg := len(req.ParralelGroups), len(req.Aggregates); lenAggGroups+lenAgg > 0 {
		chunkSize := c.conf.ScalarQueriesChunkSize

		aggChunkedGroups = make([]ScalarGroup, 0, DivCeil(lenAgg, chunkSize)+lenAggGroups)

		var scalars = make(ScalarGroup, 0, len(req.Aggregates))
		for i, a := range req.Aggregates {
			scalar, err := a.PrepareScalar(c.graph)
			if err != nil {
				return nil, err
			}
			scalars[i] = scalar
		}

		aggChunkedGroups = append(aggChunkedGroups, splitInChunks(scalars, chunkSize)...)

		for _, group := range req.ParralelGroups {
			var scalars = make(ScalarGroup, 0, len(req.Aggregates))
			for i, a := range group {
				scalar, err := a.PrepareScalar(c.graph)
				if err != nil {
					return nil, err
				}
				scalars[i] = scalar
			}
			aggChunkedGroups = append(aggChunkedGroups, scalars)
		}
	}

	wg, wgctx := errgroup.WithContext(ctx)
	wg.SetLimit(c.conf.MaxParallelWorkersPerRequest)

	var txGroupsJobs = make([]TransactionJob, 0, len(txGroups))
	for i, g := range txGroups {
		txGroupsJobs[i] = g.ToJob(i, c.client)
	}

	txGroupsRes := RunJobs(wgctx, txGroupsJobs, wg, 5*time.Second)

	var aggChunkedGroupsJobs = make([]ScalarJob, 0, len(aggChunkedGroups))
	for i, g := range aggChunkedGroups {
		aggChunkedGroupsJobs[i] = g.ToJob(i, c.client)
	}

	aggRes := RunJobs(wgctx, aggChunkedGroupsJobs, wg, 5*time.Second)

	if err := wg.Wait(); err != nil {
		return nil, err
	}

	var (
		searchesRes   = make(map[string]*SearchResponse, totalSearches)
		aggregatesRes = make(map[string]any, totalAggregates)
	)
	for _, res := range txGroupsRes {
		maps.Copy(searchesRes, res.Searches)
		maps.Copy(aggregatesRes, res.Aggregates)
	}

	for _, res := range aggRes {
		maps.Copy(aggregatesRes, res)
	}

	return &CompositeResponse{Searches: searchesRes, Meta: GlobalAggregatesMeta{aggregatesRes}}, nil
}

type ScalarJob = Job[int, map[string]any]

type ScalarGroup []*ScalarQuery

func (g ScalarGroup) ToJob(key int, client Client) ScalarJob {
	return ScalarJob{
		Key: key,
		Exec: func(ctx context.Context) (map[string]any, error) {
			return ExecuteScalars(ctx, client, g...)
		},
	}
}

type TransactionJob = Job[int, *TransactionGroupResponse]

type TransactionGroupResponse struct {
	Searches   map[string]*SearchResponse
	Aggregates map[string]any
}

type TransactionGroup struct {
	IsolationLevel sql.IsolationLevel
	Searches       []*CompositeSearch
	Aggregates     []*ScalarQuery
}

func (g *TransactionGroup) ToJob(key int, client Client) TransactionJob {
	return TransactionJob{
		Key: key,
		Exec: func(ctx context.Context) (*TransactionGroupResponse, error) {
			return WithTx(ctx, client, g.IsolationLevel,
				func(client Client) (*TransactionGroupResponse, error) {
					res := TransactionGroupResponse{}

					if length := len(g.Searches); length > 0 {
						res.Searches = make(map[string]*SearchResponse, length)
					}

					if length := len(g.Aggregates); length > 0 {
						res.Aggregates = make(map[string]any, length)
					}

					scalarQueries := g.Aggregates

					var paginates = make(map[string]*PaginateInfos)
					for _, s := range g.Searches {
						data, length, err := s.ExecFn(ctx, client)
						if err != nil {
							return nil, err
						}
						res.Searches[s.Key] = &SearchResponse{
							Data: data,
							Meta: SearchMeta{
								Count: length,
							},
						}

						if p := s.Paginate; p != nil {
							paginates[s.Key] = p
							scalarQueries = append(scalarQueries, p.ToScalarQuery(s.Key))
						}
					}

					scalarRes, err := ExecuteScalars(ctx, client, scalarQueries...)
					if err != nil {
						return nil, err
					}

					for key, p := range paginates {
						raw, exist := scalarRes[key]
						if !exist {
							return nil, fmt.Errorf("scalare result do not contain paginate count on key '%s'", key)
						}
						defer delete(scalarRes, key)
						count, ok := raw.(int64)
						if !ok {
							return nil, fmt.Errorf("paginate count type on key '%s' is not 'int64', got '%T'", key, raw)
						}
						sr, exist := res.Searches[key]
						if !exist {
							return nil, fmt.Errorf("search response not found for paginate on key '%s'", key)
						}
						sr.Meta.Paginate = p.Calculate(int(count), sr.Meta.Count)
					}

					res.Aggregates = scalarRes

					return &res, nil
				},
			)
		},
	}
}

type Job[K comparable, R any] struct {
	Key  K
	Exec func(ctx context.Context) (R, error)
}

func RunJobs[K comparable, R any](
	ctx context.Context,
	jobs []Job[K, R],
	wg *errgroup.Group,
	timeout time.Duration,
) map[K]R {
	results := make(map[K]R, len(jobs))
	var mu sync.Mutex

	for _, job := range jobs {
		job := job
		if ctx.Err() != nil {
			break
		}

		wg.Go(func() error {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			res, err := job.Exec(ctx)
			if err != nil {
				return err
			}

			mu.Lock()
			results[job.Key] = res
			mu.Unlock()
			return nil
		})
	}

	return results
}

type SliceAlias[T any] interface {
	~[]T
}

func splitInChunks[SliceT SliceAlias[ElemT], ElemT any](input SliceT, batchSize int) []SliceT {
	if batchSize <= 0 || batchSize >= len(input) {
		if len(input) == 0 {
			return nil
		}
		return []SliceT{input}
	}
	var chunks []SliceT
	for i := 0; i < len(input); i += batchSize {
		end := min(i+batchSize, len(input))
		chunks = append(chunks, input[i:end])
	}
	return chunks
}
