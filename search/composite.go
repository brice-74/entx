package search

import (
	"context"
	"database/sql"
	"fmt"
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

	for _, txGroup := range txGroups {
		txGroup.ToJob(c.client)
	}

	if err := wg.Wait(); err != nil {
		return nil, err
	}

	return &CompositeResponse{Searches: searches, Meta: GlobalAggregatesMeta{aggregates}}, nil
}

type ScalarGroup []*ScalarQuery

func (g ScalarGroup) ToJob(client Client) Job[map[string]any] {
	return Job[map[string]any]{
		Exec: func(ctx context.Context) (map[string]any, error) {
			return ExecuteScalars(ctx, client, g...)
		},
	}
}

type TransactionGroupResponse struct {
	Searches   map[string]*SearchResponse
	Aggregates GlobalAggregatesMeta
}

type TransactionGroup struct {
	IsolationLevel sql.IsolationLevel
	Searches       []*CompositeSearch
	Aggregates     []*ScalarQuery
}

func (g *TransactionGroup) ToJob(client Client) Job[*TransactionGroupResponse] {
	return Job[*TransactionGroupResponse]{
		Exec: func(ctx context.Context) (*TransactionGroupResponse, error) {
			return WithTx(ctx, client, g.IsolationLevel,
				func(client Client) (*TransactionGroupResponse, error) {
					res := TransactionGroupResponse{}

					if length := len(g.Searches); length > 0 {
						res.Searches = make(map[string]*SearchResponse, length)
					}

					if length := len(g.Aggregates); length > 0 {
						res.Aggregates = AggregatesMeta{
							Aggregates: make(map[string]any, length),
						}
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

					res.Aggregates = AggregatesMeta{
						Aggregates: scalarRes,
					}

					return &res, nil
				},
			)
		},
	}
}

type Job[R any] struct {
	Exec func(ctx context.Context) (R, error)
}

func RunJob[R any](
	ctx context.Context,
	job Job[R],
	wg *errgroup.Group,
	timeout time.Duration,
) R {
	var result R

	if ctx.Err() != nil {
		return result
	}

	wg.Go(func() (err error) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		result, err = job.Exec(ctx)
		if err != nil {
			return err
		}

		return nil
	})

	return result
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
