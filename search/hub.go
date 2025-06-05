package search

import (
	"context"
	"fmt"
	"maps"
	"sync"

	"entgo.io/ent/dialect/sql"
	"golang.org/x/sync/errgroup"
)

type Hub struct {
	graph  map[string]Node
	client Client
	conf   Config
}

func NewHub(
	graph map[string]Node,
	client Client,
	conf Config,
) *Hub {
	return &Hub{
		graph,
		client,
		conf,
	}
}

func (h *Hub) Exec(ctx context.Context, req *HubRequest) (*Response, error) {
	ctx, cancel := contextTimeout(ctx, h.conf.RequestTimeout)
	defer cancel()

	searchJobs, countInfos, scalarGroups, err := h.prepareJobs(req)
	if err != nil {
		return nil, err
	}

	wg, wgctx := errgroup.WithContext(ctx)
	wg.SetLimit(h.conf.MaxParallelWorkersPerRequest)
	searches := h.runSearchJobs(wgctx, searchJobs, wg)
	scalars := h.runScalarJobs(wgctx, scalarGroups, wg)
	if err := wg.Wait(); err != nil {
		return nil, err
	}

	fillPagination(searches, countInfos, scalars)
	aggregates := h.extractAggregates(req.Aggregates, scalars)

	return &Response{Searches: searches, Meta: GlobalAggregatesMeta{aggregates}}, nil
}

type searchJob struct {
	key    string
	execFn func(context.Context) (any, int, error)
}

type countInfo struct {
	key         string
	selector    *sql.Selector
	limit, page int
}

func (h *Hub) prepareJobs(req *HubRequest) (
	[]searchJob,
	[]countInfo,
	[][]ScalarQuery,
	error,
) {
	var (
		searches []searchJob
		counts   []countInfo
		scalars  []ScalarQuery
	)

	if err := req.ValidateAndPreprocess(&h.conf); err != nil {
		return nil, nil, nil, err
	}

	for i, s := range req.Searches {
		key := s.Key
		if key == "" {
			key = fmt.Sprintf("s%d", i+1)
		}
		execFn, countSel, err := s.Prepare(&h.conf, h.graph, h.client)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("prepare search %q: %w", key, err)
		}
		searches = append(searches, searchJob{key, execFn})
		if countSel != nil {
			counts = append(counts, countInfo{key, countSel, s.Limit, s.Page})
			scalars = append(scalars, ScalarQuery{countSel, "count_" + key, new(sql.NullInt64)})
		}
	}

	var (
		forcedGroup = make(map[string][]ScalarQuery)
		err         error
	)
	for _, oa := range req.Aggregates {
		var sq ScalarQuery
		sq.Selector, sq.Key, err = oa.Build(h.graph)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("build aggregate %q: %w", oa.Field, err)
		}
		if oa.Type == AggCount {
			sq.Dest = new(sql.NullInt64)
		} else {
			sq.Dest = new(sql.NullFloat64)
		}

		if groupName := oa.ParralelGroup; groupName != "" {
			if _, exist := forcedGroup[groupName]; !exist {
				forcedGroup[groupName] = make([]ScalarQuery, 0, 1)
			}
			forcedGroup[groupName] = append(forcedGroup[groupName], sq)
			continue
		}

		scalars = append(scalars, sq)
	}

	scalarGroups := splitInChunks(scalars, h.conf.ScalarQueriesChunkSize)
	for _, v := range forcedGroup {
		scalarGroups = append(scalarGroups, v)
	}

	return searches, counts, scalarGroups, nil
}

func (h *Hub) runSearchJobs(
	ctx context.Context,
	jobs []searchJob,
	wg *errgroup.Group,
) map[string]*SearchResponse {
	results := make(map[string]*SearchResponse, len(jobs))
	var mu sync.Mutex
	for _, job := range jobs {
		job := job
		if ctx.Err() != nil {
			break
		}
		wg.Go(func() error {
			ctx, cancel := contextTimeout(ctx, h.conf.SearchQueryTimeout)
			defer cancel()

			data, count, err := job.execFn(ctx)
			if err != nil {
				return err
			}
			mu.Lock()
			results[job.key] = &SearchResponse{
				Data: data,
				Meta: SearchMeta{
					Count: count,
				},
			}
			mu.Unlock()
			return nil
		})
	}
	return results
}

func (h *Hub) runScalarJobs(
	ctx context.Context,
	scalarBatches [][]ScalarQuery,
	wg *errgroup.Group,
) map[string]any {
	agg := make(map[string]any, len(scalarBatches))
	var mu sync.Mutex
	for _, batch := range scalarBatches {
		batch := batch
		if ctx.Err() != nil {
			break
		}
		wg.Go(func() error {
			ctx, cancel := contextTimeout(ctx, h.conf.ScalarQueryTimeout)
			defer cancel()

			res, err := NewScalarExecutor(len(batch)).
				AddQ(batch...).
				Execute(ctx, h.client)
			if err != nil {
				return err
			}

			mu.Lock()
			maps.Copy(agg, res)
			mu.Unlock()
			return nil
		})
	}
	return agg
}

func fillPagination(
	searches map[string]*SearchResponse,
	counts []countInfo,
	aggMap map[string]any,
) {
	for _, ci := range counts {
		sr, exist := searches[ci.key]
		if !exist {
			continue
		}
		raw, exist := aggMap["count_"+ci.key]
		if !exist {
			continue
		}
		total := int(raw.(int64))
		sr.Meta.Paginate = CalcPaginate(total, ci.limit, ci.page, sr.Meta.Count)
	}
}

func (h *Hub) extractAggregates(
	overall []OverallAggregate,
	aggMap map[string]any,
) map[string]any {
	out := make(map[string]any, len(overall))
	for _, oa := range overall {
		alias := oa.Alias
		if v, ok := aggMap[alias]; ok {
			out[alias] = v
		}
	}
	return out
}

func splitInChunks[T any](input []T, batchSize int) [][]T {
	if batchSize <= 0 || batchSize >= len(input) {
		if len(input) == 0 {
			return nil
		}
		return [][]T{input}
	}
	var chunks [][]T
	for i := 0; i < len(input); i += batchSize {
		end := min(i+batchSize, len(input))
		chunks = append(chunks, input[i:end])
	}
	return chunks
}
