package search

import (
	"context"
)

type Composite struct {
	graph  Graph
	client Client
	conf   Config
}

func NewComposite(
	graph Graph,
	client Client,
	conf Config,
) *Composite {
	return &Composite{
		graph,
		client,
		conf,
	}
}

func (c *Composite) Exec(ctx context.Context, req *CompositeRequest) (*CompositeResponse, error) {
	ctx, cancel := contextTimeout(ctx, c.conf.RequestTimeout)
	defer cancel()

	if err := req.ValidateAndPreprocess(&c.conf); err != nil {
		return nil, err
	}

	var (
		searches = make([]*CompositeSearch, 0, len(req.Searches))
		scalars  []*ScalarQuery
	)
	for i, s := range req.Searches {
		s, err := s.PrepareComposite(i, &c.conf, c.graph, c.client)
		if err != nil {
			return nil, err
		}
		if s.Paginate != nil {
			scalars = append(scalars, s.Paginate.ToScalarQuery(s.Key))
		}
		searches[i] = s
	}

	for _, a := range req.Aggregates {
		agg, err := a.Prepare(c.graph)
		if err != nil {
			return nil, err
		}

		scalars = append(scalars, agg.ToScalarQuery())
	}

	return &Response{Searches: searches, Meta: GlobalAggregatesMeta{aggregates}}, nil
}
