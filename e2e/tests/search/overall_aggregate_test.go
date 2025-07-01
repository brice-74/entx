package e2e_search_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/brice-74/entx/search"
	"github.com/brice-74/entx/search/common"
	"github.com/brice-74/entx/search/dsl"
	"github.com/stretchr/testify/require"
)

func TestOverallAggregatePanicPreprocessed(t *testing.T) {
	require.Panics(t, func() { (&dsl.OverallAggregate{}).Build(context.Background(), nil, "") })
}

func TestOverallAggregateValidationErrors(t *testing.T) {
	tests := []struct {
		aggregates   search.OverallAggregates
		expectedRule string
		cfg          *search.Config
	}{
		{
			aggregates:   dsl.OverallAggregates{{}},
			expectedRule: "AggregateFieldNotEmpty",
		},
		{
			aggregates:   dsl.OverallAggregates{{BaseAggregate: dsl.BaseAggregate{Type: dsl.AggCount, Field: "a.b.c"}}},
			expectedRule: "OverallAggregateFieldFormat",
		},
		{
			aggregates:   dsl.OverallAggregates{{BaseAggregate: dsl.BaseAggregate{Type: dsl.AggCount, Field: "a"}}, {BaseAggregate: dsl.BaseAggregate{Type: dsl.AggCount, Field: "a"}}},
			expectedRule: "MaxAggregatesPerRequest",
			cfg:          common.NewConfig(common.WithMaxAggregatesPerRequest(1)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.expectedRule, func(t *testing.T) {
			if tt.cfg == nil {
				tt.cfg = &common.DefaultConf
			}
			err := runExecutableErr(t, tt.aggregates, tt.cfg)
			var verr *search.ValidationError
			require.ErrorAs(t, err, &verr)
			require.Equal(t, tt.expectedRule, verr.Rule)
		})
	}
}

func TestOverallAggregateBuildErr(t *testing.T) {
	cases := []struct {
		name       string
		aggregates search.OverallAggregates
		assert     func(error)
	}{
		{
			name:       "ErrNodeNotExist",
			aggregates: search.OverallAggregates{{BaseAggregate: dsl.BaseAggregate{Type: dsl.AggCount, Field: "unknow"}}},
			assert: func(err error) {
				qerr := &common.QueryBuildError{Op: "OverallAggregate.resolveField", Err: fmt.Errorf(dsl.ErrNodeNotExist, "unknow")}
				require.EqualError(t, err, qerr.Error())
			},
		},
		{
			name:       "ErrNodeNotHaveField",
			aggregates: search.OverallAggregates{{BaseAggregate: dsl.BaseAggregate{Type: dsl.AggCount, Field: "User.unknow"}}},
			assert: func(err error) {
				qerr := &common.QueryBuildError{Op: "OverallAggregate.resolveField", Err: fmt.Errorf(dsl.ErrNodeNotHaveField, "User", "unknow")}
				require.EqualError(t, err, qerr.Error())
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := runExecutableErr(t, c.aggregates, &common.DefaultConf)
			c.assert(err)
		})
	}
}

func TestOverallAggregateExecution(t *testing.T) {
	tests := []struct {
		name             string
		aggs             search.OverallAggregates
		expectedResponse search.AggregatesResponse
		cfg              *search.Config
	}{
		{ // launch 2 queries containing 1 and several scalars
			name: "OverChunkSize",
			aggs: search.OverallAggregates{
				{BaseAggregate: dsl.BaseAggregate{Field: "User", Type: dsl.AggCount, Alias: "c1"}},
				{BaseAggregate: dsl.BaseAggregate{Field: "User", Type: dsl.AggCount, Alias: "c2"}},
				{BaseAggregate: dsl.BaseAggregate{Field: "User", Type: dsl.AggCount, Alias: "c3"}},
			},
			expectedResponse: search.AggregatesResponse{"c1": int64(5), "c2": int64(5), "c3": int64(5)},
			cfg:              common.NewConfig(common.WithScalarQueriesChunkSize(2)),
		},
		{ // launch only one query in a single group
			name: "OneScalar",
			aggs: search.OverallAggregates{
				{BaseAggregate: dsl.BaseAggregate{Field: "User.age", Type: dsl.AggSum, Alias: "c1"}},
			},
			expectedResponse: search.AggregatesResponse{"c1": float64(200)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.cfg == nil {
				tt.cfg = &common.DefaultConf
			}
			require.Equal(t, tt.expectedResponse, runExecutable(t, tt.aggs, tt.cfg))
		})
	}
}
