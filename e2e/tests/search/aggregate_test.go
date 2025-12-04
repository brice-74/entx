package e2e_search_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	entxstd "github.com/brice-74/entx"
	"github.com/brice-74/entx/search"
	"github.com/brice-74/entx/search/common"
	"github.com/brice-74/entx/search/dsl"
	"github.com/stretchr/testify/require"
)

func TestAggregatePanicPreprocessed(t *testing.T) {
	require.Panics(t, func() { (&dsl.Aggregate{}).Predicate(context.Background(), nil, "") })
}

func TestAggregateValidationErrors(t *testing.T) {
	tests := []struct {
		query        search.TargetedQuery
		expectedRule string
		cfg          *search.Config
	}{
		{
			query: search.TargetedQuery{
				From: "User",
				QueryOptions: search.QueryOptions{
					Aggregates: dsl.Aggregates{
						&dsl.Aggregate{BaseAggregate: dsl.BaseAggregate{Field: "..age", Type: dsl.AggMin}},
					},
				},
			},
			expectedRule: "AggregateFieldSyntax",
		},
		{
			query: search.TargetedQuery{
				From: "User",
				QueryOptions: search.QueryOptions{
					Aggregates: dsl.Aggregates{
						&dsl.Aggregate{BaseAggregate: dsl.BaseAggregate{Field: "age", Type: "median"}},
					},
				},
			},
			expectedRule: "AggregateTypeUnsupported",
		},
		{
			query: search.TargetedQuery{
				From: "User",
				QueryOptions: search.QueryOptions{
					Aggregates: dsl.Aggregates{
						&dsl.Aggregate{BaseAggregate: dsl.BaseAggregate{Field: "email", Type: dsl.AggMin, Distinct: true}},
					},
				},
			},
			expectedRule: "AggregateDistinctNotAllowed",
		},
		{
			query: search.TargetedQuery{
				From: "User",
				QueryOptions: search.QueryOptions{
					Aggregates: dsl.Aggregates{
						&dsl.Aggregate{BaseAggregate: dsl.BaseAggregate{Field: "artciles.comments.id", Type: dsl.AggCount}},
					},
				},
			},
			cfg:          common.NewConfig(common.WithAggregateConfig(common.AggregateConfig{MaxAggregateRelationDepth: 1})),
			expectedRule: "MaxAggregateRelationsDepth",
		},
	}

	for _, tt := range tests {
		t.Run(tt.expectedRule, func(t *testing.T) {
			if tt.cfg == nil {
				tt.cfg = &common.DefaultConf
			}
			err := runExecutableErr(t, &tt.query, tt.cfg)
			var verr *search.ValidationError
			require.ErrorAs(t, err, &verr)
			require.Equal(t, tt.expectedRule, verr.Rule)
		})
	}
}

func TestAggregateBuildErr(t *testing.T) {
	cases := []struct {
		name   string
		query  search.TargetedQuery
		assert func(error)
	}{
		{
			name:  "ErrDistinctWithoutField",
			query: search.TargetedQuery{From: "User", QueryOptions: search.QueryOptions{Aggregates: dsl.Aggregates{{BaseAggregate: dsl.BaseAggregate{Type: dsl.AggCount, Distinct: true}}}}},
			assert: func(err error) {
				qerr := &common.QueryBuildError{Op: "BaseAggregate.buildExpr", Err: errors.New(dsl.ErrDistinctWithoutField)}
				require.EqualError(t, err, qerr.Error())
			},
		},
		{
			name:  "ErrAggWithoutField",
			query: search.TargetedQuery{From: "User", QueryOptions: search.QueryOptions{Aggregates: dsl.Aggregates{{BaseAggregate: dsl.BaseAggregate{Type: dsl.AggMax}}}}},
			assert: func(err error) {
				qerr := &common.QueryBuildError{Op: "BaseAggregate.buildExpr", Err: fmt.Errorf(dsl.ErrAggWithoutField, dsl.AggMax)}
				require.EqualError(t, err, qerr.Error())
			},
		},
		{
			name:  "ErrUnknownLink",
			query: search.TargetedQuery{From: "User", QueryOptions: search.QueryOptions{Aggregates: dsl.Aggregates{{BaseAggregate: dsl.BaseAggregate{Field: "unknow", Type: dsl.AggCount}}}}},
			assert: func(err error) {
				qerr := &common.QueryBuildError{Op: "Aggregate.Predicate", Err: fmt.Errorf(dsl.ErrUnknownLink, "unknow", "User")}
				require.EqualError(t, err, qerr.Error())
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := runExecutableErr(t, &c.query, &common.DefaultConf)
			c.assert(err)
		})
	}
}

func TestAggregateExecution(t *testing.T) {
	tests := []struct {
		name          string
		aggs          dsl.Aggregates
		expectedField string
		expectedValue any
	}{
		{
			name: "SumAge",
			aggs: dsl.Aggregates{
				&dsl.Aggregate{BaseAggregate: dsl.BaseAggregate{Field: "employees.user.age", Type: dsl.AggSum, Alias: "sum_users_age"}},
			},
			expectedField: "sum_users_age",
			expectedValue: float64(150),
		},
		{
			name: "CountUser",
			aggs: dsl.Aggregates{
				&dsl.Aggregate{BaseAggregate: dsl.BaseAggregate{Field: "employees.user", Type: dsl.AggCount, Alias: "user_count"}},
			},
			expectedField: "user_count",
			expectedValue: int64(3),
		},
		{
			name: "MaxAge",
			aggs: dsl.Aggregates{
				&dsl.Aggregate{BaseAggregate: dsl.BaseAggregate{Field: "employees.user.age", Type: dsl.AggMax, Alias: "max_age"}},
			},
			expectedField: "max_age",
			expectedValue: int64(60),
		},
		{
			name: "MinAge",
			aggs: dsl.Aggregates{
				&dsl.Aggregate{BaseAggregate: dsl.BaseAggregate{Field: "employees.user.age", Type: dsl.AggMin, Alias: "min_age"}},
			},
			expectedField: "min_age",
			expectedValue: int64(40),
		},
		{
			name: "AvgAge",
			aggs: dsl.Aggregates{
				&dsl.Aggregate{BaseAggregate: dsl.BaseAggregate{Field: "employees.user.age", Type: dsl.AggAvg, Alias: "avg_age"}},
			},
			expectedField: "avg_age",
			expectedValue: float64(50),
		},
		// the aggregation is not interesting, we just check if the node's primary keys are used correctly.
		{
			name: "MinNoBridgeWithout",
			aggs: dsl.Aggregates{
				&dsl.Aggregate{BaseAggregate: dsl.BaseAggregate{Field: "name", Type: dsl.AggMin, Alias: "min_department_name"}},
			},
			expectedField: "min_department_name",
			expectedValue: "DSI",
		},
		// the aggregation is not interesting, just test distinct & alias generation
		{
			name: "CountDistinctWithoutAlias",
			aggs: dsl.Aggregates{
				&dsl.Aggregate{
					BaseAggregate: dsl.BaseAggregate{
						Field:    "employees.department_id",
						Type:     dsl.AggCount,
						Distinct: true,
					},
				},
			},
			expectedField: "count_distinct_employees_department_id",
			expectedValue: int64(1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &search.TargetedQuery{
				From: "Department",
				QueryOptions: search.QueryOptions{
					Aggregates: tt.aggs,
					Filters: dsl.Filters{
						{
							Field:    "name",
							Operator: "=",
							Value:    "DSI",
						},
					},
				},
			}

			res := runTargetedQuery[entxstd.Entity](t, q, &common.DefaultConf)
			val := res[0].Metadatas().Aggregates[tt.expectedField]
			require.Equal(t, tt.expectedValue, val)
		})
	}
}

func TestAggregateExecutionWithFilters(t *testing.T) {
	alias := "count_reports_over_40"
	expectedCount := int64(2)

	q := &search.TargetedQuery{
		From: "Employee",
		QueryOptions: search.QueryOptions{
			Aggregates: dsl.Aggregates{
				{BaseAggregate: dsl.BaseAggregate{
					Field: "reports.user",
					Type:  dsl.AggCount,
					Alias: alias,
					Filters: dsl.Filters{
						{
							Field:    "age",
							Operator: dsl.OpGreaterThan,
							Value:    40,
						},
					},
				}},
			},
			Filters: dsl.Filters{
				{
					Field:    "id",
					Operator: "=",
					Value:    1,
				},
			},
		},
	}

	res := runTargetedQuery[entxstd.Entity](t, q, &common.DefaultConf)
	val := res[0].Metadatas().Aggregates[alias]
	require.Equal(t, expectedCount, val)
}
