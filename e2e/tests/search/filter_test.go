package e2e_search_test

import (
	"e2e/ent"
	"fmt"
	"testing"

	entxstd "github.com/brice-74/entx"

	"github.com/brice-74/entx/search"
	"github.com/brice-74/entx/search/common"
	"github.com/brice-74/entx/search/dsl"
	"github.com/stretchr/testify/require"
)

func TestFilterPanicPreprocessed(t *testing.T) {
	require.Panics(t, func() { (&dsl.Filter{}).Predicate(nil) })
}

func TestFilterBuildErr(t *testing.T) {
	cases := []struct {
		name   string
		query  search.TargetedQuery
		assert func(error)
	}{
		{
			name:  "ErrChainBroken",
			query: search.TargetedQuery{From: "User", QueryOptions: search.QueryOptions{Filters: dsl.Filters{{Field: "articles.title.tags"}}}},
			assert: func(err error) {
				qerr := &common.QueryBuildError{Op: "Filter.buildCondition", Err: fmt.Errorf(dsl.ErrChainBroken, "title", "Article")}
				require.EqualError(t, err, qerr.Error())
			},
		},
		{
			name:  "ErrUnknownLink",
			query: search.TargetedQuery{From: "User", QueryOptions: search.QueryOptions{Filters: dsl.Filters{{Field: "unknow.field"}}}},
			assert: func(err error) {
				qerr := &common.QueryBuildError{Op: "Filter.buildCondition", Err: fmt.Errorf(dsl.ErrUnknownLink, "unknow", "User")}
				require.EqualError(t, err, qerr.Error())
			},
		},
		{
			name:  "ErrInvalidOperator",
			query: search.TargetedQuery{From: "User", QueryOptions: search.QueryOptions{Filters: dsl.Filters{{Field: "name", Operator: ""}}}},
			assert: func(err error) {
				qerr := &common.QueryBuildError{Op: "buildBasePredicate", Err: fmt.Errorf(dsl.ErrInvalidOperator, "")}
				require.EqualError(t, err, qerr.Error())
			},
		},
		{
			name:  "ErrUnknownLinkOnRelationField",
			query: search.TargetedQuery{From: "User", QueryOptions: search.QueryOptions{Filters: dsl.Filters{{Relation: "unknow"}}}},
			assert: func(err error) {
				qerr := &common.QueryBuildError{Op: "Filter.Predicate", Err: fmt.Errorf(dsl.ErrUnknownLink, "unknow", "User")}
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

func TestFilterValidation(t *testing.T) {
	cases := []struct {
		expectedRule string
		filters      dsl.Filters
		cfg          *search.Config
	}{
		{"MaxFilterTreeCount", dsl.Filters{{}, {}}, common.NewConfig(common.WithFilterConfig(common.FilterConfig{MaxFilterTreeCount: 1}))},
		{"MaxFilterRelationsPerTree", dsl.Filters{{Relation: "articles"}, {Relation: "comments"}}, common.NewConfig(common.WithFilterConfig(common.FilterConfig{MaxRelationTotalCount: 1}))},
		{"MaxRelationChainDepth", dsl.Filters{{Relation: "articles.tags"}}, common.NewConfig(common.WithFilterConfig(common.FilterConfig{MaxRelationChainDepth: 1}))},
		{"InvalidFilterRelationFormat", dsl.Filters{{Relation: "articles..tags"}}, &common.DefaultConf},
		{"InvalidFilterFieldFormat", dsl.Filters{{Field: "articles..name"}}, &common.DefaultConf},
		{"OperatorPrimitiveValue", dsl.Filters{{Field: "name", Operator: "=", Value: []string{""}}}, &common.DefaultConf},
		{"OperatorPrimitiveSliceValue", dsl.Filters{{Field: "name", Operator: "IN", Value: []string{""}}}, &common.DefaultConf},
		{"InvalidOperator", dsl.Filters{{Field: "name", Operator: "#"}}, &common.DefaultConf},
	}
	for _, c := range cases {
		t.Run(c.expectedRule, func(t *testing.T) {
			q := search.TargetedQuery{From: "User", QueryOptions: search.QueryOptions{Filters: c.filters}}
			err := runExecutableErr(t, &q, c.cfg)
			var verr *search.ValidationError
			require.ErrorAs(t, err, &verr)
			require.Equal(t, c.expectedRule, verr.Rule)
		})
	}
}

func TestFilterOperators(t *testing.T) {
	tests := []struct {
		name     string
		field    string
		op       dsl.Operator
		value    any
		wantVals []int
	}{
		{"Equal", "age", dsl.OpEqual, 20, []int{20}},
		{"NotEqual", "age", dsl.OpNotEqual, 20, []int{30, 40, 50, 60}},
		{"GreaterThan", "age", dsl.OpGreaterThan, 40, []int{50, 60}},
		{"GreaterOrEqual", "age", dsl.OpGreaterEqual, 40, []int{40, 50, 60}},
		{"LessThan", "age", dsl.OpLessThan, 40, []int{20, 30}},
		{"LessOrEqual", "age", dsl.OpLessEqual, 40, []int{20, 30, 40}},
		{"Like", "email", dsl.OpLike, "user1", nil},
		{"NotLike", "email", dsl.OpNotLike, "user1", nil},
		{"In", "age", dsl.OpIn, []any{20, 30}, []int{20, 30}},
		{"NotIn", "age", dsl.OpNotIn, []any{20, 30}, []int{40, 50, 60}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := search.TargetedQuery{From: "User", QueryOptions: search.QueryOptions{Filters: dsl.Filters{{Field: tt.field, Operator: tt.op, Value: tt.value}}}}
			users := runTargetedQuery[*ent.User](t, &q, &common.DefaultConf)
			if tt.wantVals != nil {
				ages := make([]int, len(users))
				for i, u := range users {
					ages[i] = u.Age
				}
				require.ElementsMatch(t, tt.wantVals, ages)
			} else {
				for _, u := range users {
					switch tt.op {
					case dsl.OpLike:
						require.Contains(t, u.Email, tt.value.(string))
					case dsl.OpNotLike:
						require.NotContains(t, u.Email, tt.value.(string))
					}
				}
			}
		})
	}
}

func TestFilterCondition(t *testing.T) {
	cases := []struct {
		name        string
		query       search.TargetedQuery
		wantCount   int
		assertUsers func(t *testing.T, users []entxstd.Entity)
	}{
		{
			name: "Or",
			query: search.TargetedQuery{
				From: "User",
				QueryOptions: search.QueryOptions{Filters: dsl.Filters{{
					Or: dsl.Filters{
						{Field: "age", Operator: "=", Value: 30},
						{Field: "is_active", Operator: "=", Value: false},
					},
				}}},
			},
			wantCount: 3,
			assertUsers: func(t *testing.T, users []entxstd.Entity) {
				for _, e := range entxstd.AsTypedEntities[*ent.User](users) {
					u := e
					require.True(t, u.Age == 30 || u.IsActive == false)
				}
			},
		},
		{
			name: "And",
			query: search.TargetedQuery{
				From: "User",
				QueryOptions: search.QueryOptions{Filters: dsl.Filters{{
					And: dsl.Filters{
						{Field: "age", Operator: "=", Value: 40},
						{Field: "is_active", Operator: "=", Value: false},
					},
				}}},
			},
			wantCount: 1,
			assertUsers: func(t *testing.T, users []entxstd.Entity) {
				u := entxstd.AsTypedEntities[*ent.User](users)[0]
				require.Equal(t, 40, u.Age)
				require.False(t, u.IsActive)
			},
		},
		{
			name: "Not",
			query: search.TargetedQuery{
				From: "User",
				QueryOptions: search.QueryOptions{Filters: dsl.Filters{{
					Not: &dsl.Filter{Field: "is_active", Operator: "=", Value: true},
				}}},
			},
			wantCount: 2,
			assertUsers: func(t *testing.T, users []entxstd.Entity) {
				u := entxstd.AsTypedEntities[*ent.User](users)[0]
				require.False(t, u.IsActive)
			},
		},
		{
			name: "Nested",
			query: search.TargetedQuery{
				From: "User",
				QueryOptions: search.QueryOptions{Filters: dsl.Filters{{
					Or: dsl.Filters{
						{And: dsl.Filters{
							{Field: "age", Operator: "=", Value: 40},
							{Field: "is_active", Operator: "=", Value: false},
						}},
						{Field: "age", Operator: "=", Value: 20},
					},
				}}},
			},
			wantCount: 2,
			assertUsers: func(t *testing.T, users []entxstd.Entity) {
				for _, e := range entxstd.AsTypedEntities[*ent.User](users) {
					u := e
					require.True(t, (u.Age == 40 && !u.IsActive) || u.Age == 20)
				}
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			users := runTargetedQuery[entxstd.Entity](t, &c.query, &common.DefaultConf)
			require.Len(t, users, c.wantCount)
			c.assertUsers(t, users)
		})
	}
}

func TestFilterRelationTypes(t *testing.T) {
	cases := []struct {
		name    string
		query   search.TargetedQuery
		wantIDs []int
	}{
		{
			"O2O",
			search.TargetedQuery{From: "Employee", QueryOptions: search.QueryOptions{Filters: dsl.Filters{{Field: "user.name", Operator: "=", Value: "User One"}}}},
			[]int{1},
		},
		{
			"O2M",
			search.TargetedQuery{From: "User", QueryOptions: search.QueryOptions{Filters: dsl.Filters{{Field: "articles.title", Operator: "=", Value: "Go Concurrency Patterns"}}}},
			[]int{1},
		},
		{
			"M2O",
			search.TargetedQuery{From: "Comment", QueryOptions: search.QueryOptions{Filters: dsl.Filters{{Field: "user.name", Operator: "=", Value: "User Two"}}}},
			[]int{1},
		},
		{
			"M2M",
			search.TargetedQuery{From: "Article", QueryOptions: search.QueryOptions{Filters: dsl.Filters{{Field: "tags.name", Operator: "=", Value: "Go"}, {Field: "tags.name", Operator: "=", Value: "DevOps"}}}},
			[]int{3},
		},
		{
			"Self",
			search.TargetedQuery{From: "Employee", QueryOptions: search.QueryOptions{Filters: dsl.Filters{{Field: "manager.user.name", Operator: "=", Value: "User One"}}}},
			[]int{2, 3, 4, 5},
		},
		{
			"RelationField",
			search.TargetedQuery{From: "User", QueryOptions: search.QueryOptions{Filters: dsl.Filters{{Relation: "articles", And: dsl.Filters{{Field: "tags.name", Operator: "=", Value: "Go"}, {Field: "tags.name", Operator: "=", Value: "DevOps"}}}}}},
			[]int{3},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res := runTargetedQuery[entxstd.Entity](t, &c.query, &common.DefaultConf)
			var got []int
			for _, e := range res {
				switch v := e.(type) {
				case *ent.User:
					got = append(got, v.ID)
				case *ent.Employee:
					got = append(got, v.ID)
				case *ent.Comment:
					got = append(got, v.ID)
				case *ent.Article:
					got = append(got, v.ID)
				}
			}
			require.ElementsMatch(t, c.wantIDs, got)
		})
	}
}
