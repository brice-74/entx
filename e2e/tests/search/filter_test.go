package e2e_search_test

import (
	"context"
	"e2e/ent"
	"e2e/ent/entx"
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

func TestFilterQueryBuildError(t *testing.T) {
	tests := []struct {
		query       search.TargetedQuery
		name        string
		expectedErr *common.QueryBuildError
	}{
		{
			name: "ErrChainBroken",
			query: search.TargetedQuery{
				From: "User",
				QueryOptions: search.QueryOptions{
					Filters: dsl.Filters{
						{
							Field: "articles.title.tags",
						},
					},
				},
			},
			expectedErr: &common.QueryBuildError{
				Op:  "Filter.buildCondition",
				Err: fmt.Errorf(dsl.ErrChainBroken, "title", "Article"),
			},
		},
		{
			name: "ErrUnknownLink",
			query: search.TargetedQuery{
				From: "User",
				QueryOptions: search.QueryOptions{
					Filters: dsl.Filters{
						{
							Field: "unknow.field",
						},
					},
				},
			},
			expectedErr: &common.QueryBuildError{
				Op:  "Filter.buildCondition",
				Err: fmt.Errorf(dsl.ErrUnknownLink, "unknow", "User"),
			},
		},
		{
			name: "ErrInvalidOperator",
			query: search.TargetedQuery{
				From: "User",
				QueryOptions: search.QueryOptions{
					Filters: dsl.Filters{
						{
							Field:    "name",
							Operator: "",
						},
					},
				},
			},
			expectedErr: &common.QueryBuildError{
				Op:  "buildBasePredicate",
				Err: fmt.Errorf(dsl.ErrInvalidOperator, ""),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.query.Execute(
				context.Background(),
				client,
				entx.Graph,
				&common.DefaultConf,
			)

			var qerr *search.QueryBuildError

			require.Error(t, err)
			require.ErrorAs(t, err, &qerr)
			require.EqualError(t, err, tt.expectedErr.Error())
		})
	}
}

func TestFilterValidationErrors(t *testing.T) {
	tests := []struct {
		query        search.TargetedQuery
		cfg          *search.Config
		expectedRule string
	}{
		{
			query: search.TargetedQuery{
				QueryOptions: search.QueryOptions{
					Filters: dsl.Filters{
						{}, {},
					},
				},
			},
			cfg: common.NewConfig(common.WithFilterConfig(common.FilterConfig{
				MaxFilterTreeCount: 1,
			})),
			expectedRule: "MaxFilterTreeCount",
		},
		{
			query: search.TargetedQuery{
				From: "User",
				QueryOptions: search.QueryOptions{
					Filters: dsl.Filters{
						{Relation: "articles"},
						{Relation: "comments"},
					},
				},
			},
			cfg: common.NewConfig(common.WithFilterConfig(common.FilterConfig{
				MaxRelationTotalCount: 1,
			})),
			expectedRule: "MaxFilterRelationsPerTree",
		},
		{
			query: search.TargetedQuery{
				From: "User",
				QueryOptions: search.QueryOptions{
					Filters: dsl.Filters{
						{Relation: "articles.tags"},
					},
				},
			},
			cfg: common.NewConfig(common.WithFilterConfig(common.FilterConfig{
				MaxRelationChainDepth: 1,
			})),
			expectedRule: "MaxRelationChainDepth",
		},
		{
			query: search.TargetedQuery{
				From: "User",
				QueryOptions: search.QueryOptions{
					Filters: dsl.Filters{
						{Relation: "articles..tags"},
					},
				},
			},
			expectedRule: "InvalidFilterRelationFormat",
		},
		{
			query: search.TargetedQuery{
				From: "User",
				QueryOptions: search.QueryOptions{
					Filters: dsl.Filters{
						{Field: "articles..name"},
					},
				},
			},
			expectedRule: "InvalidFilterFieldFormat",
		},
		{
			query: search.TargetedQuery{
				From: "User",
				QueryOptions: search.QueryOptions{
					Filters: dsl.Filters{
						{
							Field:    "name",
							Operator: "=",
							Value:    []string{""},
						},
					},
				},
			},
			expectedRule: "OperatorPrimitiveValue",
		},
		{
			query: search.TargetedQuery{
				From: "User",
				QueryOptions: search.QueryOptions{
					Filters: dsl.Filters{
						{
							Field:    "name",
							Operator: "IN",
							Value:    []string{""},
						},
					},
				},
			},
			expectedRule: "OperatorPrimitiveSliceValue",
		},
		{
			query: search.TargetedQuery{
				From: "User",
				QueryOptions: search.QueryOptions{
					Filters: dsl.Filters{
						{
							Field:    "name",
							Operator: "#",
						},
					},
				},
			},
			expectedRule: "InvalidOperator",
		},
	}

	for _, tt := range tests {
		t.Run(tt.expectedRule, func(t *testing.T) {
			if tt.cfg == nil {
				tt.cfg = &common.DefaultConf
			}
			_, err := tt.query.Execute(
				context.Background(),
				client,
				entx.Graph,
				tt.cfg,
			)

			var verr *search.ValidationError

			require.Error(t, err)
			require.ErrorAs(t, err, &verr)
			require.Equal(t, tt.expectedRule, verr.Rule)
		})
	}
}

func TestFilterOperators(t *testing.T) {
	tests := []struct {
		field          string
		operator       dsl.Operator
		value          any
		expectedCount  int
		requireContent func(*testing.T, []*ent.User)
	}{
		{
			field:         "age",
			operator:      dsl.OpEqual,
			value:         20,
			expectedCount: 1,
			requireContent: func(t *testing.T, users []*ent.User) {
				require.Equal(t, 20, users[0].Age)
			},
		},
		{
			field:         "age",
			operator:      dsl.OpNotEqual,
			value:         20,
			expectedCount: 2,
			requireContent: func(t *testing.T, users []*ent.User) {
				for _, u := range users {
					require.NotEqual(t, 20, u.Age)
				}
			},
		},
		{
			field:         "age",
			operator:      dsl.OpGreaterThan,
			value:         20,
			expectedCount: 2,
			requireContent: func(t *testing.T, users []*ent.User) {
				for _, u := range users {
					require.Greater(t, u.Age, 20)
				}
			},
		},
		{
			field:         "age",
			operator:      dsl.OpGreaterEqual,
			value:         30,
			expectedCount: 2,
			requireContent: func(t *testing.T, users []*ent.User) {
				for _, u := range users {
					require.GreaterOrEqual(t, u.Age, 30)
				}
			},
		},
		{
			field:         "age",
			operator:      dsl.OpLessThan,
			value:         40,
			expectedCount: 2,
			requireContent: func(t *testing.T, users []*ent.User) {
				for _, u := range users {
					require.Less(t, u.Age, 40)
				}
			},
		},
		{
			field:         "age",
			operator:      dsl.OpLessEqual,
			value:         30,
			expectedCount: 2,
			requireContent: func(t *testing.T, users []*ent.User) {
				for _, u := range users {
					require.LessOrEqual(t, u.Age, 30)
				}
			},
		},
		{
			field:         "email",
			operator:      dsl.OpLike,
			value:         "user1",
			expectedCount: 1,
			requireContent: func(t *testing.T, users []*ent.User) {
				require.Equal(t, "user1@example.com", users[0].Email)
			},
		},
		{
			field:         "email",
			operator:      dsl.OpNotLike,
			value:         "user1",
			expectedCount: 2,
			requireContent: func(t *testing.T, users []*ent.User) {
				for _, u := range users {
					require.NotContains(t, u.Email, "user1")
				}
			},
		},
		{
			field:         "age",
			operator:      dsl.OpIn,
			value:         []any{20, 30},
			expectedCount: 2,
			requireContent: func(t *testing.T, users []*ent.User) {
				found := make([]any, len(users))
				for i, u := range users {
					found[i] = u.Age
				}
				require.Subset(t, found, []any{20, 30})
			},
		},
		{
			field:         "age",
			operator:      dsl.OpNotIn,
			value:         []any{20, 30},
			expectedCount: 1,
			requireContent: func(t *testing.T, users []*ent.User) {
				found := make([]any, len(users))
				for i, u := range users {
					found[i] = u.Age
				}
				require.NotSubset(t, found, []any{20, 30})
			},
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Success(%s)", tt.operator), func(t *testing.T) {
			q := search.TargetedQuery{
				From: "User",
				QueryOptions: search.QueryOptions{
					Filters: dsl.Filters{
						{
							Field:    tt.field,
							Operator: tt.operator,
							Value:    tt.value,
						},
					},
				},
			}

			res, err := q.Execute(
				context.Background(),
				client,
				entx.Graph,
				&common.DefaultConf,
			)

			require.NoError(t, err)
			require.Equal(t, tt.expectedCount, res.Meta.Count)
			tt.requireContent(t, entxstd.AsTypedEntities[*ent.User](res.Data.([]entxstd.Entity)))
		})
	}
}

func TestFilterCondition(t *testing.T) {
	t.Run("SuccessOr", func(t *testing.T) {
		q := search.TargetedQuery{
			From: "User",
			QueryOptions: search.QueryOptions{
				Filters: dsl.Filters{
					{
						Or: dsl.Filters{
							{
								Field:    "age",
								Operator: "=",
								Value:    30,
							},
							{
								Field:    "is_active",
								Operator: "=",
								Value:    false,
							},
						},
					},
				},
			},
		}

		res, err := q.Execute(
			context.Background(),
			client,
			entx.Graph,
			&common.DefaultConf,
		)

		require.NoError(t, err)
		require.Equal(t, 2, res.Meta.Count)
		for i, u := range entxstd.AsTypedEntities[*ent.User](res.Data.([]entxstd.Entity)) {
			if u.IsActive != false && u.Age != 30 {
				t.Fatalf("unexpected user at index %d: IsActive=%v, Age=%d", i, u.IsActive, u.Age)
			}
		}
	})

	t.Run("SuccessAnd", func(t *testing.T) {
		q := search.TargetedQuery{
			From: "User",
			QueryOptions: search.QueryOptions{
				Filters: dsl.Filters{
					{
						And: dsl.Filters{
							{
								Field:    "age",
								Operator: "=",
								Value:    40,
							},
							{
								Field:    "is_active",
								Operator: "=",
								Value:    false,
							},
						},
					},
				},
			},
		}

		res, err := q.Execute(
			context.Background(),
			client,
			entx.Graph,
			&common.DefaultConf,
		)

		require.NoError(t, err)
		require.Equal(t, 1, res.Meta.Count)
		u := entxstd.AsTypedEntities[*ent.User](res.Data.([]entxstd.Entity))[0]
		if u.IsActive != false || u.Age != 40 {
			t.Fatalf("unexpected user: IsActive=%v, Age=%d", u.IsActive, u.Age)
		}
	})

	t.Run("SuccessNot", func(t *testing.T) {
		q := search.TargetedQuery{
			From: "User",
			QueryOptions: search.QueryOptions{
				Filters: dsl.Filters{
					{
						Not: &dsl.Filter{
							Field:    "is_active",
							Operator: "=",
							Value:    true,
						},
					},
				},
			},
		}

		res, err := q.Execute(
			context.Background(),
			client,
			entx.Graph,
			&common.DefaultConf,
		)

		require.NoError(t, err)
		require.Equal(t, 1, res.Meta.Count)
		u := entxstd.AsTypedEntities[*ent.User](res.Data.([]entxstd.Entity))[0]
		if u.IsActive != false {
			t.Fatalf("unexpected user: IsActive=%v", u.IsActive)
		}
	})

	t.Run("SuccessNested", func(t *testing.T) {
		q := search.TargetedQuery{
			From: "User",
			QueryOptions: search.QueryOptions{
				Filters: dsl.Filters{
					{
						Or: dsl.Filters{
							{
								And: dsl.Filters{
									{
										Field:    "age",
										Operator: "=",
										Value:    40,
									},
									{
										Field:    "is_active",
										Operator: "=",
										Value:    false,
									},
								},
							},
							{
								Field:    "age",
								Operator: "=",
								Value:    20,
							},
						},
					},
				},
			},
		}

		res, err := q.Execute(
			context.Background(),
			client,
			entx.Graph,
			&common.DefaultConf,
		)

		require.NoError(t, err)
		require.Equal(t, 2, res.Meta.Count)
		for i, u := range entxstd.AsTypedEntities[*ent.User](res.Data.([]entxstd.Entity)) {
			if !((u.Age == 40 && u.IsActive == false) || u.Age == 20) {
				t.Fatalf("unexpected user at index %d: IsActive=%v, Age=%d", i, u.IsActive, u.Age)
			}
		}
	})
}

func TestFilterRelationTypes(t *testing.T) {
	tests := []struct {
		name        string
		query       search.TargetedQuery
		expectedIDs []int
		resultIDs   func([]entxstd.Entity) []int
	}{
		{
			name: "O2O",
			query: search.TargetedQuery{
				From: "Employee",
				QueryOptions: search.QueryOptions{
					Filters: dsl.Filters{
						{
							Field:    "user.name",
							Operator: "=",
							Value:    "User One",
						},
					},
				},
			},
			expectedIDs: []int{1},
			resultIDs: func(e []entxstd.Entity) (ids []int) {
				for _, u := range entxstd.AsTypedEntities[*ent.Employee](e) {
					ids = append(ids, u.ID)
				}
				return
			},
		},
		{
			name: "O2M",
			query: search.TargetedQuery{
				From: "User",
				QueryOptions: search.QueryOptions{
					Filters: dsl.Filters{
						{
							Field:    "articles.title",
							Operator: "=",
							Value:    "Go Concurrency Patterns",
						},
					},
				},
			},
			expectedIDs: []int{1},
			resultIDs: func(e []entxstd.Entity) (ids []int) {
				for _, u := range entxstd.AsTypedEntities[*ent.User](e) {
					ids = append(ids, u.ID)
				}
				return
			},
		},
		{
			name: "M2O",
			query: search.TargetedQuery{
				From: "Comment",
				QueryOptions: search.QueryOptions{
					Filters: dsl.Filters{
						{
							Field:    "user.name",
							Operator: "=",
							Value:    "User Two",
						},
					},
				},
			},
			expectedIDs: []int{1},
			resultIDs: func(e []entxstd.Entity) (ids []int) {
				for _, u := range entxstd.AsTypedEntities[*ent.Comment](e) {
					ids = append(ids, u.ID)
				}
				return
			},
		},
		{
			name: "M2M",
			query: search.TargetedQuery{
				From: "Article",
				QueryOptions: search.QueryOptions{
					Filters: dsl.Filters{
						{
							Field:    "tags.name",
							Operator: "=",
							Value:    "Go",
						},
						{
							Field:    "tags.name",
							Operator: "=",
							Value:    "DevOps",
						},
					},
				},
			},
			expectedIDs: []int{3},
			resultIDs: func(e []entxstd.Entity) (ids []int) {
				for _, u := range entxstd.AsTypedEntities[*ent.Article](e) {
					ids = append(ids, u.ID)
				}
				return
			},
		},
		{
			name: "Self",
			query: search.TargetedQuery{
				From: "Employee",
				QueryOptions: search.QueryOptions{
					Filters: dsl.Filters{
						{
							Field:    "manager.user.name",
							Operator: "=",
							Value:    "User One",
						},
					},
				},
			},
			expectedIDs: []int{2, 3},
			resultIDs: func(e []entxstd.Entity) (ids []int) {
				for _, u := range entxstd.AsTypedEntities[*ent.Employee](e) {
					ids = append(ids, u.ID)
				}
				return
			},
		},
		{
			name: "UseRelationField",
			query: search.TargetedQuery{
				From: "User",
				QueryOptions: search.QueryOptions{
					Filters: dsl.Filters{
						{
							Relation: "articles",
							And: dsl.Filters{
								{
									Field:    "tags.name",
									Operator: "=",
									Value:    "Go",
								},
								{
									Field:    "tags.name",
									Operator: "=",
									Value:    "DevOps",
								},
							},
						},
					},
				},
			},
			expectedIDs: []int{3},
			resultIDs: func(e []entxstd.Entity) (ids []int) {
				for _, u := range entxstd.AsTypedEntities[*ent.User](e) {
					ids = append(ids, u.ID)
				}
				return
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := tt.query.Execute(
				context.Background(),
				client,
				entx.Graph,
				&common.DefaultConf,
			)

			require.NoError(t, err)
			require.ElementsMatch(t, tt.expectedIDs, tt.resultIDs(res.Data.([]entxstd.Entity)), "IDs mismatch")
		})
	}
}
