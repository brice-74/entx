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

func TestFilter(t *testing.T) {
	t.Run("PanicPreprocessed", func(t *testing.T) {
		require.Panics(t, func() { (&dsl.Filter{}).Predicate(nil) })
	})
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

func TestFilterRelationtypes(t *testing.T) {
	tests := []struct {
		name  string
		query search.TargetedQuery
	}{
		// ---------------------------
		// 🟦 One-to-One (O2O)
		// ---------------------------
		{
			name: "EmployeeO2OWithUserNameFilter",
			query: q("Employee", filter(
				relation("user", field("name", "=", "User One")),
			)),
		},

		// ---------------------------
		// 🟦 One-to-Many (O2M)
		// ---------------------------
		{
			name: "UserO2MWithArticleTitleFilter",
			query: q("User", filter(
				relation("articles", field("title", "=", "Go Concurrency Patterns")),
			)),
		},
		{
			name: "ArticleO2MWithCommentBodyContains",
			query: q("Article", filter(
				relation("comments", field("body", "LIKE", "%goroutines%")),
			)),
		},

		// ---------------------------
		// 🟦 Many-to-One (M2O)
		// ---------------------------
		{
			name: "CommentM2OWithUserNameFilter",
			query: q("Comment", filter(
				relation("user", field("name", "=", "User Two")),
			)),
		},
		{
			name: "EmployeeM2OWithDepartmentNameFilter",
			query: q("Employee", filter(
				relation("department", field("name", "=", "DSI")),
			)),
		},

		// ---------------------------
		// 🟦 Many-to-Many (M2M)
		// ---------------------------
		{
			name: "ArticleM2MWithTagGo",
			query: q("Article", filter(
				relation("tags", field("name", "=", "Go")),
			)),
		},
		{
			name: "ArticleM2MWithTagsGoAndDevOps",
			query: q("Article", filter(
				relation("tags", and(
					field("name", "=", "Go"),
					field("name", "=", "DevOps"),
				)),
			)),
		},

		// ---------------------------
		// 🟦 Self-relation (Employee.manager / Employee.reports)
		// ---------------------------
		{
			name: "EmployeeSelfWithManagerUserName",
			query: q("Employee", filter(
				relation("manager.user", field("name", "=", "User Three")),
			)),
		},
		{
			name: "EmployeeSelfWithReportNamedUserOne",
			query: q("Employee", filter(
				relation("reports.user", field("name", "=", "User One")),
			)),
		},
	}

}

func q(from string, f dsl.Filters) search.TargetedQuery {
	return search.TargetedQuery{
		From: from,
		QueryOptions: search.QueryOptions{
			Filters: f,
		},
	}
}

func filter(fs ...*dsl.Filter) dsl.Filters {
	return dsl.Filters(fs)
}

func relation(path string, f *dsl.Filter) *dsl.Filter {
	return &dsl.Filter{
		Relation: path,
		And:      dsl.Filters{f},
	}
}

func field(name string, op dsl.Operator, val any) *dsl.Filter {
	return &dsl.Filter{
		Field:    name,
		Operator: op,
		Value:    val,
	}
}

func and(fs ...*dsl.Filter) dsl.Filters {
	return dsl.Filters(fs)
}
