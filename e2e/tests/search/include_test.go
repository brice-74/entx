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

func TestIncludePanicPreprocessed(t *testing.T) {
	require.Panics(t, func() { (&dsl.Include{}).PredicateQ(context.Background(), nil, "") })
}

func TestIncludeBuildErr(t *testing.T) {
	t.Run("ErrBridgeNotFound", func(t *testing.T) {
		q := search.TargetedQuery{From: "User", QueryOptions: search.QueryOptions{Includes: dsl.Includes{{Relation: "unknow"}}}}

		err := runExecutableErr(t, &q, &common.DefaultConf)
		expectedErr := &common.QueryBuildError{Op: "Include.PredicateQ", Err: fmt.Errorf(dsl.ErrBridgeNotFound, "unknow", "User")}
		require.EqualError(t, err, expectedErr.Error())
	})
}

func TestIncludeValidation(t *testing.T) {
	cases := []struct {
		expectedRule string
		includes     dsl.Includes
		cfg          *search.Config
	}{
		{"InvalidIncludeRelationFormat", dsl.Includes{{Relation: ".."}}, nil},
		{"MaxIncludeRelationsDepth", dsl.Includes{{Relation: "a.b"}}, common.NewConfig(common.WithIncludeConfig(common.IncludeConfig{MaxIncludeRelationDepth: 1}))},
		{"MaxIncludeRelationsDepth", dsl.Includes{{Relation: "a", Includes: dsl.Includes{{Relation: "b"}}}}, common.NewConfig(common.WithIncludeConfig(common.IncludeConfig{MaxIncludeRelationDepth: 1}))},
		{"MaxIncludeTreeCount", dsl.Includes{{Relation: "a.b"}, {Relation: "a"}}, common.NewConfig(common.WithIncludeConfig(common.IncludeConfig{MaxIncludeTreeCount: 2}))},
	}
	for _, c := range cases {
		t.Run(c.expectedRule, func(t *testing.T) {
			q := search.TargetedQuery{From: "User", QueryOptions: search.QueryOptions{Includes: c.includes}}
			if c.cfg == nil {
				c.cfg = &common.DefaultConf
			}
			err := runExecutableErr(t, &q, c.cfg)
			var verr *search.ValidationError
			require.ErrorAs(t, err, &verr)
			require.Equal(t, c.expectedRule, verr.Rule)
		})
	}
}
