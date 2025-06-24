package e2e_test

import (
	"context"
	"e2e/ent/entx"
	"testing"

	"github.com/brice-74/entx/search"
	"github.com/brice-74/entx/search/common"
)

func TestXxx(t *testing.T) {
	opts := search.TargetedQuery{
		From: "User",
	}

	ctx := context.WithValue(context.Background(), "aaa", "ma valueee !!!!!!")

	v, err := opts.Execute(ctx, entx.NewClient(client), entx.Graph, &common.DefaultConf)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(v)
}
