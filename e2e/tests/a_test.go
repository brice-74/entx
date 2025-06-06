package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/brice-74/entx/search"
	"github.com/stretchr/testify/assert"
)

func TestXxx(t *testing.T) {
	req := search.HubRequest{
		Searches: []search.WithKey{
			{
				Key: "",
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	if res, err := hub.Exec(ctx, &req); assert.NoError(t, err) {

	}
}
