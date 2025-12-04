package dsl

import (
	"fmt"

	"github.com/brice-74/entx"
	"github.com/brice-74/entx/search/common"
)

type Select []string

func (s Select) PredicateQ(node entx.Node) (func(q entx.Query), error) {
	if len(s) > 0 {
		for i, v := range s {
			f := node.FieldByName(v)
			if f == nil {
				return nil, &common.QueryBuildError{
					Op:  "Select.PredicateQ",
					Err: fmt.Errorf("node %q has no field named %q", node.Name(), v),
				}
			}
			s[i] = f.StorageName
		}

		return func(q entx.Query) {
			q.Select(s...)
		}, nil
	}

	return func(q entx.Query) {}, nil
}
