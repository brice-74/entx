package dsl

import (
	"entgo.io/ent/dialect/sql"
	"github.com/brice-74/entx/search/common"
)

type Pageable struct {
	Page int `json:"page,omitempty"`
	Limit
}

func (p *Pageable) Predicate(useOffset bool) func(s *sql.Selector) {
	return func(s *sql.Selector) {
		s.Limit(p.Limit.Limit)
		if useOffset && p.Page > 1 {
			s.Offset((p.Page - 1) * p.Limit.Limit)
		}
	}
}

func (p *Pageable) Sanitize(c *common.PageableConfig) {
	p.Limit.Sanitize(c)
	if p.Page < 1 {
		p.Page = 1
	}
}
