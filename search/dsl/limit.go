package dsl

import (
	"entgo.io/ent/dialect/sql"
	"github.com/brice-74/entx/search/common"
)

type Limit struct {
	Limit int `json:"limit,omitempty"`
}

func (l *Limit) Predicate() func(s *sql.Selector) {
	return func(s *sql.Selector) {
		s.Limit(l.Limit)
	}
}

func (l *Limit) Sanitize(c *common.PageableConfig) {
	if l.Limit <= 0 {
		l.Limit = c.DefaultLimit
	}
	if l.Limit > c.MaxLimit {
		l.Limit = c.MaxLimit
	}
}
