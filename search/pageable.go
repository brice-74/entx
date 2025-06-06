package search

import "entgo.io/ent/dialect/sql"

func (p *Pageable) Predicate(useOffset bool) func(s *sql.Selector) {
	return func(s *sql.Selector) {
		s.Limit(p.Limit)
		if useOffset && p.Page > 1 {
			s.Offset((p.Page - 1) * p.Limit)
		}
	}
}

func (p *Pageable) Sanitize(c *PageableConfig) {
	if p.Limit <= 0 {
		p.Limit = c.DefaultLimit
	}
	if p.Limit > c.MaxLimit {
		p.Limit = c.MaxLimit
	}
	if p.Page < 1 {
		p.Page = 1
	}
}

type CompositePaginateInfos struct {
	Key string
	PaginateInfos
}

func (p *CompositePaginateInfos) ToScalarQuery() *ScalarQuery {
	return p.PaginateInfos.ToScalarQuery(p.Key)
}

type PaginateInfos struct {
	CountSelector *sql.Selector
	Page          int
	Limit         int
}

func (p *PaginateInfos) ToScalarQuery(key string) *ScalarQuery {
	return &ScalarQuery{
		Selector: p.CountSelector,
		Key:      key,
		Dest:     new(sql.NullInt64),
	}
}

func (p *PaginateInfos) Calculate(total, length int) *PaginateResponse {
	per, page := p.Limit, p.Page
	if per <= 0 {
		per = 1
	}
	var lastPage int
	if total > 0 {
		lastPage = (total + per - 1) / per
	} else {
		lastPage = 0
	}
	if page < 1 {
		page = 1
	}
	if total == 0 || (lastPage > 0 && page > lastPage) {
		return &PaginateResponse{
			From:        0,
			To:          0,
			Total:       total,
			CurrentPage: page,
			LastPage:    lastPage,
			PerPage:     per,
		}
	}
	from := (page-1)*per + 1
	to := min(from+length-1, total)
	if length == 0 {
		from = 0
		to = 0
	}
	return &PaginateResponse{
		From:        from,
		To:          to,
		Total:       total,
		CurrentPage: page,
		LastPage:    lastPage,
		PerPage:     per,
	}
}

func CalcPaginate(
	total, per, page, length int,
) *PaginateResponse {
	if per <= 0 {
		per = 1
	}
	var lastPage int
	if total > 0 {
		lastPage = (total + per - 1) / per
	} else {
		lastPage = 0
	}
	if page < 1 {
		page = 1
	}
	if total == 0 || (lastPage > 0 && page > lastPage) {
		return &PaginateResponse{
			From:        0,
			To:          0,
			Total:       total,
			CurrentPage: page,
			LastPage:    lastPage,
			PerPage:     per,
		}
	}
	from := (page-1)*per + 1
	to := min(from+length-1, total)
	if length == 0 {
		from = 0
		to = 0
	}
	return &PaginateResponse{
		From:        from,
		To:          to,
		Total:       total,
		CurrentPage: page,
		LastPage:    lastPage,
		PerPage:     per,
	}
}
