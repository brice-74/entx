package common

import "entgo.io/ent/dialect/sql"

type PaginateResponse struct {
	From        int `json:"from"`
	To          int `json:"to"`
	Total       int `json:"total"`
	CurrentPage int `json:"current_page"`
	LastPage    int `json:"last_page"`
	PerPage     int `json:"per_page"`
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
