package common

import (
	"fmt"

	"entgo.io/ent/dialect/sql"
)

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

func AttachPaginationAndClean(
	searches map[string]*SearchResponse,
	scalars map[string]any,
	pagMap map[string]*PaginateInfos,
) error {
	for key, p := range pagMap {
		raw, ok := scalars[key]
		if !ok {
			return &ExecError{
				Op:  "AttachPaginationAndClean",
				Err: fmt.Errorf("missing paginate count for '%s'", key),
			}
		}
		cnt, ok := raw.(int64)
		if !ok {
			return &ExecError{
				Op:  "AttachPaginationAndClean",
				Err: fmt.Errorf("paginate count wrong type for '%s': %T", key, raw),
			}
		}
		sr, exist := searches[key]
		if !exist {
			return &ExecError{
				Op:  "AttachPaginationAndClean",
				Err: fmt.Errorf("search response not found for paginate on key '%s'", key),
			}
		}

		sr.Meta.Paginate = p.Calculate(int(cnt), sr.Meta.Count)
		delete(scalars, key)
	}
	return nil
}

func AttachPaginationAndCleanSync(
	searches *MapSync[string, *SearchResponse],
	scalars *MapSync[string, any],
	pagMap map[string]*PaginateInfos,
) error {
	if len(pagMap) == 0 {
		return nil
	}

	keysToDelete, err := AttachPaginationSync(searches, scalars, pagMap)
	if err != nil {
		return err
	}

	scalars.DeleteBatch(keysToDelete...)
	return nil
}

func AttachPaginationSync(
	searches *MapSync[string, *SearchResponse],
	scalars *MapSync[string, any],
	pagMap map[string]*PaginateInfos,
) ([]string, error) {
	if len(pagMap) == 0 {
		return nil, nil
	}

	scalars.RLock()
	searches.RLock()
	defer scalars.RUnlock()
	defer searches.RUnlock()

	processedKeys := make([]string, len(pagMap))
	var index int
	for key, p := range pagMap {
		raw, ok := scalars.UnsafeGet(key)
		if !ok {
			return nil, &ExecError{
				Op:  "AttachPaginationSync",
				Err: fmt.Errorf("missing paginate count for '%s'", key),
			}
		}
		cnt, ok := raw.(int64)
		if !ok {
			return nil, &ExecError{
				Op:  "AttachPaginationSync",
				Err: fmt.Errorf("paginate count wrong type for '%s': %T", key, raw),
			}
		}
		sr, exist := searches.UnsafeGet(key)
		if !exist {
			return nil, &ExecError{
				Op:  "AttachPaginationSync",
				Err: fmt.Errorf("search response not found for paginate on key '%s'", key),
			}
		}
		sr.Meta.Paginate = p.Calculate(int(cnt), sr.Meta.Count)

		processedKeys[index] = key
		index++
	}

	return processedKeys, nil
}
