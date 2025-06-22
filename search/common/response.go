package common

type AggregatesResponse = map[string]any

type MetaResponse struct {
	Aggregates AggregatesResponse `json:"aggregates,omitempty"`
}

type MetaSearchResponse struct {
	Paginate *PaginateResponse `json:"paginate,omitempty"`
	Count    int               `json:"count,omitempty"`
}

type SearchResponse struct {
	Data any                 `json:"data,omitempty"`
	Meta *MetaSearchResponse `json:"meta,omitempty"`
}

type SearchesResponse = map[string]*SearchResponse

type GroupResponse struct {
	Searches SearchesResponse `json:"searches,omitempty"`
	Meta     *MetaResponse    `json:"meta,omitempty"`
}

type GroupResponseSync struct {
	Searches SearchesResponse `json:"searches,omitempty"`
	Meta     *MetaResponse    `json:"meta,omitempty"`
}
