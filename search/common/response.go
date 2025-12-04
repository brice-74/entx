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
	Searches   MapSync[string, *SearchResponse]
	Aggregates MapSync[string, any]
}

// UnsafeResponse returns a pointer to a GroupResponse using direct references
// to the underlying maps. This avoids allocations but should not be used
// concurrently or exposed outside of controlled contexts.
func (g *GroupResponseSync) UnsafeResponse() *GroupResponse {
	response := GroupResponse{
		Searches: g.Searches.UnsafeRaw(),
	}
	if aggregates := g.Aggregates.UnsafeRaw(); aggregates != nil {
		response.Meta = &MetaResponse{Aggregates: aggregates}
	}
	return &response
}

// SnapshotResponse returns a pointer to a GroupResponse using safe copies
// (snapshots) of the internal maps. This is suitable for concurrent use
// and for returning responses to external consumers.
func (g *GroupResponseSync) SnapshotResponse() *GroupResponse {
	response := GroupResponse{
		Searches: g.Searches.Snapshot(),
	}
	if aggregates := g.Aggregates.Snapshot(); aggregates != nil {
		response.Meta = &MetaResponse{Aggregates: aggregates}
	}
	return &response
}
