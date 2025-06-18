package search

// a retirer apres avoir placer les derniere structures

type (
	// ------------------------------
	// Output Types
	// ------------------------------

	AggregatesMeta struct {
		Aggregates map[string]any `json:"aggregates,omitempty"`
	}

	SearchMeta struct {
		AggregatesMeta
		Paginate *PaginateResponse `json:"paginate,omitempty"`
		Count    int               `json:"count,omitempty"`
	}

	/* --------------------------------- */
)
