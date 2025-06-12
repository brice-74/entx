package search

import (
	"database/sql"
	"errors"
	"fmt"
)

func (rb *RequestBundle) PrepareGroups(cfg *Config, graph Graph) (
	txGroups []*TransactionGroup,
	scalarGroups []ScalarGroup,
	err error,
) {
	// Prepare standalone composite searches and aggregates
	searches, aggregates, err := rb.CompositeRequest.PrepareComposites(cfg, graph)
	if err != nil {
		return nil, nil, err
	}

	// Build transaction groups: individual transactions + standalone searches
	txCount := len(rb.Transactions)
	searchCount := len(searches)

	if totalTx := txCount + searchCount; totalTx > 0 {
		txGroups = make([]*TransactionGroup, totalTx)
		for i, tx := range rb.Transactions {
			grp, err := tx.PrepareTxGroup(cfg, graph)
			if err != nil {
				return nil, nil, err
			}
			txGroups[i] = grp
		}
		for i, cs := range searches {
			txGroups[txCount+i] = &TransactionGroup{
				IsolationLevel: cfg.Transaction.IsolationLevel,
				Searches:       []*CompositeSearch{cs},
			}
		}
	}

	// Build scalar (aggregate) groups: parallel groups + standalone aggregates
	parallelCount := len(rb.ParallelGroups)
	aggCount := len(aggregates)
	chunkSize := cfg.ScalarQueriesChunkSize
	chunkCount := (aggCount + chunkSize - 1) / chunkSize

	if totalScalar := parallelCount + chunkCount; totalScalar > 0 {
		scalarGroups = make([]ScalarGroup, totalScalar)
		for i, grp := range rb.ParallelGroups {
			scs := make(ScalarGroup, len(grp))
			for j, a := range grp {
				s, err := a.PrepareScalar(graph)
				if err != nil {
					return nil, nil, err
				}
				scs[j] = s
			}
			scalarGroups[i] = scs
		}

		if aggCount > 0 {
			chunks := splitInChunks(aggregates, chunkSize)
			for k, ch := range chunks {
				scalarGroups[parallelCount+k] = ch
			}
		}
	}

	return txGroups, scalarGroups, nil
}

func (r *TransactionRequest) PrepareTxGroup(conf *Config, graph Graph) (
	group *TransactionGroup,
	err error,
) {
	group = new(TransactionGroup)
	if r.TransactionIsolationLevel != nil {
		group.IsolationLevel = sql.IsolationLevel(*r.TransactionIsolationLevel)
	} else {
		group.IsolationLevel = conf.Transaction.IsolationLevel
	}

	group.Searches, group.Aggregates, err = r.CompositeRequest.PrepareComposites(conf, graph)
	return
}

func (r *CompositeRequest) PrepareComposites(conf *Config, graph Graph) (
	searches []*CompositeSearch,
	aggregates ScalarGroup,
	err error,
) {
	if lenght := len(r.Searches); lenght > 0 {
		searches = make([]*CompositeSearch, 0, lenght)
		for i, s := range r.Searches {
			searches[i], err = s.PrepareComposite(i, conf, graph)
			if err != nil {
				return
			}
		}
	}
	if lenght := len(r.Aggregates); lenght > 0 {
		aggregates = make(ScalarGroup, 0, lenght)
		for i, a := range r.Aggregates {
			aggregates[i], err = a.PrepareScalar(graph)
			if err != nil {
				return
			}
		}
	}
	return
}

// ------------------------------
// Validations
// ------------------------------

func (r *RequestBundle) ValidateAndPreprocess(c *Config) (countAggregates, countSearches int, err error) {
	if len(r.Transactions) > 0 && !c.Transaction.EnableClientGroupsInput {
		return 0, 0, &ValidationError{
			Rule: "TransactionGroupsInputDisable",
			Err:  errors.New("transactions groups usage is not allowed"),
		}
	}

	totalAgg, totalSearch := 0, 0

	agg, search, err := r.CompositeRequest.ValidateAndPreprocess(c)
	if err != nil {
		return 0, 0, err
	}
	totalAgg += agg
	totalSearch += search

	for i := range r.Transactions {
		agg, search, err := r.Transactions[i].ValidateAndPreprocess(c)
		if err != nil {
			return 0, 0, err
		}
		totalAgg += agg
		totalSearch += search
	}

	for i1 := range r.ParallelGroups {
		for i2 := range r.ParallelGroups[i1] {
			if err = r.ParallelGroups[i1][i2].ValidateAndPreprocess(&c.FilterConfig); err != nil {
				return 0, 0, err
			}
			totalAgg++
		}
	}

	if c.MaxAggregatesPerRequest != 0 && totalAgg > c.MaxAggregatesPerRequest {
		return 0, 0, &ValidationError{
			Rule: "MaxAggregatesPerBundle",
			Err:  fmt.Errorf("found %d aggregates in bundle, but the maximum allowed is %d", totalAgg, c.MaxAggregatesPerRequest),
		}
	}
	if c.MaxSearchesPerRequest != 0 && totalSearch > c.MaxSearchesPerRequest {
		return 0, 0, &ValidationError{
			Rule: "MaxSearchesPerBundle",
			Err:  fmt.Errorf("found %d searches in bundle, but the maximum allowed is %d", totalSearch, c.MaxSearchesPerRequest),
		}
	}

	return totalAgg, totalSearch, nil
}

func (tr *TransactionRequest) ValidateAndPreprocess(c *Config) (countAggregates, countSearches int, err error) {
	if len(tr.Searches)+len(tr.Aggregates) <= 1 {
		return 0, 0, &ValidationError{
			Rule: "TransactionUnnecessary",
			Err:  errors.New("transaction with a single search or one aggregate is unnecessary"),
		}
	}
	if tr.TransactionIsolationLevel != nil && !c.Transaction.AllowClientIsolationLevel {
		return 0, 0, &ValidationError{
			Rule: "TransactionClientIsolationLevelDisallow",
			Err:  errors.New("transaction_isolation_level parameter is not allowed"),
		}
	}
	return tr.CompositeRequest.ValidateAndPreprocess(c)
}

func (sr *CompositeRequest) ValidateAndPreprocess(c *Config) (countAggregates, countSearches int, err error) {
	if c.MaxAggregatesPerRequest != 0 && len(sr.Aggregates) > c.MaxAggregatesPerRequest {
		return 0, 0, &ValidationError{
			Rule: "MaxAggregatesPerRequest",
			Err:  fmt.Errorf("found %d aggregates, but the maximum allowed is %d", len(sr.Aggregates), c.MaxAggregatesPerRequest),
		}
	}
	if c.MaxSearchesPerRequest != 0 && len(sr.Searches) > c.MaxSearchesPerRequest {
		return 0, 0, &ValidationError{
			Rule: "MaxSearchesPerRequest",
			Err:  fmt.Errorf("found %d searches, but the maximum allowed is %d", len(sr.Searches), c.MaxSearchesPerRequest),
		}
	}

	for i := range sr.Aggregates {
		if err = sr.Aggregates[i].ValidateAndPreprocess(&c.FilterConfig); err != nil {
			return 0, 0, err
		}
	}
	for i := range sr.Searches {
		if err = sr.Searches[i].ValidateAndPreprocess(c); err != nil {
			return 0, 0, err
		}
	}

	return len(sr.Aggregates), len(sr.Searches), nil
}
