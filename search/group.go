package search

import (
	stdsql "database/sql"
	"errors"
	"fmt"
)

type TxQueryGroup struct {
	TransactionIsolationLevel *stdsql.IsolationLevel `json:"transaction_isolation_level,omitempty"`
	QueryGroup
}

func (r *TxQueryGroup) PrepareTxGroup(conf *Config, graph Graph) (
	group *TxGroup,
	err error,
) {
	group = new(TxGroup)
	if r.TransactionIsolationLevel != nil {
		group.IsolationLevel = *r.TransactionIsolationLevel
	} else {
		group.IsolationLevel = conf.Transaction.IsolationLevel
	}

	group.Searches, group.Aggregates, err = r.QueryGroup.Prepare(conf, graph)
	return
}

func (tr *TxQueryGroup) ValidateAndPreprocess(c *Config) (countAggregates, countSearches int, err error) {
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
	return tr.QueryGroup.ValidateAndPreprocess(c)
}

type QueryGroup struct {
	Searches   []NamedQuery       `json:"searches,omitempty"`
	Aggregates []OverallAggregate `json:"aggregates,omitempty"`
}

func (r *QueryGroup) Prepare(conf *Config, graph Graph) (
	searches []*NamedQueryBuild,
	aggregates ScalarGroup,
	err error,
) {
	if lenght := len(r.Searches); lenght > 0 {
		searches = make([]*NamedQueryBuild, 0, lenght)
		for i, s := range r.Searches {
			searches[i], err = s.Prepare(i, conf, graph)
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

func (sr *QueryGroup) ValidateAndPreprocess(c *Config) (countAggregates, countSearches int, err error) {
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
