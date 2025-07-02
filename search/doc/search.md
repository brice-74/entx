[⬅️ Back to search README](../README.md)

# Query Options Input

```json
{
   "select": [],
   "filters": [],
   "includes": [],
   "sorts": [],
   "aggregates": [],
   "page": 0,
   "limit": 0,
   "with_pagination": false,
   "enable_transaction": false,
   "transaction_isolation_level": 0,
}
```

The **QueryOptions** structure defines the main parameters for composing data retrieval queries. It integrates selection, filtering, inclusion of related entities, sorting, aggregation, pagination, and transactional behavior into a single configuration.

More detailed documentation on each element:
  * [`filters`](./filter.md)
  * [`includes`](./include.md)
  * [`sorts`](./sort.md)
  * [`aggregates`](./aggregate.md)

---

## Response

```json
{
   // Top-level list of requested entities
   "data": [
      {
         // Selected fields
         "name": "jojo",
         "age": 12,

         // Meta-information attached to each entity
         "meta": {
            "aggregates": {
               // Each key is the alias you defined in your aggregates
               "total_spent": 1234.56,
               "order_count": 42
            }
         },
         
         // Nested relations returned with the entity
         "edges": {
            // O2M & M2M relations are arrays.
            "orders": [],
            // M2O & O2O relations are objects.
            "profile": {},
         }
      }
   ],

   // Global metadatas about the overall result set
   "meta": {
      "paginate": {
         "from": 0, // Index of the first item in this page 
         "to": 0, // Index of the last item in this page
         "total": 0, // Total number of matching items across all page
         "current_page": 0, // Current page number
         "last_page": 0, // Index of the last page available
         "per_page": 0 // Number of items per page
      },
      "count": 0 // Total number of entities returned in this response
   }
}

```

---

## Fields Explanation

| Field                         | Type                  | Description                   
| ----------------------------- | --------------------- | ----------------------------- 
| `select`                      | *[string]*                                                                  | Specify which fields to return from the root entity.
| `filters`                     | [*[Filter]*](./filter.md)                                                   | Conditions to filter the root entities.
| `includes`                    | [*[Include]*](./include.md)                                                 | Related entities to include, with their own filters, selects, sorts, etc.
| `sort`                        | [*[Sort]*](./sort.md)                                                       | Criteria to order the root entities.
| `aggregates`                  | [*[Aggregate]*](./aggregate.md)                                             | Aggregation functions (e.g., sum, count) on fields of the root entity.
| `page`                        | *int*                                                                       | Page number for pagination.`with_pagination=true`.
| `limit`                       | *int*                                                                       | Maximum number of items per page or total results when pagination is disabled.
| `with_pagination`             | *bool*                                                                      | Enable pagination mode. If `false`, returns all matching results up to `limit`.
| `enable_transaction`          |  *bool*                                                                     | Wrap both data query and pagination count in a single transaction for consistency. Ignored if no pagination or within a transaction group. 
| `transaction_isolation_level` | [*sql.IsolationLevel(int)*](https://pkg.go.dev/database/sql#IsolationLevel) | Specify isolation level for the transaction when `enable_transaction=true`. 

---

## Usage Notes

* **Core Configuration:** A `QueryOptions` instance combines all query parameters. Pass it to your search function to apply selection, filters, includes, sorting, and aggregation in one call.
* **Pagination Flow:** Set `with_pagination` to `true` and provide `page` and `limit` to retrieve paged results. When disabled, the query returns up to `limit` items without counting total pages.
* **Transactional Queries:** To ensure that the data set and its pagination count are consistent, enable `enable_transaction`. The optional `transaction_isolation_level` lets you choose a stricter isolation mode if needed.

---

## Workflows

during standalone execution, 3 paths can be taken by the structure:
* if a pagination is requested as well as a transaction, both the main query and the pagination query will be executed synchronously in a transaction.
* if paging is requested without a transaction, both queries are executed in parallel.
* if no paging is requested, the query is simply executed synchronously.

# Targeted Query Input

This input is very similar to the structure used for [**query options**](./search.md#query-options-input). It simply exposes an additional field allowing the client to specify from which node the query should be made.

The aim is to dissociate these two structures in order to offer developers greater flexibility: they can choose between defining dedicated endpoints, or using a single generic endpoint.

```json
{
   // ... Query options fields 
   "from": "root node",
}
```