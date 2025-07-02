[⬅️ Back to search README](../README.md)

# Include Input

This section describes the **Include** input used to specify related entities to load alongside the main query. It allows filtering, sorting, selecting fields, aggregating, limiting, and nested includes on related nodes.

---

## Example Input JSON

```json
// from a user entity
{
   "relation": "orders",
   "select": ["id", "total", "created_at"],
   "filters": {
      "and": [
         {
         "field": "status",
         "operator": "=",
         "value": "paid"
         }
      ]
   },
   "sort": [
      {
         "field": "created_at",
         "direction": "desc"
      }
   ],
   "aggregates": {
      "sum": ["total"],
      "count": ["id"]
   },
   "limit": {
      "limit": 10
   },
   "includes": [
      {
         "relation": "items",
         "filters": {
            "field": "quantity",
            "operator": ">",
            "value": 2
         },
         "select": {
            "fields": ["product_id", "quantity"]
         }
      }
   ]
}
```

This JSON specifies:

* Include the related `orders` relation,
* Select only `id`, `total`, and `created_at` fields,
* Filter orders where `status` is `"paid"`,
* Sort orders by `created_at` descending,
* Aggregate sums of `total` and counts of `id`,
* Limit the included orders to 10,
* Nested include `items` relation of each order, filtering on quantity > 2, selecting `product_id` and `quantity`.

---

## Fields Explanation

| Field        | Type                              | Description 
| ------------ | --------------------------------- | ------------
| `relation`   | *string*                          | The related entity or relation path to include. Supports dot notation for nested relations.
| `select`     | *[string]*                        | Specify which fields to include in the selection for this relation. 
| `filters`    | [*[Filter]*](./filter.md)         | Conditions to filter the included related entities. 
| `sort`       | [*[Sort]*](./sort.md)             | Sorting criteria for the included entities. 
| `aggregates` | [*[Aggregate]*](./aggregate.md)   | Aggregation functions (e.g., sum, count) to apply on fields of the included relation. 
| `limit`      | *Limit*                           | Limits the number of included related entities returned. 
| `includes`   | [*[Include]*](./include.md)       | Nested includes for further relations on this included entity. 

---

## Usage Notes

* **Relation Chains:** The `relation` field supports chaining using dot notation (e.g., `"order.items"`) for nested relations.
* **Nested Includes:** You can nest multiple levels of includes, each with their own filters, sorts, selects, and aggregates.
