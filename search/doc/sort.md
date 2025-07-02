[⬅️ Back to search README](../README.md)

# Sort Input

This section describes the **Sort** input used to order results based on one or more fields, optionally applying aggregate functions on related entities before sorting.

---

## Example Input JSON

```json
// from a user entity
[
   {
      "field": "created_at",
      "direction": "DESC"
   },
   {
      "field": "orders.total",
      "direction": "ASC",
      "aggregate": "sum"
   },
   {
      "field": "items.id",
      "direction": "DESC",
      "aggregate": "count"
   }
]
```

This JSON specifies three sorting rules:

* **Newest first** by `created_at`.
* **Lowest total spent first** by summing `orders.total` for each user.
* **Users with more items first** by counting `items.id`.

---

## Sort Fields Explanation

| Field       | Type                                                           | Description    
| ----------- | -------------------------------------------------------------- | -----------------
| `field`     | *string*                                                       | The field to sort by. Supports dot notation for related entities (e.g., `orders.total`).
| `direction` | *string*                                                       | Sort direction: `ASC` for ascending, `DESC` for descending. Defaults to `ASC`.
| `aggregate` | [*AggType (string)*](./aggregate.md#supported-aggregate-types) | Optional aggregate function to apply before sorting. Requires relations for non-wildcard aggregates.

---

## Usage Notes

* **Simple Sorts:** Provide `field` and `direction` to order by a column in the root entity.
* **Aggregate Sorts:** To sort by a summary value on related entities, include an `aggregate` and use dot notation in `field`.
