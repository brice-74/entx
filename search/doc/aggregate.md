[⬅️ Back to search README](../README.md)

# Aggregate Input

This section describes the **Aggregate** input used to compute summary values (like sum, average, count, min, max) on fields of your data. Aggregates can include filters and be applied to related entities.

---

## Example Input JSON

```json
// from a user entity
[
   {
      "field": "orders.total",
      "type": "sum",
      "alias": "total_spent"
   },
   {
      "field": "orders.id",
      "type": "count",
      "distinct": true,
      "alias": "unique_orders"
   },
   {
      "field": "age",
      "type": "avg",
      "alias": "average_age"
   }
]
```

This JSON specifies three aggregates:

* **Sum** of `orders.total`, aliased as `total_spent`.
* **Count distinct** of `orders.id`, aliased as `unique_orders`.
* **Average** of `age`, aliased as `average_age`.

---

## Aggregate Fields Explanation

| Field      | Type                                                            | Description                                                                                       
| ---------- | --------------------------------------------------------------- | --------------------------------------------------------------------------------------------------
| `field`    | *string*                                                        | The target field to aggregate. Supports dot notation for related entities (e.g., `orders.total`). 
| `type`     | [*AggType (string)*](./aggregate.md#supported-aggregate-types)  | The aggregate function to apply.                          
| `alias`    | *string*                                                        | Optional name for the result column. Generated when omitted.                     
| `distinct` | *boolean*                                                       | If `true`, apply the aggregate on distinct values. Only valid for `count`, `sum`, or `avg`.       
| `filters`  | [*[Filter]*](./filter.md#filter-input)                          | Optional filters to apply before aggregation.              

--- 

## Supported Aggregate Types

| Type    | Description                       
| ------- | --------------------------------- 
| `avg`   | Calculates the average of values. 
| `sum`   | Calculates the sum of values.     
| `min`   | Finds the minimum value.          
| `max`   | Finds the maximum value.          
| `count` | Counts the number of values.      

---

## Usage Notes

* **Basic Aggregation:** Provide `field` and `type` to calculate a summary over that column.
* **Aliases:** Use `alias` to name your aggregate result; otherwise a default name is generated.
* **Distinct Values:** Set `distinct=true` to ignore duplicate values (supported for `count`, `sum`, `avg`).
* **Related Data:** Aggregate fields on related entities using dot notation (e.g., `orders.total`).
* **Asterisk (*) operator** Do not specify explicitly (*) in the field, just leave it empty even after chaining.
