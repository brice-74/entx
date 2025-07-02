[⬅️ Back to search README](../README.md)

# Filter Input

This section defines the structure and usage of the **Filter** input for querying data. The **Filter** allows combining logical conditions, navigating relations, and specifying field-level constraints.

---

## Example Input JSON

```json
// from a user entity
{
   "not": {
      "field": "age",
      "operator": "<",
      "value": 18
   },
   "and": [
      {
         "field": "order.total",
         "operator": ">=",
         "value": 100.0
      },
      {
         "relation": "order.items",
         "and": [
            {
               "field": "quantity",
               "operator": ">",
               "value": 2
            },
            {
               "field": "status",
               "operator": "=",
               "value": "shipped"
            }
         ]
      }
   ],
   "or": [
      {
         "field": "country",
         "operator": "IN",
         "value": ["FR", "DE", "ES"]
      },
      {
         "field": "is_active",
         "operator": "=",
         "value": true
      }
   ]
}
```

This JSON expresses the following logic:

1. Exclude users younger than 18 (`not`).
2. Require both:

   * Orders with a `total` of at least 100.0.
   * Within each order, items where:

     * `quantity` is greater than 2, **and**
     * `status` equals `shipped`.
3. Additionally, either:

   * The user is from France, Germany, or Spain, **or**
   * The user `is_active` flag is `true`.

---

## Fields Explanation

| Field       | Type        | Description |
| ----------- | ----------- | ----------- |
| `not`       | [*Filter*](./filter.md#filter-input)      | A nested filter that negates its condition. |
| `and`       | [*[Filter]*](./filter.md#filter-input)  | An array of `Filter` objects, all of which must be true (logical AND). |
| `or`        | [*[Filter]*](./filter.md#filter-input)   | An array of `Filter` objects, at least one of which must be true (logical OR). |
| `relation`  | *string*    | Specifie related entities from context node. Implies that nested filters inside apply within that relation context. |
| `field`     | *string*    | Specify the field from the context node and optionally chain related entities. Like the relationship field, each nested filter will be in the chaining context |
| `operator`  | [*Operator (string)*](./filter.md#supported-operators) | Comparison operator to apply between the field and the value. See below for possible values. |
| `value`     | *number, string, boolean, array*       | The literal or array of literals to compare against. Types may vary, see [types column](./filter.md#supported-operators). |

---

## Supported Operators

| Symbol     | Description                       | Types                  
| ---------- | --------------------------------- | ----------------------- 
| `=`        | Equals                            | number, string, boolean 
| `!=`       | Not equals                        | number, string, boolean
| `>`        | Greater than                      | number                  
| `>=`       | Greater than or equal             | number                  
| `<`        | Less than                         | number                  
| `<=`       | Less than or equal                | number                  
| `like`     | Pattern matching (SQL LIKE style) | string                  
| `not like` | Negative pattern matching         | string                  
| `in`       | Inclusion within a list or set    | array of string, number 
| `not in`   | Exclusion from a list or set      | array of string, number 

Use these operators by setting the `operator` field to the corresponding symbol.

---

## Usage Notes

* **Single Condition:** A filter object with only `field`, `operator`, and `value` applies directly to that field.
* **Combining Conditions:** Use `and` or `or` to combine multiple filters. Nested combinations are allowed.
* **Relations:** To filter on related tables, specify `relation` or chain `field` and nest your filters accordingly.

