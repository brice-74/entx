[⬅️ Back to entx README](../README.md)

#  🔍 search

The search module offers several types of input, as well as predefined behavior for processing searches.
Most types can be reused for personal compositions.

## Summary

here's an exhaustive tree of input documentation exposed by the API:
* [`targeted query`](./doc/search.md#targeted-query-input)
   * [`query options`](./doc/search.md#query-options-input)
      * [`filters`](./doc/filter.md)
      * [`includes`](./doc/include.md)
      * [`sorts`](./doc/sort.md)
      * [`aggregates`](./doc/aggregate.md)

## Global Notes

* **Asterisk (*) operator** For selects and aggregations, don't explicitly specify (*) in the field, the program will put it in if no field is specified.