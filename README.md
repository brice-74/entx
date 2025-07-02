# entx *- Extension for entgo.io*

entx is a modular extension toolkit for [ent](https://entgo.io), the powerful ORM for Go. It eliminates repetitive logic that most projects end up rewriting — such as advanced querying, input validation, pagination handling, or mutation helpers.

Instead of re-implementing the same patterns in every service, entx generates them directly from your Ent schema and provides flexible client-side adapters tailored to your application’s needs.

In short: entx automates all the boilerplate that Ent enables — but doesn’t generate by default.

---

## 🚀 Why entx?

ent is powerful, but its internal graph representation (based on schema definitions) is not directly accessible or reusable.

entx builds its own generic and structured graph, based on your schema, and uses it to generate ready-to-use features like search engines.

> Less code to write. Zero logic to repeat.

---

## 🧩 Modular by Design

entx is composed of focused modules. Each one solves a common problem that can be automated based on your Ent schema.

| Module     | Description |
|------------|-------------|
| 🔍 `search`  | Powerful search engine with filters, sorting, pagination, includes, aggregations… |
| 🧬 `mutate` *(coming soon)* | Automated create/update/upsert/attach/detach operations, integration of customized business logic. |

Each module integrates with Ent through the `entc.Extension` system.

### 🔍 `search` (ready to use)

A declarative, type-safe, and extensible search engine for Ent. Includes:

- Dynamic filtering (with AND, OR, nesting)
- Multi-field sorting
- Offset and cursor pagination
- Relational includes
- Grouping and aggregation
- Parallelism and transactions

→ [See full `search` documentation](./search/README.md)

---

## 📦 Installation

> For the moment, generation is done via the search module extension. The future idea is to offer an entx extension by specifying the desired modules.

In your `entc.go`:

```go
import search "github.com/brice-74/entx/search/extension"

func main() {
   opts := []entc.Option{
      entc.Extensions(
         search.New(), // enable search module
      ),
   }

   if err := entc.Generate("./schema", opts...); err != nil {
      log.Fatalf("entc: %v", err)
   }
}
```
