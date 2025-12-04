//go:build ignore

package main

import (
	"log"

	"entgo.io/ent/entc"
	"entgo.io/ent/entc/gen"
	searchext "github.com/brice-74/entx/search/extension"
)

func main() {
	cfg := gen.Config{
		Features: []gen.Feature{
			gen.FeatureModifier,
			gen.FeatureExecQuery,
		},
		Target:  "./ent",
		Package: "e2e/ent",
	}
	exts := entc.Extensions(
		searchext.New(),
	)
	if err := entc.Generate("./schema", &cfg, exts); err != nil {
		log.Fatalf("running ent codegen: %v", err)
	}
}
