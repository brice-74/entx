package e2e_search_test

import (
	"log"
	"os"
	"testing"

	"e2e/ent/entx"
	_ "e2e/ent/runtime"
	"e2e/tests"

	_ "github.com/go-sql-driver/mysql"
)

var (
	client *entx.Client
)

func TestMain(m *testing.M) {
	c, err := tests.OpenAndMigrate()
	if err != nil {
		panic(err)
	}
	defer c.Close()

	client = entx.NewClient(c)

	if err := tests.EnsureBaseSeed(c); err != nil {
		log.Fatalf("failed to seed data: %v", err)
	}

	exitCode := m.Run()
	os.Exit(exitCode)
}
