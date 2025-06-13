package e2e_test

import (
	"context"
	"e2e/ent"
	"e2e/ent/entx"
	"fmt"
	"os"
	"testing"
	"time"

	"entgo.io/ent/dialect"

	"github.com/brice-74/entx/search"
	_ "github.com/go-sql-driver/mysql"
)

var (
	client   *ent.Client
	executor *search.Executor
)

func TestMain(m *testing.M) {
	var err error
	client, err = openAndMigrate()
	if err != nil {
		panic(err)
	}

	executor = search.NewExecutor(
		entx.Graph,
		entx.NewClient(client),
		search.NewConfig(),
	)

	exitCode := m.Run()
	os.Exit(exitCode)
}

func openAndMigrate() (*ent.Client, error) {
	url := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=True",
		os.Getenv("MYSQL_USER"),
		os.Getenv("MYSQL_PASSWORD"),
		os.Getenv("MYSQL_HOST"),
		os.Getenv("MYSQL_PORT"),
		os.Getenv("MYSQL_DATABASE"),
	)

	client, err := ent.Open(dialect.MySQL, url)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Schema.Create(ctx); err != nil {
		return nil, err
	}
	return client, nil
}
