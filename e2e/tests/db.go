package tests

import (
	"context"
	"e2e/ent"
	"fmt"
	"os"
	"time"

	"entgo.io/ent/dialect"
)

func OpenAndMigrate() (*ent.Client, error) {
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
