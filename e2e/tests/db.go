package tests

import (
	"context"
	"e2e/ent"
	"fmt"
	"os"
	"testing"

	"entgo.io/ent/dialect"
)

func OpenAndMigrateDB() (*ent.Client, error) {
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

	if err := client.Schema.Create(context.Background()); err != nil {
		return nil, err
	}
	return client, nil
}

func FlushDBt(t *testing.T, client *ent.Client) {
	if err := FlushDB(client); err != nil {
		t.Fatal(t)
	}
}

func FlushDB(client *ent.Client) error {
	ctx := context.Background()
	tx, err := client.Tx(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}

	// Récupération des noms de tables à truncater (hors migrations)
	rows, err := tx.QueryContext(ctx, `
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = DATABASE() AND table_name NOT LIKE '%migrations%'
	`)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("failed to retrieve table names: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("failed to scan table name: %v", err)
		}
		tables = append(tables, table)
	}

	// Disable foreign key checks
	if _, err := tx.ExecContext(ctx, `SET FOREIGN_KEY_CHECKS = 0`); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("failed to disable foreign key checks: %v", err)
	}

	// Truncate all tables
	for _, table := range tables {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE `%s`", table)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("failed to truncate table %s: %v", table, err)
		}
	}

	// Enable foreign key checks
	if _, err := tx.ExecContext(ctx, `SET FOREIGN_KEY_CHECKS = 1`); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("failed to re-enable foreign key checks: %v", err)
	}

	// Commit
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}
