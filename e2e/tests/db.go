package tests

import (
	"context"
	"database/sql"
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

func WithTx(
	ctx context.Context,
	client *ent.Client,
	fn func(ctx context.Context, client *ent.Client) error,
) error {
	tx, err := client.Tx(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer func() {
		if v := recover(); v != nil {
			panic(rollback(tx, err))
		}
	}()
	if err := fn(ctx, tx.Client()); err != nil {
		return rollback(tx, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

func rollback(tx *ent.Tx, err error) error {
	if rerr := tx.Rollback(); rerr != nil {
		err = fmt.Errorf("%w: rolling back transaction: %v", err, rerr)
	}
	return err
}

func FlushDBt(t *testing.T, client *ent.Client) {
	ctx := context.Background()
	if err := FlushDB(ctx, client); err != nil {
		t.Fatal(t)
	}
}

func FlushDB(ctx context.Context, client *ent.Client) error {
	return WithTx(ctx, client, func(ctx context.Context, client *ent.Client) error {
		// Retrieve table names to be truncated (excluding migrations)
		rows, err := client.QueryContext(ctx, `
			SELECT table_name 
			FROM information_schema.tables 
			WHERE table_schema = DATABASE()
		`)
		if err != nil {
			return fmt.Errorf("failed to retrieve table names: %v", err)
		}
		defer rows.Close()

		var tables []string
		for rows.Next() {
			var table string
			if err := rows.Scan(&table); err != nil {
				return fmt.Errorf("failed to scan table name: %v", err)
			}
			tables = append(tables, table)
		}

		// Disable foreign key checks
		if _, err := client.ExecContext(ctx, `SET FOREIGN_KEY_CHECKS = 0`); err != nil {
			return fmt.Errorf("failed to disable foreign key checks: %v", err)
		}

		// Truncate all tables
		for _, table := range tables {
			if _, err := client.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE `%s`", table)); err != nil {
				return fmt.Errorf("failed to truncate table %s: %v", table, err)
			}
		}

		// Enable foreign key checks
		if _, err := client.ExecContext(ctx, `SET FOREIGN_KEY_CHECKS = 1`); err != nil {
			return fmt.Errorf("failed to re-enable foreign key checks: %v", err)
		}

		return nil
	})
}

func ResetAutoIncrement(ctx context.Context, client *ent.Client) error {
	// Retrieve tables with an auto_increment id column
	rows, err := client.QueryContext(ctx, `
			SELECT DISTINCT TABLE_NAME
			FROM INFORMATION_SCHEMA.COLUMNS
			WHERE TABLE_SCHEMA = DATABASE()
			AND COLUMN_NAME = 'id'
			AND EXTRA LIKE '%auto_increment%'
		`)
	if err != nil {
		return fmt.Errorf("failed to retrieve auto_increment tables: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return fmt.Errorf("failed to scan table name: %v", err)
		}
		tables = append(tables, table)
	}

	// Disable foreign keys
	if _, err := client.ExecContext(ctx, `SET FOREIGN_KEY_CHECKS = 0`); err != nil {
		return fmt.Errorf("failed to disable foreign key checks: %v", err)
	}

	// Reset auto_increment for each table to max(id)+1 or 1 if empty
	for _, table := range tables {
		rows, err := client.QueryContext(ctx, fmt.Sprintf("SELECT MAX(id) FROM `%s`", table))
		if err != nil {
			return fmt.Errorf("failed to get max id from table %s: %v", table, err)
		}
		defer rows.Close()

		var maxID sql.NullInt64
		if rows.Next() {
			if err := rows.Scan(&maxID); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("no rows returned for max id query on table %s", table)
		}

		nextAutoInc := int64(1)
		if maxID.Valid && maxID.Int64 > 0 {
			nextAutoInc = maxID.Int64 + 1
		}

		if _, err := client.ExecContext(ctx, fmt.Sprintf("ALTER TABLE `%s` AUTO_INCREMENT = %d", table, nextAutoInc)); err != nil {
			return fmt.Errorf("failed to reset auto_increment for table %s: %v", table, err)
		}
	}

	// Reactivate foreign keys
	if _, err := client.ExecContext(ctx, `SET FOREIGN_KEY_CHECKS = 1`); err != nil {
		return fmt.Errorf("failed to re-enable foreign key checks: %v", err)
	}

	return nil
}
