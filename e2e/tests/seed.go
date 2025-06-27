package tests

import (
	"context"
	"e2e/ent"
	"time"
)

func SeedBaseData(ctx context.Context, client *ent.Client) error {
	if err := FlushDB(ctx, client); err != nil {
		return err
	}

	if err := WithTx(ctx, client, func(ctx context.Context, client *ent.Client) error {
		if err := client.User.CreateBulk(
			client.User.Create().
				SetID(1).
				SetEmail("user1@example.com").
				SetName("User One").
				SetAge(20),
			client.User.Create().
				SetID(2).
				SetEmail("user2@example.com").
				SetName("User Two").
				SetAge(30),
			client.User.Create().
				SetID(3).
				SetEmail("user3@example.com").
				SetName("User Three").
				SetAge(40).
				SetIsActive(false),
		).Exec(ctx); err != nil {
			return err
		}

		if err := client.Article.CreateBulk(
			client.Article.Create().
				SetID(1).
				SetTitle("Go Concurrency Patterns").
				SetContent("This article explores common concurrency patterns in Go, including channels and goroutines.").
				SetPublished(true).
				SetUserID(1),
			client.Article.Create().
				SetID(2).
				SetTitle("Understanding SQL Joins").
				SetContent("Learn how different types of SQL joins work with real-world examples.").
				SetPublished(true).
				SetUserID(1),
			client.Article.Create().
				SetID(3).
				SetTitle("Docker for Developers").
				SetContent("A beginner-friendly guide to using Docker for local development and deployments.").
				SetPublished(true).
				SetUserID(3),
		).Exec(ctx); err != nil {
			return err
		}

		if err := client.Comment.CreateBulk(
			client.Comment.Create().
				SetID(1).
				SetBody("Very good article, I learned a lot about goroutines.").
				SetUserID(2).    // User 2
				SetArticleID(1), // Article 1
			client.Comment.Create().
				SetID(2).
				SetBody("Thanks for this clear article on SQL joins!").
				SetCreatedAt(time.Now()).
				SetUserID(1).    // User 1
				SetArticleID(2), // Article 2
			client.Comment.Create().
				SetID(3).
				SetBody("Great explanation, I can't wait to see what happens next.").
				SetCreatedAt(time.Now()).
				SetUserID(3).    // User 3
				SetArticleID(2), // Article 2
		).Exec(ctx); err != nil {
			return err
		}

		if err := client.Tag.CreateBulk(
			client.Tag.Create().SetID(1).SetName("Go"),
			client.Tag.Create().SetID(2).SetName("SQL"),
			client.Tag.Create().SetID(3).SetName("DevOps"),
		).Exec(ctx); err != nil {
			return err
		}

		if err := client.ArticleTag.CreateBulk(
			client.ArticleTag.Create().
				SetArticleID(1). // Article 1
				SetTagID(1),     // Tag Go
			client.ArticleTag.Create().
				SetArticleID(2). // Article 2
				SetTagID(2),     // Tag SQL
			client.ArticleTag.Create().
				SetArticleID(3). // Article 3
				SetTagID(3),     // Tag DevOps
			client.ArticleTag.Create().
				SetArticleID(3). // Article 3
				SetTagID(1),     // Tag Go
		).Exec(ctx); err != nil {
			return err
		}

		if err := client.Department.CreateBulk(
			client.Department.Create().SetID(1).SetName("DG"),
			client.Department.Create().SetID(2).SetName("DRH"),
			client.Department.Create().SetID(3).SetName("DSI"),
		).Exec(ctx); err != nil {
			return err
		}

		if err := client.Employee.CreateBulk(
			client.Employee.Create().
				SetID(1).
				SetUserID(1).       // User 1
				SetDepartmentID(1), // Department DG
			client.Employee.Create().
				SetID(2).
				SetUserID(2).       // User 2
				SetDepartmentID(2). // Department DRH
				SetManagerID(1),
			client.Employee.Create().
				SetID(3).
				SetUserID(3).       // User 3
				SetDepartmentID(3). // Department DSI
				SetManagerID(1),
		).Exec(ctx); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	return ResetAutoIncrement(ctx, client)
}
