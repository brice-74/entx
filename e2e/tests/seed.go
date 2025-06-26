package tests

import (
	"context"
	"e2e/ent"
	"e2e/ent/teststate"
	"fmt"
)

func EnsureBaseSeed(client *ent.Client) error {
	if err := FlushDB(client); err != nil {
		return err
	}

	ctx := context.Background()

	alreadySeeded, err := client.TestState.
		Query().
		Where(teststate.Key("seed_base"), teststate.Done(true)).
		Exist(ctx)
	if err != nil {
		return fmt.Errorf("check seed status: %w", err)
	}

	if alreadySeeded {
		return nil
	}

	if err := SeedBaseData(ctx, client); err != nil {
		return fmt.Errorf("seed error: %w", err)
	}

	return client.TestState.
		Create().
		SetKey("seed_base").
		SetDone(true).
		Exec(ctx)
}

func SeedBaseData(ctx context.Context, client *ent.Client) error {
	users, err := client.User.CreateBulk(
		client.User.Create().
			SetEmail("user1@example.com").
			SetName("User One").
			SetAge(20),
		client.User.Create().
			SetEmail("user2@example.com").
			SetName("User Two").
			SetAge(30),
		client.User.Create().
			SetEmail("user3@example.com").
			SetName("User Three").
			SetAge(40).
			SetIsActive(false),
	).Save(ctx)
	if err != nil {
		return err
	}

	articles, err := client.Article.CreateBulk(
		client.Article.Create().
			SetTitle("Go Concurrency Patterns").
			SetContent("This article explores common concurrency patterns in Go, including channels and goroutines.").
			SetPublished(true).
			SetUserID(users[0].ID),
		client.Article.Create().
			SetTitle("Understanding SQL Joins").
			SetContent("Learn how different types of SQL joins work with real-world examples.").
			SetPublished(true).
			SetUserID(users[0].ID),
		client.Article.Create().
			SetTitle("Docker for Developers").
			SetContent("A beginner-friendly guide to using Docker for local development and deployments.").
			SetPublished(true).
			SetUserID(users[2].ID),
	).Save(ctx)
	if err != nil {
		return err
	}

	tags, err := client.Tag.CreateBulk(
		client.Tag.Create().SetName("Go"),
		client.Tag.Create().SetName("SQL"),
		client.Tag.Create().SetName("DevOps"),
	).Save(ctx)
	if err != nil {
		return err
	}

	_, err = client.ArticleTag.CreateBulk(
		client.ArticleTag.Create().
			SetArticleID(articles[0].ID).
			SetTagID(tags[0].ID),
		client.ArticleTag.Create().
			SetArticleID(articles[1].ID).
			SetTagID(tags[1].ID),
		client.ArticleTag.Create().
			SetArticleID(articles[2].ID).
			SetTagID(tags[2].ID),
		client.ArticleTag.Create().
			SetArticleID(articles[2].ID).
			SetTagID(tags[0].ID),
	).Save(ctx)
	if err != nil {
		return err
	}

	return nil
}
