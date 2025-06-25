package tests

import (
	"context"
	"e2e/ent"
	"e2e/ent/teststate"
	"fmt"
	"time"
)

func EnsureBaseSeed(client *ent.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

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
	_, err := client.User.
		Create().
		SetEmail("user1@example.com").
		SetName("User One").
		SetAge(20).
		Save(ctx)
	if err != nil {
		return err
	}

	_, err = client.User.
		Create().
		SetEmail("user2@example.com").
		SetName("User Two").
		SetAge(30).
		SetIsActive(true).
		Save(ctx)
	if err != nil {
		return err
	}

	_, err = client.User.
		Create().
		SetEmail("user3@example.com").
		SetName("User Three").
		SetAge(40).
		SetIsActive(true).
		Save(ctx)
	if err != nil {
		return err
	}
	return nil
}
