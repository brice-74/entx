.PHONY: run/test
run/test:
	-@docker compose run --rm entx-test-svc sh -c "go test -p 1 -v -vet=off -run \"$(func)\" $(path)"
	@echo "Cleaning up resources..."
	-@docker compose down --volumes --remove-orphans