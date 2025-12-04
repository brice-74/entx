SHELL := /bin/bash
MAKEFLAGS += --no-print-directory

# Runs end-to-end tests from ./e2e/tests/<path>, with coverage for all entx/* packages except search/extension and e2e project
.PHONY: run/test/e2e
run/test/e2e:
	-@docker compose -f ./e2e/docker-compose.yml run \
		--rm \
		entx-test-svc sh -c " \
			cd e2e && \
			go test -p 1 -v -vet=off \
				-run \"$(func)\" ./tests/$(if $(path),$(path),...) \
				-coverpkg=$$( \
					go list github.com/brice-74/entx/... | grep -v '/e2e' | grep -v 'extension$$' | paste -sd "," \
				-) \
				-coverprofile=./coverage.out \
		" 
	@$(MAKE) gen/test/e2e/html
ifeq ($(DOWN),true)
	@$(MAKE) down/mysql
endif

down/mysql:
	@docker compose -f ./e2e/docker-compose.yml rm -sfv entx-mysql-test-svc

.PHONY: show/test/e2e
show/test/e2e: gen/test/e2e/html
	@cd e2e && { \
		explorer.exe coverage.html; code=$$?; \
		if [ $$code -eq 0 ] || [ $$code -eq 1 ]; then exit 0; else exit $$code; fi; \
	}

.PHONY: gen/test/e2e/html
gen/test/e2e/html:
	@cd e2e && \
		go tool cover -html=coverage.out -o coverage.html