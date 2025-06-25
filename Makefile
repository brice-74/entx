SHELL := /bin/bash

# Runs end-to-end tests from ./e2e/tests/search/<path>, with coverage for all search/* packages except search/extension
.PHONY: run/test/e2e
run/test/e2e:
	-@docker compose -f ./e2e/docker-compose.yml run \
		--rm \
		entx-test-svc sh -c " \
			cd e2e && \
			go test -p 1 -v -vet=off \
				-run \"$(func)\" ./tests/$(path) \
				-coverpkg=$$(go list github.com/brice-74/entx/search/... | grep -v 'extension$$' | paste -sd "," -) \
				-coverprofile=./coverage.out \
		" 
ifeq ($(DOWN),true)
	@$(MAKE) down/mysql
endif

down/mysql:
	@docker compose -f ./e2e/docker-compose.yml rm -sfv entx-mysql-test-svc

.PHONY: show/test/e2e
show/test/e2e:
	@cd e2e && \
		go tool cover -html=coverage.out -o coverage.html && { \
			explorer.exe coverage.html; code=$$?; \
			if [ $$code -eq 0 ] || [ $$code -eq 1 ]; then exit 0; else exit $$code; fi; \
		}
