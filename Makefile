fmt:
	poetry run ruff format .
.PHONY: fmt

lint: fmt
	poetry run ruff check . --fix
.PHONY: lint

server-start: fmt
	poetry run uvicorn things_report_archive_service.service:server --port 8001 &
.PHONY: server-start

test-unit: fmt
	poetry run pytest tests/unit/
.PHONY: test-unit

test-integration: fmt
	poetry run pytest tests/integration/
.PHONY: test-integration

test: fmt
	poetry run pytest tests/
.PHONY: test
