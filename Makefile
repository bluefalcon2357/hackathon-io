.PHONY: dev install demo-recorded demo-live test lint clean docker deploy teardown

install:
	pip install -e ".[dev]"

dev:
	uvicorn backend.main:app --reload --host 0.0.0.0 --port 8080

demo-recorded:
	./scripts/demo_recorded.sh

demo-live:
	./scripts/demo_live.sh

test:
	pytest -v

lint:
	ruff check backend tests

clean:
	rm -rf chunks/ /tmp/factcheck-* .pytest_cache .ruff_cache

docker:
	docker build -t hackathon-io .
	docker run --rm -p 8080:8080 --env-file .env hackathon-io

deploy:
	./scripts/deploy.sh

setup-trigger:
	./scripts/setup-trigger.sh

teardown:
	./scripts/teardown.sh
