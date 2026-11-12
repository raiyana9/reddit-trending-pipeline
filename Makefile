SHELL := /bin/bash
PY    := uv run python
COMPOSE := docker compose

.PHONY: help install up down topics test test-unit test-integration lint fmt \
        demo demo-produce live bench clean logs frontend-dev frontend-test requirements scale-workers

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n",$$1,$$2}'

install:  ## Install Python (uv) and frontend dependencies
	uv sync
	cd frontend && npm install

up:  ## Start Kafka + Redis and create topics
	$(COMPOSE) up -d --wait kafka redis
	$(PY) scripts/create_topics.py

down:  ## Stop everything and remove volumes
	$(COMPOSE) --profile app --profile demo --profile live down -v --remove-orphans

topics:  ## (Re)create Kafka topics idempotently
	$(PY) scripts/create_topics.py

test: test-unit  ## Run unit tests (default)

test-unit:  ## Python + frontend unit tests (no infrastructure needed)
	uv run pytest -m "not integration" -q
	cd frontend && npm test -- --run

test-integration: up  ## Integration tests against the compose stack
	uv run pytest -m integration -q

lint:  ## ruff + mypy + frontend lint/typecheck
	uv run ruff check .
	uv run mypy shared services scripts
	cd frontend && npm run lint && npx tsc --noEmit

fmt:  ## Auto-format
	uv run ruff check --fix .
	uv run ruff format .
	cd frontend && npx prettier --write src

demo-produce:  ## Replay data/sample_posts.jsonl onto reddit.raw (host, needs `make up`)
	$(PY) -m services.producer replay --speed 10

demo:  ## One command: full stack in Docker + replay producer at 10x
	@test -f .env || cp .env.example .env
	$(COMPOSE) --profile app --profile demo up -d --build
	@python3 scripts/wait_ready.py http://localhost:8000/health 300
	@python3 scripts/wait_ready.py http://localhost:3000/ 60
	@echo ""
	@echo "  Dashboard:  http://localhost:3000"
	@echo "  API:        http://localhost:8000/api/trends"
	@echo "  WebSocket:  ws://localhost:8000/ws/trends"
	@echo ""
	@echo "  Stop with:  make down"

live:  ## Full stack with the live Reddit producer (requires Reddit creds in .env)
	@test -f .env || (echo "Missing .env — cp .env.example .env and fill in REDDIT_* vars" && exit 1)
	@grep -qE '^REDDIT_CLIENT_ID=.+' .env || (echo "REDDIT_CLIENT_ID is empty in .env" && exit 1)
	$(COMPOSE) --profile app --profile live up -d --build
	@python3 scripts/wait_ready.py http://localhost:8000/health 300
	@echo "  Dashboard:  http://localhost:3000"

bench:  ## Run throughput / latency / freshness benchmarks -> BENCHMARKS.md (~12 min)
	@test -f .env || cp .env.example .env
	$(COMPOSE) --profile app up -d --build
	-$(COMPOSE) --profile app --profile demo stop replay_producer
	@python3 scripts/wait_ready.py http://localhost:8000/health 300
	BENCH_HOST_CPU="$$(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -p)" \
	BENCH_HOST_CORES="$$(sysctl -n hw.ncpu 2>/dev/null || nproc)" \
	BENCH_HOST_OS="$$(uname -sm)" \
	BENCH_HOST_DOCKER="$$(docker info --format '{{.NCPU}} CPUs')" \
	$(COMPOSE) --profile app --profile bench up --build --no-deps --exit-code-from bench bench; \
	status=$$?; \
	$(COMPOSE) --profile bench cp bench:/tmp/BENCHMARKS.md BENCHMARKS.md && echo "wrote BENCHMARKS.md"; \
	exit $$status

logs:  ## Tail service logs
	$(COMPOSE) --profile app --profile demo --profile live logs -f --tail=50

frontend-dev:  ## Vite dev server on :5173
	cd frontend && npm run dev

requirements:  ## Re-export requirements.txt (used by the Docker images) from uv.lock
	uv export --frozen --no-dev --no-hashes --no-emit-project -o requirements.txt

scale-workers:  ## Run 3 NLP worker replicas
	$(COMPOSE) --profile app up -d --scale nlp_worker=3 nlp_worker

clean:
	rm -rf .pytest_cache .ruff_cache .mypy_cache frontend/dist
