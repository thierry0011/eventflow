# A Makefile is the cheapest documentation you will ever write: it records the
# exact commands the team actually runs, so nobody has to remember them.
.DEFAULT_GOAL := help
SHELL := /bin/bash
COMPOSE := docker compose

.PHONY: help up down clean logs ps migrate seed smoke test lint shell-orders shell-inventory psql redis-cli topics

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS=":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

up: ## Build and start the whole system
	$(COMPOSE) up -d --build
	@echo ""
	@echo "orders     -> http://localhost:8001/api/v1/"
	@echo "inventory  -> http://localhost:8002/docs"
	@echo "notifs     -> http://localhost:8003/health"
	@echo "kafka ui   -> http://localhost:8080"
	@echo "rabbit ui  -> http://localhost:15672 (guest/guest)"

down: ## Stop everything, keep the data
	$(COMPOSE) down

clean: ## Stop everything and delete all volumes (fresh start)
	$(COMPOSE) down -v --remove-orphans

ps: ## Show container status
	$(COMPOSE) ps

logs: ## Tail logs. Use: make logs s=orders-api
	@if [ -z "$(s)" ]; then $(COMPOSE) logs -f --tail=100; else $(COMPOSE) logs -f --tail=100 $(s); fi

migrate: ## Apply database migrations for every service
	$(COMPOSE) exec orders-api python manage.py migrate
	$(COMPOSE) exec inventory-api alembic upgrade head
	$(COMPOSE) exec notifications-api python -m app.cli init-db

seed: ## Load demo products and a demo user
	$(COMPOSE) exec inventory-api python -m app.cli seed
	$(COMPOSE) exec orders-api python manage.py seed_demo

smoke: ## Place one order end-to-end and show the resulting state
	bash scripts/smoke_test.sh

test: ## Run every test suite
	$(COMPOSE) exec orders-api pytest -q
	$(COMPOSE) exec inventory-api pytest -q
	$(COMPOSE) exec notifications-api pytest -q

lint: ## Format and lint everything
	$(COMPOSE) exec orders-api sh -c "ruff check . && ruff format --check ."
	$(COMPOSE) exec inventory-api sh -c "ruff check . && ruff format --check ."
	$(COMPOSE) exec notifications-api sh -c "ruff check . && ruff format --check ."

shell-orders: ## Django shell
	$(COMPOSE) exec orders-api python manage.py shell

shell-inventory: ## Python shell inside the inventory service
	$(COMPOSE) exec inventory-api python

psql: ## Open psql. Use: make psql db=orders_db
	$(COMPOSE) exec postgres psql -U eventflow -d $(or $(db),orders_db)

redis-cli: ## Open redis-cli
	$(COMPOSE) exec redis redis-cli

topics: ## List Kafka topics and consumer groups
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

lag: ## Show consumer lag (the single most important event-driven metric)
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
		--bootstrap-server localhost:9092 --all-groups --describe
