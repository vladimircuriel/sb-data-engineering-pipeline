.PHONY: up down down-all restart restart-all

up:
	@echo "Checking if abctl is installed..."
	@if ! command -v abctl >/dev/null 2>&1; then \
		echo "Error: abctl is not installed. Please install it first. at https://docs.airbyte.com/platform/using-airbyte/getting-started/oss-quickstart#part-2-install-abctl"; \
		exit 1; \
	fi

	@echo "Starting Airbyte..."
	abctl local install

	@echo "Extracting Airbyte credentials..."
	@CREDS="$$(abctl local credentials 2>/dev/null | sed 's/\x1b\[[0-9;]*m//g')"; \
	EMAIL="$$(echo "$$CREDS" | grep 'Email:' | awk '{print $$2}')"; \
	PASSWORD="$$(echo "$$CREDS" | grep 'Password:' | awk '{print $$2}')"; \
	CLIENT_ID="$$(echo "$$CREDS" | grep 'Client-Id:' | awk '{print $$2}')"; \
	CLIENT_SECRET="$$(echo "$$CREDS" | grep 'Client-Secret:' | awk '{print $$2}')"; \
	sed -i '' '/^AIRBYTE_EMAIL=/d;/^AIRBYTE_PASSWORD=/d;/^CLIENT_ID=/d;/^CLIENT_SECRET=/d' .env; \
	echo "AIRBYTE_EMAIL=$$EMAIL" >> .env; \
	echo "AIRBYTE_PASSWORD=$$PASSWORD" >> .env; \
	echo "CLIENT_ID=$$CLIENT_ID" >> .env; \
	echo "CLIENT_SECRET=$$CLIENT_SECRET" >> .env; \
	echo "Airbyte credentials saved to .env"

	@echo "Building dbt image..."
	docker compose build dbt
	@echo "Starting Airflow and ClickHouse stack..."
	docker compose up --build -d

restart-up:
	@echo "Building dbt image..."
	docker compose build dbt
	@echo "Starting Airflow and ClickHouse stack..."
	docker compose up --build -d

down:
	@echo "Stopping Airflow and ClickHouse stack..."
	docker compose down

down-all:
	@echo "Stopping Airflow and ClickHouse stack..."
	docker compose down

	@echo "Stopping Airbyte..."
	abctl local uninstall

restart: down restart-up

restart-all: down-all up

reload-dags:
	@echo "Reloading Airflow DAGs..."
	docker compose restart airflow-dag-processor
