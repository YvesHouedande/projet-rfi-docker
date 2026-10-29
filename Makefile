SHELL := /bin/bash
DOCKER_SOCK := /var/run/docker.sock

.PHONY: help init build up down logs psql install test sonar-up sonar-down sonar clean
.DEFAULT_GOAL := help

help: ## Affiche cette aide
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

# Remplace la valeur d'une clé de .env (toujours, ou seulement si elle est vide).
define SETVAR
set_var() { sed -i "s|^$$1=.*|$$1=$$(printf '%s' "$$2" | sed 's/[&|]/\\&/g')|" .env; }
is_empty() { grep -qE "^$$1=$$" .env; }
endef
export SETVAR

init: ## Prépare .env (uid, groupe docker, secrets) et le dossier logs/
	@test -S $(DOCKER_SOCK) || { echo "Socket Docker introuvable : $(DOCKER_SOCK)"; exit 1; }
	@test -f .env || cp .env.example .env
	@eval "$$SETVAR"; \
	set_var AIRFLOW_UID "$$(id -u)"; \
	set_var DOCKER_GID "$$(stat -c %g $(DOCKER_SOCK))"; \
	set_var RFI_PROJECT_DIR "$$(pwd)"; \
	if is_empty FERNET_KEY; then \
	  set_var FERNET_KEY "$$(python3 -c 'import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())')"; \
	fi; \
	if is_empty JWT_SECRET; then \
	  set_var JWT_SECRET "$$(python3 -c 'import secrets;print(secrets.token_hex(32))')"; \
	fi
	@mkdir -p logs
	@echo ".env prêt. Lancez : make up"

build: ## Construit l'image rfi-ingest et les images compose
	docker build -t rfi-ingest:latest .
	docker compose build

up: ## Démarre Airflow et PostgreSQL
	@test -f .env || { echo "Fichier .env absent : lancez d'abord 'make init'."; exit 1; }
	docker compose up -d --build

down: ## Arrête les services (conserve les volumes)
	docker compose --profile quality down

logs: ## Suit les logs des services
	docker compose logs -f --tail=100

psql: ## Ouvre psql sur la base RFI
	docker compose exec rfi-postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

install: ## Installe les dépendances de développement
	pip install -r requirements-dev.txt

lint: ## Vérifie le style et la qualité (ruff, sans modifier)
	ruff check src tests dags
	ruff format --check src tests dags

lint-fix: ## Corrige automatiquement le style (modifie les fichiers)
	ruff check --fix src tests dags
	ruff format src tests dags

test: ## Lance pytest avec couverture (coverage.xml pour Sonar)
	pytest --cov --cov-report=xml --cov-report=term

sonar-up: ## Démarre SonarQube (http://localhost:9000)
	docker compose --profile quality up -d sonarqube

sonar-down: ## Arrête SonarQube
	docker compose --profile quality stop sonarqube

sonar: test ## Analyse SonarQube (exige SONAR_TOKEN)
	@test -n "$$SONAR_TOKEN" || { echo "Définissez SONAR_TOKEN (voir README)."; exit 1; }
	docker run --rm --network rfi-net \
	  -e SONAR_HOST_URL=http://sonarqube:9000 \
	  -e SONAR_TOKEN \
	  -v "$(CURDIR):/usr/src" \
	  sonarsource/sonar-scanner-cli

clean: ## Supprime services et volumes, y compris rfi-data
	docker compose --profile quality down -v --remove-orphans
	-docker volume rm rfi-data
