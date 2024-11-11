# Project name
PROJECT_NAME := goatdb

# Docker compose file
DOCKER_COMPOSE_FILE := docker-compose.yml

# Docker compose command
DOCKER_COMPOSE := docker-compose -f $(DOCKER_COMPOSE_FILE)

.PHONY: build build-app up down clean rebuild logs ps test test-api test-db test-coverage test-load

# Build the Docker images
build:
	$(DOCKER_COMPOSE) build

# Build the Go application (for CI)
build-app:
	go build -v ./...

# Start the Docker containers
up:
	$(DOCKER_COMPOSE) up -d

# Stop the Docker containers
down:
	$(DOCKER_COMPOSE) down

# Clean up Docker resources
clean:
	$(DOCKER_COMPOSE) down --rmi all --volumes --remove-orphans
	docker image prune -f

# Rebuild and restart the Docker containers
rebuild: clean build up

# View the logs
logs:
	$(DOCKER_COMPOSE) logs -f

# List the running containers
ps:
	$(DOCKER_COMPOSE) ps

# Default target
all: build up

# Run all tests
test:
	go test -v ./...

# Run only API tests
test-api:
	go test -v ./src/api

# Run only DB tests
test-db:
	go test -v ./src/db

# Run tests with coverage
test-coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Run load tests specifically
test-load:
	go test -v ./src/api -run TestKVControllerLoadTest

# CI pipeline commands
ci: build-app test test-coverage