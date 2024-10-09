# Project name
PROJECT_NAME := goatdb

# Docker compose file
DOCKER_COMPOSE_FILE := docker-compose.yml

# Docker compose command
DOCKER_COMPOSE := docker-compose -f $(DOCKER_COMPOSE_FILE)

.PHONY: build up down clean rebuild logs ps

# Build the Docker images
build:
	$(DOCKER_COMPOSE) build

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