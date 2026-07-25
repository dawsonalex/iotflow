.PHONY: help build run test test-integration test-integration-docker clean

INTEGRATION_IMAGE = iotflow-integration

# ROOT_DIR is the path of the makefile (including trailing slash)
ROOT_DIR := $(dir $(realpath $(lastword $(MAKEFILE_LIST))))
PROJECT_PATH := $(ROOT_DIR:/=)
BIN_NAME = example

help: ## Display this help message
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*##"; printf "\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Linting

lint: ## Run golangci-lint
	@echo "Running golangci-lint..."
	@go tool golangci-lint run --timeout=5m

fmt: ## Format Go code
	@echo "Formatting code..."
	@go tool golangci-lint fmt ./...

vet: ## Run go vet
	@echo "Running go vet..."
	@go vet ./...

commit-check: fmt vet lint test ## Run all checks before committing
	@echo "✓ All commit checks passed!"

##@ General

test: ## Run tests
	@echo "Running tests..."
	go test -v -race ./...

test-integration: ## Run integration tests against the fake NetworkManager (needs dbus-daemon)
	@echo "Running integration tests..."
	@go test -tags integration -race ./...

test-integration-docker: ## Run the integration tests in a container (for hosts without dbus-daemon)
	@echo "Building integration test image..."
	@docker build -f Dockerfile.integration -t '${INTEGRATION_IMAGE}' .
	@echo "Running integration tests in container..."
	@docker run --rm '${INTEGRATION_IMAGE}'

build: ## Build the binary
	go build -C '${ROOT_DIR}cmd/example' -o '${ROOT_DIR}${BIN_NAME}'

run: build ## Build and run the binary bin/imageservice
	${ROOT_DIR}/${BIN_NAME}

clean: ## remove build files
	rm -rv '${ROOT_DIR}${BIN_NAME}'