.PHONY: help build run test clean

# ROOT_DIR is the path of the makefile (including trailing slash)
ROOT_DIR := $(dir $(realpath $(lastword $(MAKEFILE_LIST))))
PROJECT_PATH := $(ROOT_DIR:/=)
BIN_NAME = todo

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
	@go test -v -race ./...

build: ## Build the binary
	go build -C '${ROOT_DIR}cmd' -o '${ROOT_DIR}${BIN_NAME}'

run: build ## Build and run the binary bin/imageservice
	${ROOT_DIR}/${BIN_NAME}

clean: ## remove build files
	rm -rv '${ROOT_DIR}${BIN_NAME}'