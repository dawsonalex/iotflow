.PHONY: help build run test test-integration test-integration-docker clean lint fmt vet commit-check install-tools vendor-tooling

INTEGRATION_IMAGE = iotflow-integration

# ROOT_DIR is the path of the makefile (including trailing slash)
ROOT_DIR := $(dir $(realpath $(lastword $(MAKEFILE_LIST))))
PROJECT_PATH := $(ROOT_DIR:/=)
BIN_NAME = example
BUILD_DIR := bin

# Development tools live in their own module (tools/go.mod) so that linter
# dependencies never enter the library's dependency graph, and so never reach
# the go.sum of anyone importing it. They are built into $(BUILD_DIR) and
# invoked from there.
TOOLS_DIR := tools
TOOLS_STAMP := $(BUILD_DIR)/.tools-stamp
TOOL_PKGS := \
	github.com/golangci/golangci-lint/v2/cmd/golangci-lint

help: ## Display this help message
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*##"; printf "\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Tools

# Rebuilds only when the tools module or this makefile changes, so every other
# target can depend on it without paying a rebuild each time. The makefile is a
# prerequisite because TOOL_PKGS lives here.
#
# tools/ has no vendor directory unless `make vendor-tooling` made one. An
# empty GOFLAGS lets Go use vendor mode only when it exists.
$(TOOLS_STAMP): $(TOOLS_DIR)/go.mod $(TOOLS_DIR)/go.sum makefile
	@echo "Building development tools into $(BUILD_DIR)/..."
	@mkdir -p $(BUILD_DIR)
	@GOFLAGS= go -C $(TOOLS_DIR) build -o ../$(BUILD_DIR)/ $(TOOL_PKGS)
	@touch $@

install-tools: $(TOOLS_STAMP) ## Build the development tools into bin/

vendor-tooling: ## Vendor the development tools so they build without network access
	@echo "Vendoring development tools..."
	@go -C $(TOOLS_DIR) mod vendor
	@echo "Tools vendored into $(TOOLS_DIR)/vendor/"

##@ Linting

lint: $(TOOLS_STAMP) ## Run golangci-lint
	@echo "Running golangci-lint..."
	@$(BUILD_DIR)/golangci-lint run --timeout=5m

fmt: $(TOOLS_STAMP) ## Format Go code
	@echo "Formatting code..."
	@$(BUILD_DIR)/golangci-lint fmt ./...

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
