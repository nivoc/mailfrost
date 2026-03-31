.PHONY: build run backup rebaseline restore setup test fmt clean help

BINARY=mail-backup
MAIN_SRC=./cmd/mail-backup

all: build

build:
	@echo "Building $(BINARY)..."
	@go build -o $(BINARY) $(MAIN_SRC)

run:
	@go run $(MAIN_SRC)

backup:
	@go run $(MAIN_SRC) backup

rebaseline:
	@go run $(MAIN_SRC) rebaseline

restore:
	@go run $(MAIN_SRC) restore

setup:
	@go run $(MAIN_SRC) setup

test:
	@echo "Running tests..."
	@go test ./...

fmt:
	@echo "Formatting code..."
	@go fmt ./...

clean:
	@echo "Cleaning up..."
	@rm -f $(BINARY)

help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  build      Build the binary"
	@echo "  backup     Run the backup flow"
	@echo "  rebaseline Accept the current Maildir as the new baseline"
	@echo "  restore    Restore from a kopia snapshot"
	@echo "  setup      Run the interactive setup wizard"
	@echo "  test       Run all tests"
	@echo "  fmt        Format Go source code"
	@echo "  clean      Remove the binary"
	@echo "  help       Show this help"
