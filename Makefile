UI_DIR=cmd/sql-to-logsql/web/ui

.PHONY: ui-install ui-build build backend-build run test all jdbc-build jdbc-test

ui-install:
	cd $(UI_DIR) && npm install

ui-build: ui-install
	cd $(UI_DIR) && npm run build

backend-build:
	go build -v ./cmd/sql-to-logsql

build: ui-build backend-build

run: ui-build
	go run ./cmd/sql-to-logsql

test:
	go test ./...

check:
	bash ./scripts/check-all.sh

lint:
	bash ./scripts/lint-all.sh

all: test check lint build

jdbc-build:
	bash ./scripts/jdbc-build.sh

jdbc-test:
	bash ./scripts/jdbc-build.sh
