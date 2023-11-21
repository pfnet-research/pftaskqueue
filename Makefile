# env
export GO111MODULE=on
export CGO_ENABLED=0

# project metadta
NAME         := pftaskqueue
REVISION     := $(shell git rev-parse --short HEAD)
IMAGE_PREFIX ?= pftaskqueue/
LDFLAGS      := -ldflags="-s -w -X github.com/pfnet-research/pftaskqueue/cmd.Version=dev -X github.com/pfnet-research/pftaskqueue/cmd.Revision=$(REVISION) -extldflags \"-static\""
OUTDIR       ?= ./dist

.DEFAULT_GOAL := build

.PHONY: setup
setup:
	npm install --prefix=.dev markdown-toc && \
	cd $(shell go env GOPATH) && \
	go install golang.org/x/tools/cmd/goimports@latest && \
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin v1.55.2 && \
	go install github.com/elastic/go-licenser@latest


.PHONY: fmt
fmt: build-readme-toc
	$(shell go env GOPATH)/bin/goimports -w cmd/ pkg/
	$(shell go env GOPATH)/bin/go-licenser --licensor "Preferred Networks, Inc."

.PHONY: lint
lint: fmt
	$(shell go env GOPATH)/bin/golangci-lint run --config .golangci.yml --deadline 30m
	git diff --exit-code README.md

.PHONY: build-readme-toc
build-readme-toc:
	.dev/node_modules/markdown-toc/cli.js -i README.md

.PHONY: build
build: fmt lint 
	go build -tags netgo -installsuffix netgo $(LDFLAGS) -o $(OUTDIR)/$(NAME) .

.PHONY: build-only
build-only:
	go build -tags netgo -installsuffix netgo $(LDFLAGS) -o $(OUTDIR)/$(NAME) .

.PHONY: test
test:
	go test  ./...

.PHONY: clean
clean:
	rm -rf "$(OUTDIR)"
