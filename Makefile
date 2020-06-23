# env
export GO111MODULE=on
export CGO_ENABLED=0

# project metadta
NAME         := pftaskqueue
VERSION      ?= $(if $(RELEASE),$(shell cat ./VERSION),$(shell cat ./VERSION)-dev)
REVISION     := $(shell git rev-parse --short HEAD)
IMAGE_TAG    ?= $(if $(RELEASE),$(VERSION),$(VERSION)-$(REVISION))
LDFLAGS      := -ldflags="-s -w -X \"github.com/pfnet-research/pftaskqueue/cmd.Version=$(VERSION)\" -X \"github.com/pfnet-research/pftaskqueue/cmd.Revision=$(REVISION)\" -extldflags \"-static\""
OUTDIR       ?= ./dist

.DEFAULT_GOAL := build

.PHONY: setup
setup:
	npm install --prefix=.dev markdown-toc && \
	npm install --prefix=.dev github-release-notes && \
	cd $(shell go env GOPATH) && \
	go get -u golang.org/x/tools/cmd/goimports && \
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin v1.27.0 && \
	go get -u github.com/elastic/go-licenser && \
	go get -u github.com/tcnksm/ghr

.PHONY: fmt
fmt:
	$(shell go env GOPATH)/bin/goimports -w cmd/ pkg/
	$(shell go env GOPATH)/bin/go-licenser --licensor "Preferred Networks, Inc."

.PHONY: lint
lint: fmt
	$(shell go env GOPATH)/bin/golangci-lint run --config .golangci.yml --deadline 30m

.PHONY: build-readme-toc
build-readme-toc:
	.dev/node_modules/markdown-toc/cli.js -i README.md

.PHONY: build
build: fmt lint build-readme-toc
	go build -tags netgo -installsuffix netgo $(LDFLAGS) -o $(OUTDIR)/$(NAME) .

.PHONY: build-only
build-only: 
	go build -tags netgo -installsuffix netgo $(LDFLAGS) -o $(OUTDIR)/$(NAME) .

.PHONY: test
test: fmt lint
	go test  ./...

.PHONY: clean
clean:
	rm -rf "$(OUTDIR)"

.PHONY: build-image
build-image:
	docker build -t $(shell make -e docker-tag) --build-arg RELEASE=$(RELEASE) --target runtime .
	docker tag $(shell make -e docker-tag) $(IMAGE_PREFIX)pftaskqueue:$(VERSION)
	if [ "$(RELEASE)" != "" ]; then \
		docker tag $(shell make -e docker-tag) $(IMAGE_PREFIX)pftaskqueue:latest; \
	else \
		docker tag $(shell make -e docker-tag) $(IMAGE_PREFIX)pftaskqueue:dev; \
	fi

.PHONY: push-image
push-image:
	docker push $(shell make -e docker-tag)
	docker push $(IMAGE_PREFIX)pftaskqueue:$(VERSION)
	if [ "$(RELEASE)" != "" ]; then \
	  docker push $(IMAGE_PREFIX)pftaskqueue:latest; \
	else \
	  docker push $(IMAGE_PREFIX)pftaskqueue:dev; \
	fi

.PHONY: docker-tag
docker-tag:
	@echo $(IMAGE_PREFIX)pftaskqueue:$(IMAGE_TAG)

#
# Release
#
guard-%:
	@ if [ "${${*}}" = "" ]; then \
    echo "Environment variable $* is not set"; \
		exit 1; \
	fi

.PHONY: build-only-all
build-only-all: build-only-linux build-only-darwin build-only-windows

.PHONY: build-only-linux-amd64
build-only-linux-amd64:
	make build-only \
		GOOS=linux \
		GOARCH=amd64 \
		NAME=pftaskqueue-linux-amd64

.PHONY: build-only-linux
build-only-linux: build-only-linux-amd64

.PHONY: build-only-darwin
build-only-darwin:
	make build-only \
		GOOS=darwin \
		NAME=pftaskqueue-darwin-amd64

.PHONY: build-only-windows
build-only-windows:
	make build-only \
		GOARCH=amd64 \
		GOOS=windows \
		NAME=pftaskqueue-windows-amd64.exe

.PHONY: release
release: release-code release-executables release-image

.PHONY: release-code
release-code: guard-RELEASE guard-RELEASE_TAG
	git diff --quiet HEAD || (echo "your current branch is dirty" && exit 1)
	git tag $(RELEASE_TAG) $(REVISION)
	git push origin $(RELEASE_TAG)
	@[ "$${GITHUB_API}" != "" ] && GREN_GITHUB_API_OPT="-a $${GITHUB_API%/}"; \
		GREN_GITHUB_TOKEN=$${GITHUB_TOKEN} .dev/node_modules/github-release-notes/bin/gren.js release --tags $(RELEASE_TAG) $${GREN_GITHUB_API_OPT}

.PHONY: release-executables
release-executables: guard-RELEASE guard-RELEASE_TAG
	@GITHUB_TOKEN=$(GITHUB_TOKEN)
	git diff --quiet HEAD || (echo "your current branch is dirty" && exit 1)
	git checkout $(RELEASE_TAG)
	make clean build-only-all
	ghr $(RELEASE_TAG) $(OUTDIR)
	git checkout -

.PHONY: release-image
release-image: guard-RELEASE guard-RELEASE_TAG
	git diff --quiet HEAD || (echo "your current branch is dirty" && exit 1)
	git checkout $(RELEASE_TAG)
	make build-image
	docker push $(shell make -e docker-tag RELEASE=$(RELEASE))
	git checkout -
