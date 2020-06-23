# env
export GO111MODULE=on
export CGO_ENABLED=0

# project metadta
NAME         := pftaskqueue
VERSION      ?= $(if $(RELEASE),$(shell git semv),$(shell git semv patch -p))
REVISION     := $(shell git rev-parse --short HEAD)
IMAGE_PREFIX ?= pftaskqueue/
IMAGE_NAME   := $(if $(RELEASE),release,dev)
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
	go get -u github.com/tcnksm/ghr && \
	go get -u github.com/linyows/git-semv/cmd/git-semv

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
	docker build -t $(shell make -e docker-tag) --build-arg RELEASE=$(RELEASE) --build-arg VERSION=$(VERSION) --target runtime .
	docker tag $(shell make -e docker-tag) $(IMAGE_PREFIX)$(IMAGE_NAME):$(VERSION)  # without revision
	docker tag $(shell make -e docker-tag) $(IMAGE_PREFIX)$(IMAGE_NAME):latest      # latest

.PHONY: push-image
push-image:
	docker push $(shell make -e docker-tag)
	docker push $(IMAGE_PREFIX)$(IMAGE_NAME):$(VERSION) # without revision
	docker push $(IMAGE_PREFIX)$(IMAGE_NAME):latest     # latest

.PHONY: docker-tag
docker-tag:
	@echo $(IMAGE_PREFIX)$(IMAGE_NAME):$(IMAGE_TAG)
