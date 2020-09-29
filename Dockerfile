FROM golang:1.14 as builder
ARG RELEASE
ARG VERSION
WORKDIR /workspace
COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

COPY . /workspace
RUN make build-only

FROM ubuntu:18.04 as runtime
RUN apt-get update && apt-get install -y ca-certificates && apt-get clean && rm -rf /var/lib/apt/lists/*
WORKDIR /
COPY --from=builder /workspace/dist/pftaskqueue /usr/local/bin/pftaskqueue
ENTRYPOINT ["/usr/local/bin/pftaskqueue"]
