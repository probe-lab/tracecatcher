FROM golang:1.20-bullseye AS builder
MAINTAINER Ian Davis <ian.davis@protocol.ai>

ENV SRC_PATH    /build/tracecatcher
ENV GO111MODULE on
ENV GOPROXY     https://proxy.golang.org

RUN apt-get update && apt-get install -y ca-certificates

WORKDIR $SRC_PATH
COPY go.* $SRC_PATH/
RUN go mod download

COPY . $SRC_PATH
ARG GOFLAGS
RUN go build $GOFLAGS -trimpath -mod=readonly

#-------------------------------------------------------------------

#------------------------------------------------------
FROM buildpack-deps:bullseye
MAINTAINER Ian Davis <ian.davis@protocol.ai>

ENV SRC_PATH    /build/tracecatcher

COPY --from=builder $SRC_PATH/tracecatcher /usr/local/bin/tracecatcher
COPY --from=builder /etc/ssl/certs /etc/ssl/certs

ENTRYPOINT ["/usr/local/bin/tracecatcher"]
