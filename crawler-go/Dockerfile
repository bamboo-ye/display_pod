ARG GO_IMAGE=golang:1.24-alpine
ARG ALPINE_IMAGE=alpine:3.21

FROM ${GO_IMAGE} AS builder
WORKDIR /src
COPY go.mod .
COPY main.go .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/cvpr-crawler-go .

FROM ${ALPINE_IMAGE}
COPY --from=builder /out/cvpr-crawler-go /usr/local/bin/cvpr-crawler-go
ENTRYPOINT ["cvpr-crawler-go"]
