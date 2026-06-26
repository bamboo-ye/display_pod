ARG GO_IMAGE=golang:1.22-alpine
ARG ALPINE_IMAGE=alpine:3.20

FROM ${GO_IMAGE} AS build
WORKDIR /src
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
RUN go build -o /out/display-pod-api ./cmd/server

FROM ${ALPINE_IMAGE}
RUN adduser -D -H appuser
USER appuser
COPY --from=build /out/display-pod-api /usr/local/bin/display-pod-api
EXPOSE 8080
CMD ["display-pod-api"]
