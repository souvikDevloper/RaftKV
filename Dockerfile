FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /raftkv ./cmd/raftkv

FROM alpine:3.22
WORKDIR /app
COPY --from=build /raftkv /app/raftkv
ENTRYPOINT ["/app/raftkv"]
