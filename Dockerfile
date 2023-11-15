FROM golang:1.21.2-alpine AS build-stage

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY ./ ./

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./pgreplay ./cmd/pgreplay/main.go

# Deploy the application binary into a lean image
FROM alpine:latest
RUN adduser -D pgreplay-user

WORKDIR /
COPY --from=build-stage /app/pgreplay ./pgreplay

USER pgreplay-user

CMD [ "sh" ]
