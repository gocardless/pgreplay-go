FROM golang:1.21

RUN apt update && apt -y upgrade
RUN apt install -y make postgresql-client

WORKDIR /app

COPY ./ ./
RUN make bin/pgreplay.linux_amd64

EXPOSE 9445
