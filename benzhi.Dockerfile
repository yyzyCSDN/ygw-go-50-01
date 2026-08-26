FROM golang:1.23

WORKDIR /app

COPY go.mod go.sum ./
COPY vendor ./vendor

COPY . .

RUN go build -mod=vendor ./...

ENV GOPROXY=off \
    GOSUMDB=off

CMD ["bash"]
