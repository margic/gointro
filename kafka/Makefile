build:
	docker run --rm --env 'CGO_ENABLED=0' -v ${GOPATH}/src:/go/src -w /go/src/github.com/margic/gointro/kafka golang:1.9 go build -v
	docker build --tag pcrofts/tmkafka .

generate:
	protoc -I . counter.proto --go_out=plugins=grpc:protos