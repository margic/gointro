build:
	docker run --rm -v ${GOPATH}/src:/go/src -w /go/src/github.com/margic/gointro/kafka pcrofts/tmkafkabuild go build -tags static -v
	docker build --tag pcrofts/tmkafka .