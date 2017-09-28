build:
	docker run --rm --env 'CGO_ENABLED=0' -v ${GOPATH}/src:/go/src -w /go/src/github.com/margic/gointro/simpleserver golang:1.9 go build -v -o server
	docker build --tag pcrofts/tmsimple .
