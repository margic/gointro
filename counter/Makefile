build:
	docker run --rm --env 'CGO_ENABLED=0' -v ${GOPATH}/src:/go/src -w /go/src/github.com/margic/gointro/counter golang:1.9 go build -v -o counter
	docker build --tag pcrofts/tmcounter .

generate:
	protoc --go_out=./proto plugins=grpc,import_path=counter counter.proto