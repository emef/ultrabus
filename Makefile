PROTOC ?= protoc

all: build

build: deps
	go build ./...

clean:
	rm -rf ./pb/*
	go clean -i ./...

deps:
	go get -t -d -v ./...

install:
	go install ./...

proto:
	go get google.golang.org/grpc
	go get github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway
	go get github.com/golang/protobuf/protoc-gen-go

	$(PROTOC) \
		-I/usr/local/include \
		-I/usr/include \
		-I. \
		-I$$GOPATH/src \
		-I$$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
		--go_out=Mgoogle/api/annotations.proto=github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis/google/api,plugins=grpc:pb \
		*.proto

	$(PROTOC) \
		-I/usr/local/include \
		-I/usr/include \
		-I. \
		-I$$GOPATH/src \
		-I$$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
		--grpc-gateway_out=logtostderr=true:pb \
		*.proto

test: deps
	go test -v -cpu 1,4 ./...

docker-build: test
	docker build -t emef/ultrabus .

docker-push: docker-build
	docker push emef/ultrabus:latest

.PHONY: \
	all \
	build \
	deps \
	clean \
	test \
	install \
	proto \
	docker-build \
	docker-push
