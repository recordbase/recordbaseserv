BIN := recordbase
IMAGE := recordbase
TARGET := distr
VERSION := $(shell git describe --tags --always --dirty)
TAG := $(VERSION)
REGISTRY := codeallergy
PWD := $(shell pwd)
NOW := $(shell date +"%m-%d-%Y")

all: build

version:
	@echo $(TAG)

deps:
	go install \
		github.com/codeallergy/go-bindata/go-bindata \
		github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway \
		github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2 \
		google.golang.org/protobuf/cmd/protoc-gen-go \
		google.golang.org/grpc/cmd/protoc-gen-go-grpc

proto: version
	rm -f *.swagger.json
	protoc proto/*.proto -I proto -I third_party -I $(GOPATH)/src/github.com/protocolbuffers/protobuf/src --go_out=. --go-grpc_out=. --grpc-gateway_out=logtostderr=true,allow_delete_body=true:. --openapiv2_out=logtostderr=true,allow_delete_body=true:.
	mv *.swagger.json resources/openapi/

bindata: proto
	go-bindata -pkg resources -o pkg/resources/bindata.go -nocompress -nomemcopy -fs -prefix "resources/" resources/...

build: bindata
	rm -rf rsrc.syso
	go mod tidy
	go test -cover ./...
	go build -o $(BIN) -v -ldflags "-X main.Version=$(VERSION) -X main.Build=$(NOW)"

generate:
	npm run generate --prefix webapp

update:
	go get -u ./...

distr: build
	rm -rf $(TARGET)
	mkdir $(TARGET)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o $(TARGET)/$(BIN)_linux -v -ldflags "-X main.Version=$(VERSION) -X main.Build=$(NOW)"
    CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o $(TARGET)/$(BIN)_arm64_linux -v -ldflags "-X main.Version=$(VERSION) -X main.Build=$(NOW)"
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o $(TARGET)/$(BIN)_darwin -v -ldflags "-X main.Version=$(VERSION) -X main.Build=$(NOW)"
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o $(TARGET)/$(BIN).exe -v -ldflags "-X main.Version=$(VERSION) -X main.Build=$(NOW)"

build-arm: bindata
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o $(TARGET)/$(BIN)_arm64_linux -v -ldflags "-X main.Version=$(VERSION) -X main.Build=$(NOW)"

run: build
	env COS=dev ./$(BIN)

test: build
	env COS=test ./$(BIN)

docker:
	docker build --build-arg VERSION=$(VERSION) --build-arg BUILD=$(NOW) -t $(REGISTRY)/$(IMAGE):$(TAG) -f Dockerfile .

docker-run: docker
	docker run -it -p 8080:8080 -p 8443:8443 --env PRECOOK_BOOT --env PRECOOK_AUTH $(REGISTRY)/$(IMAGE):$(TAG) /app/bin/template run

docker-build:
	mkdir -p $(TARGET)
	rm -rf $(TARGET)/$(BIN)_linux
	docker build --build-arg VERSION=$(VERSION) --build-arg BUILD=$(NOW) -t $(REGISTRY)/$(IMAGE):$(TAG)-build -f Dockerfile.build .
	docker run --rm $(REGISTRY)/$(IMAGE):$(TAG)-build > $(TARGET)/$(BIN)_linux

docker-push: docker
	docker push ${REGISTRY}/${IMAGE}:${TAG}
	docker tag ${REGISTRY}/${IMAGE}:${TAG} ${REGISTRY}/${IMAGE}:latest
	docker push ${REGISTRY}/${IMAGE}:latest

clean:
	docker ps -q -f 'status=exited' | xargs docker rm
	echo "y" | docker system prune

licenses:
	go-licenses csv "github.com/recordbase/recordbaseserv" > resources/licenses.txt



