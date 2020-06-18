
# Create version tag from git tag
VERSION=$(shell git describe | sed 's/^v//')
REPO=cybermaggedon/evs-parquetwriter
DOCKER=docker
GO=GOPATH=$$(pwd)/go go

all: evs-parquetwriter build

SOURCE=evs-parquetwriter.go config.go model.go parquet.go

evs-parquetwriter: ${SOURCE} go.mod go.sum
	${GO} build -o $@ ${SOURCE}

build: evs-parquetwriter
	${DOCKER} build -t ${REPO}:${VERSION} -f Dockerfile .
	${DOCKER} tag ${REPO}:${VERSION} ${REPO}:latest

push:
	${DOCKER} push ${REPO}:${VERSION}
	${DOCKER} push ${REPO}:latest

