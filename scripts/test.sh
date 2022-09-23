#!/bin/bash

set -e
cd $(dirname "${BASH_SOURCE[0]}")/..

export CGO_ENABLED=0

cd tests
go test -coverpkg=../internal/server -coverprofile=/tmp/coverage.out
go tool cover -html=/tmp/coverage.out -o /tmp/coverage.html
echo "details: file:///tmp/coverage.html"
cd ..

# go test -coverpkg=internal/ \
#     $(go list ./... | grep -v /vendor/ | grep -v /tests)
