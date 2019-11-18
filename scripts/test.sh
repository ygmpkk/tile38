#!/bin/bash

set -e
cd $(dirname "${BASH_SOURCE[0]}")/..

cd tests && go test && cd ..
go test $(go list ./... | grep -v /vendor/ | grep -v /tests)
