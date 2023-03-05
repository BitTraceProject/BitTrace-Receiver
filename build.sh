#!/bin/bash

export GOOS="linux" # windows darwin linux

go build -v -o ./output/receiver-cli ./cmd...
echo "build successfully!"
