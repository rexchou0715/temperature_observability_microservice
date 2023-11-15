#!/bin/bash

USAGE="
Usage: run.sh <topic>

    topic: The topic where messages are to be produced.
"

if ! (( $# > 0 )); then
    echo "$USAGE"
    exit 1
fi

topic="$1"

docker build \
    -f test_producer/Dockerfile_producer \
    -t image/experiment-producer ./test_producer 


for i in {0..10}; do
	docker run \
	    --rm \
	    -v $(pwd)/auth:/usr/src/cc-assignment-2023/experiment-producer/auth \
	    image/experiment-producer \
	    --topic "$topic"
done
