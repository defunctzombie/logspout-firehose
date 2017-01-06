#!/bin/bash

docker run -it --rm \
    -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    -e AWS_REGION=$AWS_REGION \
    -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v $(pwd):/go/src/github.com/defunctzombie/logspout-firehose \
    logspout /bin/sh
