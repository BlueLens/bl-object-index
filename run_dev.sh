#!/usr/bin/env bash

#sudo nvidia-docker run -dit --restart unless-stopped -p 50053:50053 bluelens/bl-index:dev

RELEASE_MODE='dev'
PORT=50053

sudo nvidia-docker run -dit --restart unless-stopped \
    -e RELEASE_MODE=$RELEASE_MODE \
    -p $PORT:$PORT bluelens/bl-index:$RELEASE_MODE
