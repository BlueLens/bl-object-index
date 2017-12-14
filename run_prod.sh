#!/usr/bin/env bash

RELEASE_MODE='prod'
PORT=30053

sudo nvidia-docker run -dit --restart unless-stopped \
    -e RELEASE_MODE=$RELEASE_MODE \
    -p $PORT:$PORT bluelens/bl-index:$RELEASE_MODE
