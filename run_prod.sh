#!/usr/bin/env bash

RELEASE_MODE='prod'
FEATURE_GRPC_HOST='magi-0.stylelens.io'
FEATURE_GRPC_PORT=30051

sudo nvidia-docker run -dit --restart unless-stopped \
    -e RELEASE_MODE=$RELEASE_MODE \
    -e FEATURE_GRPC_PORT=$FEATURE_GRPC_PORT \
    bluelens/bl-index:$RELEASE_MODE
