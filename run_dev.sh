#!/usr/bin/env bash

#sudo nvidia-docker run -dit --restart unless-stopped -p 50053:50053 bluelens/bl-index:dev

RELEASE_MODE='dev'
FEATURE_GRPC_HOST='magi-0.stylelens.io'
FEATURE_GRPC_PORT=50051

sudo nvidia-docker run -dit --restart unless-stopped \
    -e RELEASE_MODE=$RELEASE_MODE \
    -e FEATURE_GRPC_PORT=$FEATURE_GRPC_PORT \
    bluelens/bl-index:$RELEASE_MODE
