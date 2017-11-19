#!/usr/bin/env bash

sudo nvidia-docker run -dit --restart unless-stopped -p 50052:50052 bluelens/bl-detect:latest