#!/usr/bin/env bash

sudo nvidia-docker run -dit --restart unless-stopped -p 30053:30053 bluelens/bl-index:dev