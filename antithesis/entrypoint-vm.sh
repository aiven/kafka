#!/bin/bash

sudo service ssh start

set -Eeuo pipefail

exec "$@"
