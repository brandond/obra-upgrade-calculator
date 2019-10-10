#!/bin/bash
set -euo pipefail

TOPLEVEL=`git rev-parse --show-toplevel`
REPONAME=`basename ${TOPLEVEL}`

docker build --force-rm -t ${USER}/${REPONAME}:latest ${TOPLEVEL}
