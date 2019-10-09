#!/bin/sh

source /app/venv/bin/activate
exec /app/venv/bin/obra-upgrade-calculator $@
