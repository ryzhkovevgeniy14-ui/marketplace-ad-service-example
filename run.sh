#!/bin/bash

uv run python -m bin.outbox &
uv run python -m bin.api &

wait