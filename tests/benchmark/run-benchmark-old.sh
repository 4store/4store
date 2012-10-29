#!/usr/bin/env bash

cat benchmark.rq  benchmark.rq benchmark.rq benchmark.rq benchmark.rq benchmark.rq benchmark.rq benchmark.rq benchmark.rq | time ../../frontend/4s-query $1 -P > /dev/null
