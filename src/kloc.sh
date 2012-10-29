#!/usr/bin/env bash
find . -type f -name '*.[ch]' -exec cat '{}' \; | wc -l
