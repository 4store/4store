#!/bin/sh

(git describe --tags --always 2>/dev/null || git describe --tags) | tr -d '\n'
