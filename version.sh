#!/bin/sh

git describe --tags --always | tr -d '\n'
