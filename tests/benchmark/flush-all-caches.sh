#!/usr/bin/env bash

# This script will flush the disk caches on backend nodes
#
# It must be run as root, and there must be no 4s-backend
# processes running. It's just mean for perfroamnce testing
# not recommended for any other use.

echo Before
ssh-all free | egrep '(Mem:|total)'
ssh-all 'umount /raid && mount /raid'
echo After
ssh-all free | egrep '(Mem:|total)'
