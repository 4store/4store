#!/bin/sh

if test -f "$1" ; then
	cat $1 | tr -d '\n'
else
	(git describe --tags --always 2>/dev/null || git describe --tags 2>/dev/null) | tr -d '\n'
fi
