#!/bin/sh

if [ "$CLUSTER" == "yes" ] ; then
  4s-cluster-start "$@" >/dev/null
else
  $BACKPRECMD ../../backend/4s-backend "$@"
fi;
