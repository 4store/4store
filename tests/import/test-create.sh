#!/bin/sh

if [ "$CLUSTER" == "yes" ] ; then
  4s-cluster-create "$@" >/dev/null
else
  ../../src/utilities/4s-backend-setup --node 0 --cluster 1 "$@"
fi;
