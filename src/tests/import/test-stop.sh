#!/bin/sh

if [ "$CLUSTER" == "yes" ] ; then
  4s-cluster-stop "$1" >/dev/null
else
  if [ -x /usr/bin/pkill ] ; then
    pkill -f "^(valgrind )?../../backend/4s-backend\ $1\$"
  else
    killall 4s-backend
  fi;
fi;
