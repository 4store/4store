#!/usr/bin/env bash

../utilities/4s-backend-setup --node 0 --cluster 1 --segments 1 dawg_test
../backend/4s-backend -D dawg_test &
pid=$!
sleep 2
if [ "x$*" = "x" ]; then
	./runtest.pl data/**/manifest.* > results.html
else
	./runtest.pl $* > results.html
fi
kill $pid
