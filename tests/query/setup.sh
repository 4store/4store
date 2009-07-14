#!/bin/bash

kb="query_test_$USER"
../../src/utilities/4s-backend-setup --node 0 --cluster 1 --segments 8 $kb
FS_DISK_LIMIT=2.0 ../../src/backend/4s-backend $kb || exit
../../src/frontend/4s-import -v $kb -m http://example.com/swh.xrdf ../../data/swh.xrdf -m http://example.com/TGR06001.nt ../../data/tiger/TGR06001.nt -m http://example.com/nasty.ttl ../../data/nasty.ttl
../../src/frontend/4s-delete-model $kb http://example.com/nasty.ttl

if [ "$1" == "--autorun" ] ; then
	./run.pl --nospawn
fi
if [ -x /usr/bin/pkill ] ; then
	pkill -f "^../../src/backend/4s-backend\ $kb\$"
else
	killall 4s-backend
fi
