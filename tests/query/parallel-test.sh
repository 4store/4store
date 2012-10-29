#!/usr/bin/env bash

logfile="/tmp/pq-$$.log"

for (( its=1 ; its < 20 ; its++ )); do
   for (( i=0 ; i < $its ; i++ )) ; do
      ./parallel-query.pl 100 $i >> $logfile &
   done
   wait
   echo -n $its " "
   awk '{ t+=$1 } END { print t }' < $logfile
   rm $logfile
done
