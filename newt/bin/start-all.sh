#!/bin/bash

for mst in `cat master |sed  "s/#.*$//;/^$/d"`; do
    echo "Starting Master on $mst"
    ssh $mst start-master.sh;
done

for slave in `cat slaves |sed  "s/#.*$//;/^$/d"`; do
    echo "Starting Peer on $slave"
    ssh $slave start-peer.sh;
done
