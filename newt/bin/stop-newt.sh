#!/bin/bash
BASE="/home/clash/sparks/spark-lineage-newt"
TMP="/tmp"
#BASE="/Users/kshitij/GitRepo/spark-lineage"

if [ -f $TMP/newtp.pid ]; then
    lastPid=`cat $TMP/newtp.pid`

    if kill -0 $lastPid > /dev/null 2>&1; then
        kill -9 $lastPid &> /dev/null
        rm $TMP/newtp.pid
    fi
fi


if [ -f $TMP/newts.pid ]; then
    lastPid=`cat $TMP/newts.pid`

    if kill -0 $lastPid > /dev/null 2>&1; then
        kill -9 $lastPid &> /dev/null
        rm $TMP/newts.pid
    fi
fi