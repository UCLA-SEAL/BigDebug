#!/bin/bash
MASTER="scai01.cs.ucla.edu"
SLAVES="scai02.cs.ucla.edu scai03.cs.ucla.edu scai04.cs.ucla.edu scai05.cs.ucla.edu scai06.cs.ucla.edu scai07.cs.ucla.edu scai08.cs.ucla.edu scai09.cs.ucla.edu scai10.cs.ucla.edu scai11.cs.ucla.edu scai12.cs.ucla.edu scai13.cs.ucla.edu scai14.cs.ucla.edu scai15.cs.ucla.edu scai16.cs.ucla.edu"
BASE="/home/clash/sparks/spark-lineage-newt"
TMP="/tmp"

for mst in $MASTER; do #`cat master |sed  "s/#.*$//;/^$/d"`; do
    echo "Cleaning $TMP on $mst"
    ssh clash@$mst "rm -rf $TMP/*jvm* && rm -rf $TMP/*executor* && rm -rf $TMP/Newt && mkdir $TMP/Newt && chmod 777 $TMP/Newt";
    echo $?
done

for slave in $SLAVES; do #`cat slaves |sed  "s/#.*$//;/^$/d"`; do
    echo "Cleaning $TMP on $slave"
    ssh clash@$slave "rm -rf $TMP/*jvm* && rm -rf $TMP/*executor* && rm -rf $TMP/Newt && mkdir $TMP/Newt && chmod 777 $TMP/Newt" ;
    echo $?
done
