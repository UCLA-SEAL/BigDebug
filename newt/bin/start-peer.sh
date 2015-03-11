BASE="/home/clash/sparks/spark-lineage-newt"
TMP="/tmp"
#BASE="/Users/kshitij/GitRepo/spark-lineage"
export NEWT_HOME="$BASE/newt"

if [ -f $TMP/newtp.pid ]; then
    lastPid=`cat $TMP/newtp.pid`

    if kill -0 $lastPid > /dev/null 2>&1; then
        kill -9 $lastPid &> /dev/null
        rm $TMP/newtp.pid
    fi
fi

java -cp $NEWT_HOME/newt.jar:$NEWT_HOME/lib/* newt.server.NewtServer clean peer &> $TMP/peer.log &
pid=$!
echo $pid
echo $pid > $TMP/newtp.pid