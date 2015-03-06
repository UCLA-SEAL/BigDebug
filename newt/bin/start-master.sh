BASE="/home/clash/sparks/spark-lineage-newt"
TMP="/tmp"
#BASE="/Users/kshitij/GitRepo/spark-lineage"
export NEWT_HOME="$BASE/newt"

if [ -f $TMP/newts.pid ]; then
    #lastPid=`cat /tmp/Newt/newt.pid`
    lastPid=`cat $TMP/newts.pid`

    if kill -0 $lastPid > /dev/null 2>&1; then
        kill -9 $lastPid &> /dev/null
        rm $TMP/newts.pid
    fi
fi

java -cp $NEWT_HOME/newt.jar:$NEWT_HOME/lib/* newt.server.NewtServer clean master &> $TMP/master.log &
pid=$!
echo $pid
echo $pid > $TMP/newts.pid