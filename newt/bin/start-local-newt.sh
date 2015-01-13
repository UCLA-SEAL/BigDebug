export NEWT_HOME=/home/zhy001/test_gl_jni/newt
export CLASSPATH=/home/zhy001/test_gl_jni/newt/newt.jar
cd $NEWT_HOME

echo "hostname = "$HOSTNAME


canStart=0
#if [ -f /tmp/Newt/newt.pid ]; then
if [ -f /lineage/Newt/newt.pid ]; then
    #lastPid=`cat /tmp/Newt/newt.pid`
    lastPid=`cat /lineage/Newt/newt.pid`   
 
    if kill -0 $lastPid > /dev/null 2>&1; then
        kill -9 $lastPid
        canStart=$?
    fi
fi

if [ $canStart -eq 0 ]; then
    CLASSPATH=$CLASSPATH
    for file in `ls lib/*.jar`; do
        CLASSPATH=$CLASSPATH:$NEWT_HOME/$file
    done

    dateString=`date|sed "s/ /_/g"|sed "s/_[_]*/_/g"`
    java -Xmx1534m -cp $CLASSPATH newt.server.NewtServer $@ < /dev/null > log/$HOSTNAME-newt-$dateString.log 2>&1 &
    pid=$!

    #if [ ! -d /tmp/Newt ]; then
    if [ ! -d /lineage/Newt ]; then
        #mkdir /tmp/Newt
        mkdir /lineage/Newt
        if [ $? != 0 ]; then
            echo "Unable to create Newt tmp directoty in /tmp/Newt. Aborting..."
            exit 255
        fi
    fi
    #echo $pid > /tmp/Newt/newt.pid
    echo $pid > /lineage/Newt/newt.pid
    #echo "Let me check `cat /tmp/Newt/newt.pid`"
    #echo "check done"
 
    sleep 5
    #echo "Let me check AGAIN `cat /tmp/Newt/newt.pid`"
    #if ! kill -0 `cat /tmp/Newt/newt.pid` > /dev/null 2>&1; then
    if kill -0 `cat /lineage/Newt/newt.pid` > /dev/null 2>&1; then
        echo "Sucessfully started Newt on $HOSTNAME. Startup log: "
        echo "============================================================================="
        cat log/$HOSTNAME-newt-$dateString.log
        echo "============================================================================="
        echo ""
        exit 0
    else
        echo "Unable to start newt on $HOSTNAME. Aborting..."
        exit 255
    fi
fi
