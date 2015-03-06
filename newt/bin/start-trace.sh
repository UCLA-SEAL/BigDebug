cd $NEWT_HOME
CLASSPATH=$CLASSPATH
for file in `ls lib/*.jar`; do
    CLASSPATH=$CLASSPATH:$NEWT_HOME/$file
done
echo "java -cp $CLASSPATH newt.test.NewtTest"
java -cp $CLASSPATH newt.trace.NewtTraceClient $NEWT_HOME/src/newt/trace/ReplayConf.xml $NEWT_HOME/src/newt/trace/ReplayData
