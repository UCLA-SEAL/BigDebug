cd $NEWT_HOME
CLASSPATH=$CLASSPATH
for file in `ls lib/*.jar`; do
    CLASSPATH=$CLASSPATH:$NEWT_HOME/$file
done
echo "java -cp $CLASSPATH newt.test.NewtTest"
java -cp $CLASSPATH newt.test.NewtTest 1 1000
