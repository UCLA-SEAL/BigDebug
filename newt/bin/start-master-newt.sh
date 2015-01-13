export NEWT_HOME=/home/zhy001/test_gl_jni/newt 

cd $NEWT_HOME
./bin/start-local-newt.sh $@

for slave in `cat ./bin/slaves|sed  "s/#.*$//;/^$/d"`; do
    ssh $slave $NEWT_HOME/bin/start-local-newt.sh $@;
done
