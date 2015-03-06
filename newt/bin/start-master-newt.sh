export NEWT_HOME=/Users/kshitij/GitRepo/spark-lineage/newt

#cd $NEWT_HOME
#./bin/start-local-newt.sh $@

for slave in `cat slaves |sed  "s/#.*$//;/^$/d"`; do
    #ssh $slave $NEWT_HOME/bin/start-local-newt.sh $@;
   echo $slave
done
