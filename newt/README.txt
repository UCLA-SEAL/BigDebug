The classes of importance are NewtClient, NewtHandler and NewtState. In order to run Newt, you need MySQL. Moreover, if you are running locally (which I suggest for the beginning) you need to provide a different port to the master and peer. 

1) Install MySQL
2) Set environment variable NEWT_HOME to point to the path of the newt source code. 
3) Create a directory called Newt in /tmp
3) Set your IP in NewtConfig.xml Since you are running locally in the beginning, set both Master and Slave to your localhost IP. Use the actual IP and not 127.0.0.1
4) Set log dir in NewtConfig.xml For example: /Users/papavas/tmp/Newt
5) Set your IP in file NewtClient.java for variable DEFAULT_MASTER_IP

You need to run file NewtServer.java once to create a master that is a web server listening to connections from the clients. Moreover, the master communicates with the peers. In order to run the master:
1) Set Configuration.masterPort=8899
2) In file NewtState.java I have some lines in comments. Lines: 134,230,242,249,250 Don't change these lines.

In order to start a peer, you need to run file NewtServer.java again but this time to create a peer. In order to set this flag you need to:
1) Set Configuration.masterPort=8898 <---- Must be different than the master since you are running two web servers on the same machine
2) In file NewtState.java I have some lines in comments. Lines: 134,230,242,249,250 Uncomment the ones that are in comments and change the port in lines 249 and 249 from 8899 to 8898

Now you are ready to run the test file that creates clients. There is a one-to-one mapping between clients and actors/subActors. Basically, in order to create an actor/subActor you need to create a client. Then you can signify the client to capture lineage and call commit when its done to send the captured data items to the peer. 

For every client, there is a designated peer that waits to receive the lineage from the client and stores it in the database.

--- Old Readme

Starting and running newt:

1. Set NEWT_HOME environment variable to this folder.
2. Include newt.jar in classpath.
3. Start mysql on each node in Newt cluster. Update $NEWT_HOME/NewtClient.xml with mysql username and password.
4. Write master node IP in $NEWT_HOME/bin/master. Write slave node IPs in $NEWT_HOME/bin/slaves. IPs can be different from Hadoop cluster.
5. Create $NEWT_HOME/log folder if it doesn't exist. This is where each Newt instance logs stdout and stderr to.
6. On Newt master node, run '$NEWT_HOME/bin/start-master-newt.sh'. This starts Newt on master and all slaves. 

Note:
1. To make a clean start, run '$NEWT_HOME/bin/start-master-newt.sh clean'. This cleans all tables on all nodes.
2. Each Newt node (master and slave) writes a pid into /tmp/newt.pid. Additionally, each Newt peer also writes its peerID into /tmp/newt.peerid. To avoid inconsistencies, do not clear /tmp/ unless making a clean start.


