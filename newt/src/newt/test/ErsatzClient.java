package newt.test;

import java.util.*;
import java.io.*;
import java.lang.*;

import jargs.gnu.CmdLineParser;

import newt.client.*;
import newt.client.NewtClient.Mode;


/** 
 * Create a fake client that uses the NewtClient API to generate provenance records.
 * Test generation rates, queries, and replay, without any Hadoop code. 
 *
 * Operates in multiple phases.  
 * 1.) Create provenance records with 1 .. N actors.  Concurrently. 
 *     We could also just use multiple ErsatzClients.  
 * 2.) Make queries to the system. 
 * 3.) Attempt to use replay.
 *
 *  Much of this code was stolen from the code in Hadoop which
 *  creates universes and uses the provenance API. 
 */
public class ErsatzClient {
    
    int actorCount = 1; /* default to one actor */
    int queryCount = 0; /* */
    Vector<ErsatzActor>  actors;     
    
    /* Create actors, initialize, start threads 
     * Each is given their own NewtClients at the moment.  
     */	
    public ErsatzClient(String args[]){
	int i;
	System.out.println("Starting ErsatzClient with "+actorCount+" actors.");
	parseCmdLineOptions(args);		
	actors = new Vector<ErsatzActor>(actorCount);       

	for (i=0;i<actorCount;i++){
	    ErsatzActor ea = new ErsatzActor(i); 
	    ea.init();
	    actors.add(ea);
	}
    }

    public void start(){
	ErsatzActor ea = null;
	Iterator it = actors.iterator();
	while ( (ea = (ErsatzActor)it.next()) != null){
	    new Thread(ea).start();
	}	
    }

    public void stop(){
	ErsatzActor ea = null;
	Iterator it = actors.iterator();
	while ( (ea = (ErsatzActor)it.next()) != null){
	    ea.stop();
	}	
    }

    /* inner class ErsatzActor
     * Has its own thread and NewtClient (for now).  
     */
    private class ErsatzActor implements Runnable {
	int id; 
	int rps = 0; /* records per second */
	int running = 1;

	NewtClient newtClient = null;
	int universeID;
	int actorID;
	org.w3c.dom.Node actorRoot;
	org.w3c.dom.Node tableRoot;
	
	public ErsatzActor(int id) {this.id=id;}

	public void init(){
	    newtClient = new NewtClient( Mode.CAPTURE );

	    //newtClient.clientTest();

	   System.out.println("crap");



	    universeID = newtClient.getUniverseID("TEST");
	    actorID = newtClient.register("test:"+id, universeID);
	}

	public void stop(){
	    running = 0;
	}
       
	public void run(){
	    while(running!=0){		
		Thread.yield();
	    }
	   System.out.println("ErsatzActor["+id+"] finishing.");
	}
    }

    /**
     * Parses command line options
     */
    void parseCmdLineOptions(String args[]){
	CmdLineParser parser = new CmdLineParser();
	CmdLineParser.Option start = parser.addBooleanOption('s', "start");
	CmdLineParser.Option stop = parser.addBooleanOption('k', "kill");
	//	CmdLineParser.Option edlfile = parser.addStringOption('f', "edlfile");
	CmdLineParser.Option numActors = parser.addIntegerOption("n");
       
	try{
	    parser.parse(args);
	} catch(CmdLineParser.OptionException e){
	    e.printStackTrace();
	    System.err.println(e.getMessage());
	    printUsage();
	    System.exit(2);
	}
	if (parser.getOptionValue(numActors) != null) 
	    actorCount = ((Integer)(parser.getOptionValue(numActors))).intValue();
	if ((Boolean) parser.getOptionValue(start) != null) {}
	if ((Boolean) parser.getOptionValue(stop) != null) {}	
    }


    public static void main( String[] args ){
	ErsatzClient eClient; 
	BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        try {
	    eClient = new ErsatzClient(args);
	   System.out.println("Press return to start provenance generation.");
	    br.readLine();
	    eClient.start();
	   System.out.println("Press return to stop test.");
	    br.readLine();
	    eClient.stop();
        } catch ( Exception e ) {
            e.printStackTrace();
        }
    }


    private void printUsage(){
	String use0 = new String("Usage: java ErsatzClient");
	System.err.println(use0);
    }



}
