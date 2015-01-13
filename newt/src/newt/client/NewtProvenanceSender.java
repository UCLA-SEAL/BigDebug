package newt.client;

import java.lang.*;
import java.util.*;
import java.io.*;
import java.security.*;

import java.net.MalformedURLException;
import java.net.URL;

import newt.contract.*;
import newt.common.*;
import newt.actor.*;

public class NewtProvenanceSender extends Thread {
    NewtService             masterClient = null;
    ProvenanceTask          provenanceTask = null;
    ArrayList               provenance = null;
    int                     requests = 0;
    double                  totalTimeTaken = 0;

    public NewtProvenanceSender( NewtService masterClient )
    {
        provenance = new ArrayList();
        this.masterClient = masterClient;
        start();
    }

    public ArrayList getProvenanceBuffer()
    {
        return provenance;
    }

    public void waitForProvenance()
    {
        synchronized( provenance ) {
            while( provenance.size() == 0 ) {
                try {
                    provenance.wait();
                } catch( Exception e ) {
                }
            }

            provenanceTask = (ProvenanceTask) provenance.get( 0 );
            provenance.remove( provenanceTask);
        }
    }

    public void run()
    {
        while( true ) {
            waitForProvenance();
            totalTimeTaken += provenanceTask.execute();
            requests++;

            if( provenanceTask.isCommitTask() ) {
               //System.out.println( "Average time taken to send provenance = " + (totalTimeTaken / requests) + " ms, for " + requests + " requests" );
                break;
            }
        }

    }
}
