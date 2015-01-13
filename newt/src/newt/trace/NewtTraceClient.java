package newt.trace;

import java.lang.*;
import java.io.*;
import java.util.*;

import org.w3c.dom.*;
import newt.client.*;
import newt.actor.*;
import newt.common.*;

public class NewtTraceClient {
    public static void main( String args[] ) {
        if( args.length < 2 ) {
           System.out.println( "Enter config path and data path" );
            return;
        }

        String configPath = args[ 0 ];
        String dataFilePath = args[ 1 ];
        int replayTraceID = -1;
            
        try {
            if( configPath == null ) {
                throw new Exception( "Replay configuration not found." );
            }
                
            NewtXmlDOM dom = new NewtXmlDOM();
            Document doc = dom.doc( configPath );
            Node root = dom.root( doc );
            if( !root.getNodeName().equals( "Replay" ) ) {
                throw new Exception( "Invalid replay config file format: " + root.getNodeName() );
            }
                    
            ArrayList<Node> children = dom.children( root );
            String replayActor = null;
            String replayActorType = null;
            int traceActorID = -1;
            String traceActorType = null;
            for( Node n: children ) {
                if( n.getNodeName().equals( "Actor" ) ) {
                    replayActor = n.getFirstChild().getNodeValue();
                } else if( n.getNodeName().equals( "ActorType" ) ) {
                    replayActorType = n.getFirstChild().getNodeValue();
                } else if( n.getNodeName().equals( "TraceActorID" ) ) {
                    traceActorID = (n.getFirstChild() == null || n.getFirstChild().getNodeValue() == null) 
                                    ? -1 : Integer.parseInt( n.getFirstChild().getNodeValue() );
                } else if( n.getNodeName().equals( "TraceActorType" ) ) {
                    traceActorType = (n.getFirstChild() == null) ? null : n.getFirstChild().getNodeValue();
                } else if( n.getNodeName().equals( "TraceID" ) ) {
                    replayTraceID = (n.getFirstChild() == null || n.getFirstChild().getNodeValue() == null)
                               ? -1 : Integer.parseInt( n.getFirstChild().getNodeValue() );
                }
            }

            if( ( replayActor == null || dataFilePath == null || ( traceActorType == null && traceActorID == -1 ) ) && replayTraceID == -1 ) {
                throw new Exception( "Invalid replay config file format: information missing" );
            }

            if( replayTraceID == -1 ) {
                FileReader dataFile = new FileReader( dataFilePath );
                BufferedReader reader = new BufferedReader( dataFile );
                String line = reader.readLine();
                Vector<String> data = new Vector<String>();
                while( line != null ) {
                    if( !line.startsWith( "#" ) ) {
                        data.add( line );
                    }
                    line = reader.readLine();
                }
           
                int containingActorID = NewtClient.getActorID( replayActor, replayActorType );
                if( containingActorID == -1 ) {
                    throw new Exception( "Replay actor " + replayActor + " not found. Aborting replay..." );
                }
                replayTraceID = NewtClient.trace( data, "backward", containingActorID, traceActorID, traceActorType );
                if( replayTraceID == -1 ) {
                    throw new Exception( "NewtProvenance: Error: Trace request failed. Aborting replay..." );
                }

                if( replayTraceID == -1 ) {
                    throw new Exception( "Invalid replay trace ID. Aborting replay..." );
                }
            }
           System.out.println( "Trace completed." );

            while( true ) {
               System.out.println( "Enter actor name to get trace results (e.g. attempt_201202281606_0002_m_000000_0_rr): " );
                BufferedReader br = new BufferedReader( new InputStreamReader( System.in ) );
                String actorName = br.readLine();
                NewtClient.printTraceResults( replayTraceID, actorName );
            }
        } catch (Exception e) {
           System.out.println( e.getMessage() );
            e.printStackTrace();
        }

    }
}
