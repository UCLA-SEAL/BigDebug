package newt.client;

import java.io.*;
import java.util.*;

import org.apache.xmlrpc.client.XmlRpcClient;

import newt.common.*;
import newt.contract.*;
import newt.actor.*;

public class NewtLogger {
    String              logRoot = Configuration.clientLogDir;  /* no reason to persist these */
    ObjectOutputStream  log = null;
    int                 actorID = -1;
    int                 schemaID = -1;
    String              tableID = null;
    String              filename = null;
    int                 maxProv = 5000;

    public NewtLogger( int actorID, String tableID, int schemaID )
    throws Exception
    {
        this.actorID = actorID;
        this.tableID = tableID;
        this.schemaID = schemaID;
        
	    File f;
	    f = new File( logRoot );
	    if( !f.exists() ) {
            f.mkdirs();  /* make sure temp log dir exists */
        }
        
        filename = logRoot + "/" + actorID + "_" + tableID + "_" + schemaID + ".log";
        f = new File( filename );
        if( f.exists() ) {
            f.delete();
        }

        log = new ObjectOutputStream( new FileOutputStream( filename ) );
    }

    public void log( ProvenanceContainer provenanceContainer )
    {
        try {
            for( int i = 0; i < provenanceContainer.size(); i++ ) {
                synchronized( log ) {
                    log.writeObject( provenanceContainer.get( i ) );
                }
            }
            log.flush();
        } catch( Exception e ) {
            System.err.println( "Cannot log provenance: " + e.getMessage() );
            return;
        }

        System.err.println( "Logged provenance: " + actorID );
    }

/*
    public void makeAttempt( NewtService client )
    {
        ProvenanceContainer provenanceContainer = new ProvenanceContainer( maxProv );

        try {
            log.flush();
            log.close();

            int index = 0;
            ObjectInputStream o = new ObjectInputStream( new FileInputStream( filename ) );
            while( o.available() > 0 ) {
                while( index < maxProv && o.available() > 0 ) {
                    provenanceContainer.add( (ProvenanceAssociation)o.readObject() );
                    index++;
                }
                Integer result = (Integer) client.addProvenance( actorID, tableID, schemaID, provenanceContainer );
                index = 0;
            }

            o.close();
        } catch( Exception e ) {
            System.err.println( e.getMessage() );
        }
    }
*/
}
