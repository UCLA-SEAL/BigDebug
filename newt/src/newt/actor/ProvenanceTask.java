package newt.actor;

import newt.contract.NewtService;

public class ProvenanceTask {
    ProvenanceContainer    provenance = null;
    int                    actorID = -1;
    int                    subActorID = -1; // <-- New
    int                    schemaID = -1;
    String                 tableName = null;
    NewtService            peerClient = null;
    int                    requestID = -1;
    boolean                commit = false;
    static int             maxRetries = 5;
    static int             retryInterval = 10;
    boolean                committed = false;

    public ProvenanceTask( ProvenanceContainer provenance, String tableName, boolean commit )
    {
        this.provenance = provenance;
        this.tableName = tableName;
        this.commit = commit;
    }

    public ProvenanceTask( int requestID, ProvenanceContainer provenance, int actorID, int subActorID, int schemaID, String tableName,  NewtService  peerClient, boolean commit )
    {
        this.requestID = requestID;
        this.provenance = provenance;
        this.actorID = actorID;
        this.schemaID = schemaID;
        this.tableName = tableName;
        this.peerClient = peerClient;
        this.commit = commit;
        this.subActorID = subActorID;
    }

    public ProvenanceContainer getProvenance()
    {
        return provenance;
    }

    public int getActorID()
    {
        return actorID;
    }
    
    public int getSubActorID()
    {
        return subActorID;
    }

    public int getSchemaID()
    {
        return schemaID;
    }

    public String getTableName()
    {
        return tableName;
    }
    
    public long execute()
    {
        long timeTaken = 0;
        if( provenance.size() != 0 ) {
            int retries = 0;
            Integer result = -1;
            do {
                try {
                    long start = System.currentTimeMillis();
//DEN
                    result = (Integer) peerClient.addProvenance(tableName, schemaID, provenance );
//		    result = 0;
                    timeTaken = System.currentTimeMillis() - start;
                } catch( Throwable t ) {
                    System.err.println( "NewtProvenance: " + t.getMessage() );
                    t.printStackTrace();

                    // Retry logic: retry as many times as permitted; 
                    retries++;
                    if( retries <= maxRetries ) {
                        try {
                            System.err.println( "NewtProvenance: Actor " + actorID 
                                + ", thread " + Thread.currentThread().getName() + ": retry " + retries + "..." );
                            Thread.currentThread().sleep( retryInterval );
                        } catch( Exception e ) {
                        }
                    } else {
                        break;
                    }
                }
            } while( result != 0 );

//           System.out.println( "Request " + requestID + " for actorID " + actorID + " took " + timeTaken + " ms" );
        }

        if( commit ) {
            synchronized( this ) {
                setCommitted();
                this.notifyAll();
            }
        }

        return timeTaken;
    }

    public boolean isCommitTask()
    {
        return commit;
    }

    public void setCommitted()
    {
        committed = true;
    }
    
    public boolean committed()
    {
        return committed;
    }
}
