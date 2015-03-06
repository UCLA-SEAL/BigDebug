package newt.contract;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Vector;

import newt.actor.ProvenanceContainer;

public interface NewtService {
//    public int getPeerID( String slaveUrl );
//    public int reportSlave( String slaveUrl, int peerID );
    public int getUniverse(String uname);
    public int register(String aname, int parentID);
    
    /**
     * Returns the RPC address of the node that maintains the specified table
     * for the specified actor. If the table hasn't been created yet, a new
     * one is created and a node is assigned and returned.
     * 
     * The decision about where to create the table might be locality-aware, so
     * we pass an IP as well. This will typically be the IP of machine where
     * the calling actor is running.
     * 
     * XXX The assignment of tables to machines, and the querying for their
     * XXX location should be separated.     
     */
//    public String getProvenanceNode( int actorID, String tableID, int schemaID, String peerIP , boolean isSubActor);
    
    public String getProvenanceNode(int actorID, String tableID, int schemaID, byte[] peerIP, boolean isSubActor);
    
    public int addFileLocatable(int actorID, String locatable, boolean isInput, boolean isGhostable);
    /**
     * The table name uniquely identifies an actor or subActor. 
     * I think we don't need to give as argument the actorID and subActorID.
     * @param tableID
     * @param schemaID
     * @param pContainer
     * @return
     */
    public int addProvenance(String tableID, int schemaID, ProvenanceContainer pContainer);
    public int getID(String aname, String atype);
    public int getActorID();
    public int addActor(int parentID, String aname, String atype, String relativeID);
    public int addSubActor(int parentID, String aname, String atype, String relativeID);
    public int addSourceOrDestinationActor(int aid, int otherAid, boolean isSource);
    public int addSourceBySubactor(int aid, int otherAid);
    public int setProvenanceHierarchy(int actorID, String hierarchy);
    public int commit(int actorID, boolean isSubActor);
    public int loadData(String table, boolean isSubActor);
    public int cleanFiles(Vector<String> files);
    public int finalizeCommit(String tableName, boolean isSubActor);
    public int sendGhostData(String ghostSourceTable, String ghostDestinationUrl, String ghostDestinationTable);
    public int ghost(String ghostSourceTable, String ghostDestinationTable, Vector<String> ghostData);
    public int ghostComplete(String sourceTable, String destinationTable, String ghostTable);
    public int setProvenanceSchemas(int actorID, String hierarchy);
    public int getSchemaID(int uID, String schemaName);
    
    public List<Integer> isParentOfSubActors(String table);
    public Integer getParentOfSubActorByID(String actorID);
    public Integer getParentOfSubActorByTable(String table);
    public Integer getActorIdOfParentOfSubActor(String table);
    public ResultSet getAllActors(String type);
    public void loadDataIntoActor(Integer actorID, String actorTable);
    
    /**
     * 
     * @param update
     * @param filename
     * @param isSubActor
     * @return
     */
    public int createProvTable(String update, String filename, boolean isSubActor);
    
    public int trace(Vector data, String direction, int caid, int aid, String atype);
    public int traceLocal(int tid,
                          HashMap<String, HashMap<String, Vector<Vector>>> traceQuery,
                          HashMap<String, Vector<String>> tableSources,
                          Vector data,
                          Vector locatableData);
    public int continueTrace(int tid, String resultTable, String resultDataType, Vector results);
    public int traceComplete(int tid, String slave);
    public Object[] getTraceNode(int tid, int rpid, String atype, String rid);
    public Object[] getTraceNodeByActorID(int tid, String aname);
    public HashSet getReplayFilter(int tid, String tableName);
}
