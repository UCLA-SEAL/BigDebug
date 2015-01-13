/*
 * Created on Apr 30, 2011
 *
 */
package newt.client.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class to query the Newt metadata table for locations
 * of actor tables.
 *
 */
public class MetadataQuery {
    
    private SQLConnection metaDataServer;
    
    public MetadataQuery(SQLConnection metaDataServer) {
	this.metaDataServer = metaDataServer;
    }
    
    /**
     * Given the actor type, it returns a set of actor entries 
     */
    public ArrayList<ActorEntry> getActors(String actorType) throws SQLException {
	ArrayList<ActorEntry> list = new ArrayList<ActorEntry>();
	String query = "select * from actorInstances WHERE ActorLType='"+actorType+"'";
	ResultSet rs = metaDataServer.executeQuery(query);
	while (rs.next()) {
	    list.add(new ActorEntry(
		    Integer.parseInt(rs.getString(ActorEntry.COL_ACTOR_ID)),
		    rs.getString(ActorEntry.COL_ACTOR_NAME),
		    Integer.parseInt(rs.getString(ActorEntry.COL_PARENT_ID)),
		    rs.getString(ActorEntry.COL_ACTOR_LTYPE),
		    rs.getString(ActorEntry.COL_ACTOR_TABLE),
		    rs.getString(ActorEntry.COL_ACTOR_URL),
		    Boolean.parseBoolean(rs.getString(ActorEntry.COL_COMMITTED))));
	}
	return list;
    }
    
    
    /**
     * Represents a row in the Actor Instance Table (actorInstances)
     *
     */
    public class ActorEntry {
	public static final String COL_ACTOR_ID = "ActorID";
	public static final String COL_ACTOR_NAME = "ActorIName";
	public static final String COL_PARENT_ID = "ParentID";
	public static final String COL_ACTOR_LTYPE = "ActorLType";
	public static final String COL_ACTOR_TABLE = "ActorTable";
	public static final String COL_ACTOR_URL = "ActorUrl";
	public static final String COL_COMMITTED = "Committed";
	
	private int actorID;
	private String actorName;
	private int parentID;
	private String actorLType;
	private String actorTable;
	private String actorURL;
	private boolean committed;
	
	public ActorEntry(int actorID, String actorName, int parentID,
		String actorLType, String actorTable, String actorURL, boolean committed) {
	    this.actorID = actorID;
	    this.actorName = actorName;
	    this.parentID = parentID;
	    this.actorLType = actorLType;
	    this.actorTable = actorTable;
	    this.actorURL = actorURL;
	    this.committed = committed;
	}
	
	// Locations are of the type: http://<hostname>/xmlrpc
	public String getTableLocationIP() {
	    String pattern = "^(?:[^/]+://)?([^/:]+)";
	    Matcher matcher = Pattern.compile(pattern).matcher(actorURL);
	    if (matcher.find()) {
		int start = matcher.start(1);
		int end = matcher.end(1);
		return actorURL.substring(start,end);
	    }
	    return null;
	}
	
	public String toString() {
	    return COL_ACTOR_ID+":"+actorID+", "+
	    COL_ACTOR_NAME+":"+actorName+", "+
	    COL_PARENT_ID+":"+parentID+", "+
	    COL_ACTOR_LTYPE+":"+actorLType+", "+
	    COL_ACTOR_TABLE+":"+actorTable+", "+
	    COL_ACTOR_URL+":"+actorURL+", "+
	    COL_COMMITTED+":"+committed;
	}
    }
}
