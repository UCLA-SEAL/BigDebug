package newt.server;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import newt.actor.ProvenanceContainer;
import newt.common.Configuration;
import newt.common.RpcCommunication;
import newt.contract.NewtService;
import newt.server.sql.NewtSelectStatementBuilder;
import newt.server.sql.NewtUpdateStatementBuilder;
import newt.utilities.Utilities;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import com.caucho.hessian.server.HessianServlet;

public class NewtHandler extends HessianServlet implements NewtService {
	
    static NewtState    newtState = null;
    InetAddress         addr = null;
    String              ipAddr = null;
    String              hostname = null;

    public NewtHandler()
    {
        newtState = NewtState.getInstance();

        try {
            addr = InetAddress.getLocalHost();
        } catch( UnknownHostException uhe ) {
           System.out.println( "UnknownHostException: " + uhe.getMessage() );
        }

        /* Get local IP address. */
        ipAddr = addr.getHostAddress();

        /* Get local hostname. */
        hostname = addr.getCanonicalHostName();
    }

    public int getUniverse( String uname )
    {
        int uid = -1;

        if(!Configuration.doServerCalls)
        {
            return uid;
        }
        
        HashMap<String, Object[]> queryVars = new HashMap<String, Object[]>();
        queryVars.put( "ActorName", new Object[] { uname } );
        queryVars.put( "ActorType", new Object[] { "Universe" } );
        NewtMySql   newtMysql = newtState.queryActorInstance( queryVars, true );
        ResultSet   rs = newtMysql.getResultSet();

        try {
            if( !rs.next() ) {
                uid = newtState.getNextActorID();
                newtState.addActorInstance( uid, uname, -1, "Universe", "", "", "", "", -1, -1 );
                newtState.ActorNamesToID.put( uname, uid );
                newtState.ActorIDToNames.put( uid, uname );
                newtState.ActorsToParent.put( uid, null );
                newtState.ActorTypes.put( uid, "Universe" );
            } else {
                uid = rs.getInt( 1 );
            }
        } catch( Exception e ) {
            e.printStackTrace();
        }

        newtMysql.close();
       //System.out.println( "Returning uid: " + uid + " for universe: " + uname );
        return uid;
    }

    public int register( String aname, int parentID )
    {
        int aid = -1;

        if(!Configuration.doServerCalls)
        {
            return aid;
        }
        
        HashMap<String, Object[]> queryVars = new HashMap<String, Object[]>();
        queryVars.put( "ActorName", new Object[] { aname } );
        queryVars.put( "ActorType", new Object[] { "RootActor" } );
        queryVars.put( "ParentID", new Object[] { (new Integer( parentID )).toString() } );
        NewtMySql   newtMysql = newtState.queryActorInstance( queryVars, true );
        ResultSet   rs = newtMysql.getResultSet();

        try {
            if( !rs.next() ) {
                aid = newtState.getNextActorID();
                newtState.addActorInstance( aid, aname, parentID, "RootActor", "", "", "", "", -1, -1 );
                newtState.ActorNamesToID.put( aname, aid );
                newtState.ActorIDToNames.put( aid, aname );
                newtState.ActorsToParent.put( aid, parentID );
                newtState.ActorTypes.put( aid, "RootActor" );
            } else {
                aid = rs.getInt( 1 );
            }
        } catch( Exception e ) {
            e.printStackTrace();
        }
        
        newtMysql.close();
       //System.out.println( "Returning root aid: " + aid + " for root actor: " + aname + " with parent: " + parentID );
        return aid;
    }

    public synchronized String getProvenanceNode( int actorID, String tableID, int schemaID, byte[] peerIP, boolean isSubActor )
    {
        String  aUrl = null;
        
        if(!Configuration.doServerCalls)
        {
            return aUrl;
        }
        
        HashMap<String, Object[]> queryVars = new HashMap<String, Object[]>();
        queryVars.put( "ActorID", new Object[] { (new Integer( actorID )).toString() } );
        NewtMySql newtMysql = null;
        
        try 
        {
        	if(isSubActor)
        	{
        		newtMysql = newtState.querySubActorInstance( queryVars, true );
        	}
        	else
        	{
        		newtMysql = newtState.queryActorInstance( queryVars, true );
        	}
            ResultSet   rs = newtMysql.getResultSet();

            if( !rs.next() ) 
            {
               System.out.println( "Failed to get node for " + actorID );
                return null;
            } 
            else 
            {
                String tname = rs.getString( 5 );
                if( tname.equals( "" ) ) 
                {
                    aUrl = newtState.createProvenanceTable( actorID, tableID, schemaID, peerIP, isSubActor );
                } 
                else 
                { 
                    aUrl = rs.getString( 6 );
                }
            }
        } 
        catch( Exception e ) 
        {
           System.out.println( "Failed to get node for " + actorID );
            e.printStackTrace();
            return null;
        }

        newtMysql.close();
       //System.out.println( "Assigned node: " + aUrl + " for actorID " + actorID );
        return aUrl;
    }

    public synchronized int addFileLocatable( int actorID, String locatable, boolean isInput, boolean isGhostable )
    {
       //System.out.println( "Adding locatable: " + locatable + " for actor: " + actorID );
        newtState.addFileLocatable( actorID, locatable, isInput, isGhostable );
        return actorID;
    }

    /**
     * If subActorID != -1, then it is called from a subActor
     */
    public synchronized int addProvenance( String tableID, int schemaID, ProvenanceContainer provenance )
    {
        if(!Configuration.doServerCalls)
        {
            return 0;
        }

      	newtState.addProvenance(tableID, provenance );
        
        return 0;
    }

    public synchronized int getID( String aname, String atype )
    {
        int aid = -1;
        
        if(!Configuration.doServerCalls)
        {
            return aid;
        }
        
        HashMap<String, Object[]> queryVars = new HashMap<String, Object[]>();
        queryVars.put( "ActorName", new Object[] { aname } );
        queryVars.put( "ActorType", new Object[] { atype } );
        NewtMySql   newtMysql = newtState.queryActorInstance( queryVars, true );
        ResultSet   rs = newtMysql.getResultSet();

        try {
            if( !rs.next() ) {
            } else {
                aid = Integer.parseInt( rs.getString( 1 ) );
            }
        } catch( Exception e ) {
            e.printStackTrace();
        }

        newtMysql.close();
        return aid;
    }

    public synchronized int getActorID()
    {
        int aid = newtState.getNextActorID();
        return aid;
    }

    /*public synchronized String getTraceNode( int aid )
    {
        String url = null;
        
        if(!Configuration.doServerCalls)
        {
            return url;
        }

        HashMap<String, Object[]> queryVars = new HashMap<String, Object[]>();
        queryVars.put( "ActorID", new Object[] { aid } );
        NewtMySql   newtMysql = newtState.queryActorInstance( queryVars, true );
        ResultSet   rs = newtMysql.getResultSet();

        try {
            rs.next();
            url = rs.getString( 6 );
        } catch( Exception e ) {
            e.printStackTrace();
        }

        newtMysql.close();
        return url;
    }*/

    // Master calls
    public synchronized int addActor( int parentID, String aname, String atype, String relativeID )
    {
        int     aid = -1;
       
        if(!Configuration.doServerCalls)
        {
            return aid;
        }

        HashMap<String, Object[]> queryVars = new HashMap<String, Object[]>();
        queryVars.put( "ActorName", new Object[] { aname } );
        queryVars.put( "ActorType", new Object[] { atype } );
        queryVars.put( "ParentID", new Object[] { parentID } );
        NewtMySql   newtMysql = newtState.queryActorInstance( queryVars, true );
        ResultSet   rs = newtMysql.getResultSet();

        try {
            if( !rs.next() ) {
                aid = getActorID();
                newtState.addActorInstance( aid, aname, parentID, atype, "", "", "", relativeID, -1, -1 );
                newtState.ActorNamesToID.put( aname, aid );
                newtState.ActorIDToNames.put( aid, aname );
                newtState.ActorsToParent.put( aid, parentID );
                newtState.ActorTypes.put( aid, atype );
            } else {
               //System.out.println( "Actor exists. Returning prior newtState.ID." );
                aid = rs.getInt( 1 );
            }
        } catch( Exception e ) {
            e.printStackTrace();
        }

        newtMysql.close();
       //System.out.println( "Returning aid: " + aid + " for actor: " + aname + " with parent: " + parentID );
        return aid;
    }
    
    /**
     * Add a new subActor to the table subActorInstances. The subActor belongs to the actor denoted by parentID
     * @param parentID
     * @param aname
     * @param atype
     * @param relativeID
     * @return
     */
    public synchronized int addSubActor( int parentID, String aname, String atype, String relativeID )
    {
        int     aid = -1;
       
        if(!Configuration.doServerCalls)
        {
            return aid;
        }

        HashMap<String, Object[]> queryVars = new HashMap<String, Object[]>();
        queryVars.put( "ActorName", new Object[] { aname } );
        queryVars.put( "ActorType", new Object[] { atype } );
        queryVars.put( "ParentID", new Object[] { parentID } );
        NewtMySql   newtMysql = newtState.querySubActorInstance( queryVars, true );
        ResultSet   rs = newtMysql.getResultSet();

        try {
            if( !rs.next() ) {
                aid = getActorID();
                newtState.addSubActorInstance( aid, aname, parentID, atype, "", "", "", relativeID, -1, -1 );
                newtState.ActorNamesToID.put( aname, aid );
                newtState.ActorIDToNames.put( aid, aname );
                newtState.ActorsToParent.put( aid, parentID );
                newtState.ActorTypes.put( aid, atype );
            } else {
               //System.out.println( "Actor exists. Returning prior newtState.ID." );
                aid = rs.getInt( 1 );
            }
        } catch( Exception e ) {
            e.printStackTrace();
        }

        newtMysql.close();
//       System.out.println( "Created subActor. Returning aid: " + aid + " for actor: " + aname + " with parent: " + parentID );
        return aid;
    }    

    public synchronized int addSourceOrDestinationActor( int aid, int otherAid, boolean isSource )
    {
        if(!Configuration.doServerCalls)
        {
            return aid;
        }

        newtState.updateSourceOrDestinationActor( aid, otherAid, (isSource ? "SourceActor" : "DestinationActor" ) );
       //System.out.println( "addSourceOrDestinationActor: " + otherAid + " for actor: " + aid );
        return 0;
    }

    // Zhaomo
    public synchronized int addSourceBySubactor(int aid, int otherAid)
    {
        if(!Configuration.doServerCalls)
        {
            return aid;
        }

        newtState.updateSourceBySubactor(aid, otherAid);
       //System.out.println( "addSourceBySubactor: " + otherAid + " for subactor: " + aid );
        return 0;
    }



    public synchronized int setProvenanceHierarchy( int actorID, String hierarchy )
    {
        if(!Configuration.doServerCalls)
        {
            return actorID;
        }

        Node root;

        try {
        	String tempDir = Utilities.GetTempDirectory();
            FileWriter f = new FileWriter( tempDir + "/" + actorID + "hierarchy.xml" );
            BufferedWriter b = new BufferedWriter( f );

            b.write( hierarchy );
            b.flush();
            b.close();

            NewtXmlDOM dom = new NewtXmlDOM();
            Document doc = dom.doc( tempDir + "/" + actorID + "hierarchy.xml" );
            root = dom.root( doc );
        
            boolean done = setupActorHierarchy( dom, root, null );
            if( done == true ) {
                return 0;
            } else {
                return -1;
            }
        } catch( Exception e ) {
        	e.printStackTrace();
            return -1;
        }
    }

    protected synchronized boolean setupActorHierarchy( NewtXmlDOM dom, Node n, String pname )
    {
        if(!Configuration.doServerCalls)
        {
            return true;
        }

        try {
            switch( n.getNodeType() ) {
            case Node.ELEMENT_NODE:
            {
                if( n.getNodeName().equals( "Universe" ) ) {
                    Node uname = dom.getAttribute( n, "name" );
                    
                    if( uname != null ) {
                        newtState.LogicalParents.put( uname.getNodeValue(), null );
                        newtState.ActorStatics.put( uname.getNodeValue(), true );

                        String uFilename = newtState.getLogDir() + uname.getNodeValue();
                        if( !(new File( uFilename )).exists() ) {
                            boolean dir = (new File( uFilename )).mkdirs();
                            if( dir == false ) {
                                throw new Exception( "Couldn't create universe directory." );
                            }
                        }
                        newtState.ActorDirs.put( newtState.ActorNamesToID.get( uname.getNodeValue() ), uFilename );
                        newtState.addActorGset( "Universe", "", "True", -1, "", "" );
                    } else {
                        throw new Exception( "Universe name missing in actor containment specification." );
                    }

                    n = n.getFirstChild();
                    while( n != null ) {
                        setupActorHierarchy( dom, n, uname.getNodeValue() );
                        n = (Node)n.getNextSibling();
                    }
                    
                } else if( n.getNodeName().equals( "RootActor" ) ) {
                    Node rname = dom.getAttribute( n, "name" );
                    
                    if( rname != null ) {
                        newtState.LogicalParents.put( rname.getNodeValue(), pname );
                        newtState.ActorStatics.put( rname.getNodeValue(), true );
                        Vector<String> childActors = newtState.LogicalChildren.get( pname );
                        
                        if( childActors == null ) {
                            childActors = new Vector<String>();
                        }
                        childActors.add( rname.getNodeValue() );
                        newtState.LogicalChildren.put( pname, childActors );

                        String rFilename = newtState.ActorDirs.get( newtState.ActorNamesToID.get( pname ) ) + "/" + rname.getNodeValue();
                        if( !(new File( rFilename )).exists() ) {
                            boolean dir = (new File( rFilename )).mkdirs();
                            if( dir == false ) {
                                throw new Exception( "Couldn't create root actor directory." );
                            }
                        }
                        newtState.ActorDirs.put( newtState.ActorNamesToID.get( rname.getNodeValue() ), rFilename );
                        newtState.addActorGset( "RootActor", "Universe", "True", -1, "", "" );
                    } else {
                        throw new Exception( "RootActor name missing in actor containment specification." );
                    }

                    n = n.getFirstChild();
                    while( n != null ) {
                        setupActorHierarchy( dom, n, rname.getNodeValue() );
                        n = (Node)n.getNextSibling();
                    }
                    
                } else {
                    newtState.LogicalParents.put( n.getNodeName(), pname );

                    Node source = dom.getAttribute(n, "input" );
                    Node destination = dom.getAttribute(n, "output" );
                    Node s = dom.getAttribute( n, "static" );
                    if( s != null ) {
                        boolean svalue;
                        
                        if( s.getNodeValue().equals( "true" ) ) {
                            svalue = true;
                            newtState.addActorGset( n.getNodeName(), pname, "True", -1, source.getNodeValue(), destination.getNodeValue() );
                        } else {
                            svalue = false;
                            newtState.addActorGset( n.getNodeName(), pname, "False", -1, source.getNodeValue(), destination.getNodeValue() );
                        }
                        newtState.ActorStatics.put( n.getNodeName(), svalue );
                    } else {
                        newtState.ActorStatics.put( n.getNodeName(), true );
                        newtState.addActorGset( n.getNodeName(), pname, "True", -1, source.getNodeValue(), destination.getNodeValue() );
                    }

                    Vector<String> childActors = newtState.LogicalChildren.get( pname );
                    if( childActors == null ) {
                        childActors = new Vector<String>();
                    }
                    childActors.add( n.getNodeName() );
                    newtState.LogicalChildren.put( pname, childActors );

                    Node c = n.getFirstChild();
                    while( c != null ) {
                        setupActorHierarchy( dom, c, n.getNodeName() );
                        c = (Node)c.getNextSibling();
                    }
                }
            
                break;
            }

            case Node.TEXT_NODE:
            {
                return false;
            }
            }
        } catch( Exception e ) {
           System.out.println( "Error: " + e.getMessage() );
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public synchronized int commit( int actorID, boolean isSubActor)
    {
        if(!Configuration.doServerCalls)
        {
            return 0;
        }
        
        
        if(actorID == 1)
        {
    		//System.out.println("Commiting JobActor");
    		loadData("JobActor", false);   // <---------------------- change when running locally
    		return 0;
        }
        
       	newtState.commitActorInstance( actorID, isSubActor );
        if( newtState.ActorsToFiles.get( actorID ) != null ) {
            try {
                newtState.ActorsToFiles.get( actorID ).close();
            } catch( Exception e ) {
               System.out.println( "Error: " + e.getMessage() );
                e.printStackTrace();
            }
        }

        return 0;
    }
    
    public List<Integer> isParentOfSubActors(String table)
    {
    	NewtMySql newtMysql = new NewtMySql( Configuration.mysqlUser, Configuration.mysqlPassword, "Newt" );
    	int actorID = -1;
    	ResultSet rs = null;
    	List<Integer> result = new ArrayList<Integer>();
		try 
		{
	       rs = newtMysql.executeQuery("SELECT ActorID FROM actorInstances WHERE ActorTable='" + table +"'");
	       while(rs.next())
	       {
	    	   actorID = rs.getInt(1);
	       }
		
	       rs = newtMysql.executeQuery("SELECT actorID FROM subActorInstances WHERE parentID="+actorID);
	       while(rs.next())
	       {
	    	   result.add(rs.getInt(1));
	       }
	       newtMysql.close();
	       
           return result;
		} 
        catch (SQLException e) 
        {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    	return null;
    }
    
    public Integer getParentOfSubActorByID(String table)
    {
    	NewtMySql newtMysql = new NewtMySql( Configuration.mysqlUser, Configuration.mysqlPassword, "Newt" );
    	int parentID = -1;
    	ResultSet rs = null;
		try 
		{
		       rs = newtMysql.executeQuery("SELECT ParentID FROM subActorInstances WHERE ActorID=" + table);
		       while(rs.next())
		       {
		    	   parentID = rs.getInt(1);
		       }
    		
//		       rs = newtMysql.executeQuery("SELECT actor FROM ActorInstances WHERE actorID="+parentID);
//		       while(rs.next())
//		       {
//		    	   parentName= rs.getString(1);
//		       }
		       newtMysql.close();
    		       
               return parentID;
    		
		} 
        catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    	return null;
    }    
    
    public Integer getParentOfSubActorByTable(String table)
    {
    	NewtMySql newtMysql = new NewtMySql( Configuration.mysqlUser, Configuration.mysqlPassword, "Newt" );
    	int parentID = -1;
    	ResultSet rs = null;
		try 
		{
		       rs = newtMysql.executeQuery("SELECT ParentID FROM subActorInstances WHERE ActorTable='" + table +"'");
		       while(rs.next())
		       {
		    	   parentID = rs.getInt(1);
		       }
    		
		       newtMysql.close();
    		       
               return parentID;
    		
		} 
        catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    	return null;
    }        
    
    
    public Integer getActorIdOfParentOfSubActor(String table)
    {
    	NewtMySql newtMysql = new NewtMySql( Configuration.mysqlUser, Configuration.mysqlPassword, "Newt" );
    	int parentID = -1;
    	ResultSet rs = null;
		try 
		{
		       rs = newtMysql.executeQuery("SELECT a.ActorID FROM actorInstances as a,subActorInstances as s" +
		       		" WHERE a.ActorTable='" + table + "' and s.ParentID=a.ActorID");
		       while(rs.next())
		       {
		    	   parentID = rs.getInt(1);
		       }
		       newtMysql.close();
    		       
               return parentID;
    		
		} 
        catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    	return null;
    }        
    
    public ResultSet getAllActors(String type)
    {
    	NewtMySql newtMysql = new NewtMySql( Configuration.mysqlUser, Configuration.mysqlPassword, "Newt" );
    	ResultSet rs = null;
	
    	rs = newtMysql.executeQuery("SELECT ActorID,ActorTable,ActorUrl FROM ActorInstances WHERE ActorType='" + type +"'");
    	return rs;
    }
    

    /**
     * Load the contents of the log file tablename_final into the table tablename.
     * Add a commit ProveanceTask to the provenanceContainer. Flush the log file. 
     */
    public synchronized int loadData( String table, final boolean isSubActor ) {

        if (isSubActor) 
        { 
        	final String filename = table;
            Thread thr = new Thread() {
                public void run() {
                	// Add a commit ProvenanceTask. Needed to notify the thread that is waiting to write provenace to the file
                    newtState.commitFile( filename );
                    newtState.commitComplete( filename, isSubActor );
                   //System.out.println( "Subactor Finished populating: " + filename );
                }
            };
            thr.start();
        }
        // It's an actor
        else 
        {
        	// If the actor is a parent of subActors
        	final List<Integer> subActors = newtState.isParentOfSubActors(table); 
        	final String tn = table;
        	if(!subActors.isEmpty()) 
        	{
        		Thread thr = new Thread() 
			    {
			        public void run() 
			        {
		        		newtState.finalize( tn );
		        		newtState.commitComplete(tn, isSubActor );
		        		//System.out.println( "Actor with subactors finished populating: " + tn );
			        }
			    };
			    thr.start();
			    return 0;
        	}
        	// If the actor has no subactors 
        	else 
        	{
        		/**** Only when JobActor commits, are the files going to get loaded ***/
                final String filename = table;
                final String newfilename = table + "_final";
                String tempDir = Utilities.GetTempDirectory();
                //TODO Ksh: Added local to the query below , weird bug related to Ubuntu
                final String update = "load data local infile '" + tempDir + "/Newt/" + newfilename + "' into table Newt." + table
                                      + " fields terminated by '" + NewtState.fieldTerminator + "' lines terminated by '" 
                		              + NewtState.lineTerminator + "'";

                
                if(filename.equals("JobActor")) // <------------------------ Change when running locally
        		{
            		//System.out.println("Loading data of all actors to database");
            		//Get all children actors and for each call loadDataFromSubActors
            		ResultSet rs = getAllActors("TestActor"); // <------------------------ Change when running locally
            		
            		try {
						while(rs.next())
						{
							final Integer actorID = rs.getInt(1);
							final String actorTable = rs.getString(2);
							final String actorUrl = rs.getString(3);
							
						    Thread thr = new Thread() 
						    {
						        public void run() 
						        {
						        	RpcCommunication.getService(actorUrl).loadDataIntoActor(actorID,actorTable);
						        }
						    };
						    thr.start();
						    return 0;
						}
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            		
        		}
                else
                {
	                Thread thr = new Thread() {
	                    public void run() {
	                    	// Add a commit ProvenanceTask
	                        newtState.commitFile( filename );
	                        // Flush the log file
	                        newtState.finalize( filename );
	                        newtState.formatProvenanceFile( filename, newfilename );
	                        Vector<String> batchUpdate = new Vector<String>();
	                        batchUpdate.add( update );
	                        try {
	                            newtState.updateBatch( batchUpdate );
	                        } catch( Exception e ) {
	                            e.printStackTrace();
	                        }
	                        //Delete the file tablename and set Committed=true
	                        newtState.commitComplete( filename, isSubActor );
	                       //System.out.println( "actor Finished populating: " + filename );
	                    }
	                };
	                thr.start();
	                return 0;
	        	}
        	}
        } 
        
        return -1;
    }    
    

    public void loadDataIntoActor(Integer actorID, String actorTable)
    {
    	newtState.loadDataFromSubActors(actorID, actorTable);
    }
    
    public synchronized int cleanFiles( Vector<String> files )
    {
       //System.out.println( "Dropping uncommitted files upon parent commit" );
        newtState.cleanFiles( files );
        return 0;
    }

    public synchronized int finalizeCommit( String tableName, boolean isSubActor )
    {
        newtState.finalizeCommit( tableName, isSubActor );
        return 0;
    }

    public synchronized int sendGhostData( String ghostSourceTable, String ghostDestinationUrl, String ghostDestinationTable )
    {
        NewtSelectStatementBuilder statement = new NewtSelectStatementBuilder();
        statement.addTable( "Newt", ghostSourceTable );
        statement.addColumn( "Output" );
        statement.setQuery( false );
        ResultSet rs = statement.execute();

        Vector<String> ghostData = new Vector<String>();

        try {
            while( rs.next() ) {
                String locatable = rs.getString( 1 );
                ghostData.add( locatable );
            }
        } catch( Exception e ) {
           System.out.println( e.getMessage() );
            e.printStackTrace();
        }

        if( ghostDestinationUrl.equals( "self" ) ) {
            statement.addTable( "Newt", ghostDestinationTable );
            statement.addColumn( "Input" );
            statement.setQuery( false );
            rs = statement.execute();

            Vector<String> ghostMatch = new Vector<String>();

            try {
                while( rs.next() ) {
                    String locatable = rs.getString( 1 );
                    ghostMatch.add( locatable );
                }
            } catch( Exception e ) {
               System.out.println( e.getMessage() );
                e.printStackTrace();
            }
            newtState.ghost( ghostData, ghostMatch, ghostSourceTable, ghostDestinationTable );
        } else {
            try {
                RpcCommunication.getService( ghostDestinationUrl ).ghost( ghostSourceTable, ghostDestinationTable, ghostData );
            } catch( Exception e ) {
               System.out.println( e.getMessage() );
                e.printStackTrace();
            }
        }

        statement.close();
        return 0;
    }

    public synchronized int ghost( String ghostSourceTable, String ghostDestinationTable, Vector<String> ghostData )
    {
        NewtSelectStatementBuilder statement = new NewtSelectStatementBuilder();
        statement.addTable( "Newt", ghostDestinationTable );
        statement.addColumn( "Input" );
        statement.setQuery( false );
        ResultSet rs = statement.execute();

        Vector<String> ghostMatch = new Vector<String>();

        try {
            while( rs.next() ) {
                String locatable = rs.getString( 1 );
                ghostMatch.add( locatable );
            }
        } catch( Exception e ) {
           System.out.println( e.getMessage() );
            e.printStackTrace();
        }

        statement.close();
        
        newtState.ghost( ghostData, ghostMatch, ghostSourceTable, ghostDestinationTable );
        return 0;
    }

    public synchronized int ghostComplete( String sourceTable, String destinationTable, String ghostTable )
    {
        NewtSelectStatementBuilder selectStatement = new NewtSelectStatementBuilder();
        selectStatement.addTable( "Newt", "actorInstances" );
        selectStatement.addColumn( "ActorID" );
        selectStatement.addColumn( "ActorTable" );
        selectStatement.addColumn( "ActorUrl" );
        selectStatement.addColumn( "ParentID" );
        selectStatement.addAbsoluteCondition( "ActorTable", sourceTable );
        selectStatement.addAbsoluteCondition( "ActorTable", destinationTable );
        selectStatement.setQuery( false );
        ResultSet rs = selectStatement.execute();

        int sid = -1;
        int did = -1;
        int pid = -1;
        String dUrl = null;

        try {
            while( rs.next() ) {
                if( rs.getString( 2 ).equals( sourceTable ) ) {
                    sid = rs.getInt( 1 );
                    pid = rs.getInt( 4 );
                } else if( rs.getString( 2 ).equals( destinationTable ) ) {
                    did = rs.getInt( 1 );
                    dUrl = rs.getString( 3 );
                }
            }
        } catch( Exception e ) {
           System.out.println( e.getMessage() );
            e.printStackTrace();
        }

        selectStatement.close();

        NewtUpdateStatementBuilder updateStatement = new NewtUpdateStatementBuilder();
        if( did != -1 && sid != -1 && pid != -1 && dUrl != null ) {
            int gid = getActorID();
            newtState.addActorInstance( gid, ghostTable, pid, "GHOST", ghostTable, dUrl, "True", "", sid, did );

            updateStatement.setTable( "Newt", "actorInstances" );
            updateStatement.addColumn( "SourceActor", gid );
            updateStatement.addCondition( "ActorID", did );
            updateStatement.setQuery( false );
            updateStatement.execute();

            updateStatement.setTable( "Newt", "actorInstances" );
            updateStatement.addColumn( "DestinationActor", gid );
            updateStatement.addCondition( "ActorID", sid );
            updateStatement.setQuery( false );
            updateStatement.execute();
        }

        updateStatement.setTable( "Newt", "actorInstances" );
        updateStatement.addColumn( "Committed", "True" );
        updateStatement.addCondition( "ActorTable", destinationTable );
        updateStatement.setQuery( false );
        updateStatement.execute();
        updateStatement.close();

       //System.out.println( "Commit completed for: " + destinationTable );

        return 0;
    }

    public synchronized int setProvenanceSchemas( int actorID, String hierarchy )
    {
        if(!Configuration.doServerCalls)
        {
            return 0;
        }

        Node root;

        try {
        	String tempDir = Utilities.GetTempDirectory();
            FileWriter f = new FileWriter( tempDir + "/" + actorID + "schemas.xml" );
            BufferedWriter b = new BufferedWriter( f );

            b.write( hierarchy );
            b.flush();
            b.close();

            NewtXmlDOM dom = new NewtXmlDOM();
            Document doc = dom.doc( tempDir + "/" + actorID + "schemas.xml" );
            root = dom.root( doc );
        
            boolean done = setupSchemas( dom, -1, root );
            if( done == true ) {
                return 0;
            } else {
                return -1;
            }
        } catch( Exception e ) {
            e.printStackTrace();
            return -1;
        }
    }

    protected synchronized boolean setupSchemas( NewtXmlDOM dom, int sid, Node n )
    {
        if(!Configuration.doServerCalls)
        {
            return true;
        }

        try {
            switch( n.getNodeType() ) {
            case Node.ELEMENT_NODE:
            {
                if( n.getNodeName().equals( "Provenance" ) ) {
                    n = n.getFirstChild();
                    while( n != null ) {
                        setupSchemas( dom, 1, n );
                        n = (Node)n.getNextSibling();
                    }
                } else if( n.getNodeName().equals( "Schema" ) ) {
                    Node sname = dom.getAttribute( n, "name" );
                    
                    if( sname != null ) {
                        Node actor = dom.getAttribute( n, "actor" );
                        int schemaID = newtState.getNextSchemaID();

                        newtState.addSchema( actor.getNodeValue(), schemaID, sname.getNodeValue(), sname.getNodeValue() + "_logical" );

                        Node c = n.getFirstChild();
                        while( c != null ) {
                            setupSchemas( dom, schemaID, c );
                            c = (Node)c.getNextSibling();
                        }
                        newtState.addSchemaTableRow( schemaID, "Time", "time", "TimeStamp", "bigint", 0 );
                    }
                } else {
                    String cname = n.getNodeName();
                    Node t = dom.getAttribute( n, "type" );
                    String ctype = t.getNodeValue();
                    Node d = dom.getAttribute( n, "datatype" );
                    String ndtype = d.getNodeValue();
                    String sdtype = null;
                    int indexlen = -1;
                    if( ndtype.equals( "KeyValuePair" ) || ndtype.equals( "ByteArrayProvenance" ) ) {
//                        sdtype = "varbinary(16)";
//                        indexlen = 16;
                        sdtype = "varchar(255)";
                        indexlen = 32;
                    } else if( ndtype.equals( "FileLocatable" ) ) {
                        sdtype = "varchar(255)";
                        indexlen = 32;
                    } else {
                        sdtype = "varchar(1024)";
                        indexlen = 1024;
                    }

                    newtState.addSchemaTableRow( sid, cname, ctype, ndtype, sdtype, indexlen );
                }
            }
            
            case Node.TEXT_NODE:
            {
                return true;
            }
            }
        } catch( Exception e ) {
           System.out.println( "Error: " + e.getMessage() );
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public synchronized int getSchemaID( int uID, String schemaName )
    { 
        int sid = -1;
        
        if(!Configuration.doServerCalls)
        {
            return sid;
        }

        HashMap<String, Object[]> queryVars = new HashMap<String, Object[]>();
        queryVars.put( "SchemaName", new Object[] { schemaName } );
        NewtMySql newtMysql = newtState.queryActorGset( queryVars, true );
        ResultSet   rs = newtMysql.getResultSet();

        try {
            if( !rs.next() ) {
                sid = -1;
            } else {
                sid = rs.getInt( 7 );
            }
        } catch( Exception e ) {
            e.printStackTrace();
        }

        newtMysql.close();
        return sid;
    }

    public synchronized int createProvTable( String update, String filename, boolean isSubActor )
    {
        if(!Configuration.doServerCalls)
        {
            return 0;
        }

        newtState.makeProvTable( update, filename , isSubActor);
        return 0;
    }

    public int trace( Vector data, String direction, int caid, int aid, String atype )
    {
        if(!Configuration.doServerCalls)
        {
            return 0;
        }

        int tid = -1;
        synchronized( newtState ) {
            tid = newtState.getNextTraceID();
        }

        NewtTrace newtTrace = new NewtTrace( tid, data, direction, newtState, aid, atype );
        int result = newtTrace.trace( caid, aid, atype );
        String status = result == tid ? "In-progress" : "Incorrect";
        String tableName = result == tid ? tid + "_table" : "";

        Vector<String> completedNodes = new Vector<String>();
        int numSlaves = newtState.registerTrace( tid, status, caid, tableName, completedNodes );

        if( result == tid ) {
            try {
                newtTrace.setTraceTable( tableName );
                newtTrace.start();
                synchronized( completedNodes ) {
                    while( completedNodes.size() != numSlaves ) {
                        try {
                            completedNodes.wait();
                        } catch( Exception e ) {
                        }
                    }
                }
            } catch( Exception e ) {
               System.out.println( e.getMessage() );
                e.printStackTrace();
            }
        }

        return result;
    }

    public synchronized int traceLocal( int tid,
                                         HashMap<String, HashMap<String, Vector<Vector>>> traceQuery,
                                         HashMap<String, Vector<String>> tableSources,
                                         Vector data,
                                         Vector locatableData )
    {
       System.out.println( "Local trace received: " + tid );
        Set entrySet = traceQuery.entrySet();

        for( Object eo: entrySet ) {
            Map.Entry e = (Map.Entry) eo;
            String st = (String) e.getKey();
            HashMap targetTables = (HashMap<String, Vector<Vector>>) e.getValue();
            Set targetEntries = targetTables.entrySet();

            for( Object to: targetEntries ) {
                Map.Entry t = (Map.Entry) to;
                String url = (String) t.getKey();
//                Object[] tables = (Object[]) t.getValue();
                Vector<Vector> tables = (Vector<Vector>) t.getValue();

                for( Vector tableo: tables ) {
                	Vector table = (Vector) tableo;
                    String tableName = (String) table.get(0);
                   System.out.println( "Join result of " + st + " with " + tableName + " on node " + url );
                }
            }
        }

       //System.out.println( "" );
        entrySet = tableSources.entrySet();

        for( Object eo: entrySet ) {
            Map.Entry e = (Map.Entry) eo;
            String st = (String) e.getKey();

//            Object[] tables = (Object[]) e.getValue();
            Vector<String> tables = (Vector<String>) e.getValue();
            for( String tableo: tables ) {
               System.out.println( st + ": " + (String) tableo );
            }
        }

        return newtState.traceLocal( tid, traceQuery, tableSources, data, locatableData );
    }

    public synchronized int continueTrace( int tid, String resultTable, String resultDataType, Vector results )
    {
       System.out.println( "Received continuing trace" );
        newtState.continueTrace( tid, resultTable, resultDataType, results );
        return tid;
    }

    public synchronized int traceComplete( int tid, String slave )
    {
        newtState.traceComplete( tid, slave );
        return 0;
    }

    public synchronized Object[] getTraceNode( int tid, int rpid, String atype, String rid )
    {
        NewtSelectStatementBuilder statement = new NewtSelectStatementBuilder();
        statement.addTable( "Trace", "traceActors" );
        statement.addColumn( "Status" );
        statement.addColumn( "TraceTable" );
        statement.addAbsoluteCondition( "TraceID", tid );
        statement.setQuery( false );
        ResultSet rs = statement.execute();

        String traceTable = null;
        try {
            if( !rs.next() ) {
                return null;
            } else {
                if( !rs.getString( 1 ).equals( "Complete" ) ) {
                    return null;
                } 
                traceTable = rs.getString( 2 );
            }
        } catch( Exception e ) {
            e.printStackTrace();
        }
            
        statement.addTable( "Trace", traceTable );
        statement.addColumns( new String[] { "ActorID", "ActorTable", "ActorUrl" } );
        statement.addAbsoluteCondition( "ActorType", atype );
        statement.addAbsoluteCondition( "ParentID", rpid );
        statement.addAbsoluteCondition( "RelativeID", rid );
        statement.setQuery( true );
        rs = statement.execute();

        Object[] result = null;
        try {
            if( !rs.next() ) {
                return null;
            } else {
                result = new Object[] { rs.getInt( 1 ), rs.getString( 2 ), rs.getString( 3 ) };
            }
        } catch( Exception e ) {
            e.printStackTrace();
        }

        statement.close();
        return result;
    }

    public Object[] getTraceNodeByActorID( int tid, String aname )
    {
        NewtSelectStatementBuilder statement = new NewtSelectStatementBuilder();
        statement.addTable( "Newt", "actorInstances" );
        statement.addTable( "Trace", tid + "_table" );
        statement.addColumn( tid + "_table.ActorUrl" );
        statement.addColumn( tid + "_table.ActorTable" );
        statement.addAbsoluteCondition( "ActorName", aname );
        statement.addJoinCondition( "actorInstances", "ActorID", tid + "_table", "ActorID" );
        statement.setQuery( true );
        ResultSet rs = statement.execute();

        Object[] result = null;
        try {
            if( !rs.next() ) {
                return null;
            } else {
                result = new Object[] { rs.getString( 1 ), rs.getString( 2 ) };
            }
        } catch( Exception e ) {
            e.printStackTrace();
        }

        statement.close();
        return result;
    }

    public synchronized HashSet getReplayFilter( int tid, String tableName )
    {
        System.err.println( "Returning replay filter for trace: " + tid + " from table: " + tableName );
        return newtState.getReplayFilter( tid, tableName );
    }
}


