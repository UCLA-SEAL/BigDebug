package newt.server;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import newt.actor.ProvenanceContainer;
import newt.actor.ProvenanceItem;
import newt.actor.ProvenanceTask;
import newt.common.ByteArray;
import newt.common.Configuration;
import newt.common.RpcCommunication;
import newt.server.sql.NewtBulkInsertStatementBuilder;
import newt.server.sql.NewtCreateStatementBuilder;
import newt.server.sql.NewtDeleteStatementBuilder;
import newt.server.sql.NewtInsertStatementBuilder;
import newt.server.sql.NewtSelectStatementBuilder;
import newt.server.sql.NewtSqlStatementBuilder;
import newt.server.sql.NewtUpdateStatementBuilder;
import newt.utilities.Utilities;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

//import javax.security.auth.login.Configuration;

public class NewtState {
    static HashMap<String, Integer>                 ActorNamesToID = null;
    static HashMap<Integer, String>                 ActorIDToNames = null;
    static HashMap<Integer, Integer>                ActorsToParent = null;
    static HashMap<String, String>                  LogicalParents = null;
    static HashMap<String, Vector<String>>          LogicalChildren = null; 
    static HashMap<Integer, String>                 ActorTypes = null;
    static HashMap<Integer, String>                 ActorDirs = null;
    static HashMap<String, Boolean>                 ActorStatics = null;
    static HashMap<Integer, FileWriter>             ActorsToFiles = null;
    static HashMap<String, List<OutputStream>>      ProvenanceFiles = null;

    static HashMap<Integer, NewtTrace>                  traceInstances = null;
    static HashMap<Integer, HashMap<String, String[]>>  traceResults = null;
    static HashMap<Integer, Vector<String>>             completedTraces = null;
    
    private static NewtState                    instance = null;

    String                                      user = null;
    String                                      password = "";
    String                                      master = null;
    String                                      logdir = null;
    String                                      newtHome = null;
    boolean                                     isMaster = Configuration.isMaster;
    Vector<String>                              slaves = null;
    static int                                  nextSlave = 0;
    String                                      selfName = null;
    String                                      selfUrl = null;
    InetAddress[]                               selfs = null;
    static Object                               lock = new Object();
    static int                                  longLength = Long.toString( Long.MAX_VALUE ).length();
    Vector<ProvenanceTask>                      provenanceData = null;
    
    public static String fieldTerminator= "##||\t||##";
    public static String fieldTerminator_ESCAPED= "##\\|\\|\\t\\|\\|##";
    public static String lineTerminator= "##||||##\n";
    public static String lineTerminator_ESCAPED= "##\\|\\|\\|\\|##\\n";
    static byte[] fieldTerminatorBytes = fieldTerminator.getBytes();
    static byte[] lineTerminatorBytes = lineTerminator.getBytes();

    private static String tempNewtDirectory = Utilities.GetTempDirectory() + "/" + "Newt/";
    protected NewtState()
    { 
        newtHome = System.getenv( "NEWT_HOME" );
        if( newtHome == null ) {
           System.out.println( "Configuration not found. Export NEWT_HOME." );
            System.exit( 1 );
        }

        try {
            selfName = InetAddress.getLocalHost().getCanonicalHostName();
            selfs = InetAddress.getAllByName( selfName );
        } catch( Exception e ) {
            e.printStackTrace();
        }

       System.out.println( "Initing newtState..." );
        slaves = new Vector<String>();
        parseConfig();
        user = newt.common.Configuration.mysqlUser;
        password = newt.common.Configuration.mysqlPassword;
        
        ActorNamesToID = new HashMap<String, Integer>();
        ActorIDToNames = new HashMap<Integer, String>();
        ActorsToParent = new HashMap<Integer, Integer>();
        LogicalParents = new HashMap<String, String>();
        LogicalChildren = new HashMap<String, Vector<String>>();
        ActorTypes = new HashMap<Integer, String>();
        ActorDirs = new HashMap<Integer, String>();
        ActorStatics = new HashMap<String, Boolean>();
        ActorsToFiles = new HashMap<Integer, FileWriter>();
        ProvenanceFiles = new HashMap<String, List<OutputStream>>();
        provenanceData = new Vector<ProvenanceTask>();

        traceInstances = new HashMap<Integer, NewtTrace>();
        traceResults = new HashMap<Integer, HashMap<String, String[]>>();
        completedTraces = new HashMap<Integer, Vector<String>>();

        NewtMySql newtMysql = new NewtMySql( user, password );
        //isMaster = true;newt.common.Configuration.cleanDB= true;
        //Configuration.cleanDB = false; 	//<----------------------------- Needed for running locally. Set when running peer
        //TODO Ksh
        isMaster = Configuration.isMaster;

        newtMysql.init( isMaster, "Newt" );
        
        if( isMaster ) {
            addSchema( "GHOST", 0, "GHOST_table", "GHOST_table_logical" );
            addSchemaTableRow( 0, "Input", "input", "FileLocatable", "varchar(255)", 32 );
            addSchemaTableRow( 0, "Output", "output", "FileLocatable", "varchar(255)", 32 );
        } else {
            try {
                String dir = tempNewtDirectory;
                File newtDir = new File( dir );
                if( newtDir.exists() ) {
                    File[] newtTempFiles = newtDir.listFiles();
                    for( File f: newtTempFiles ) {
                        f.delete();
                    }
                    newtDir.delete();
                }
            
                boolean success = newtDir.mkdir();
                if (success) {
                   System.out.println( "Directory: " + dir + " created" );
                }
            } catch( Exception e ) {
                e.printStackTrace();
            }

            Thread provenanceWriter = new Thread() {
                public void run() {
                    while( true ) {
                        writeProvenance();
                    }
                }
            };
            provenanceWriter.start();
        }
    }
    
    public static NewtState getInstance()
    {
        synchronized( lock ) {
            if( instance == null ) {
                instance = new NewtState();
            }
        }
        return instance;
    }

    public void parseConfig()
    {
        String configFile = newtHome + "/" + "NewtConfig.xml";
        Node root = null;
        NewtXmlDOM dom = new NewtXmlDOM();

        try {
            Document doc = dom.doc( configFile );
            root = dom.root( doc );
        } catch( Exception e ) {
           System.out.println( "Failed to process config file." );
            System.exit( 1 );
        }

        setupNewt( dom, root );
    }

    public void setupNewt( NewtXmlDOM dom, Node n )
    {
        try {
            switch( n.getNodeType() ) {
            case Node.ELEMENT_NODE:
            {
                if( n.getNodeName().equals( "Configuration" ) ) {
                    n = n.getFirstChild();
                    while( n != null ) {
                        setupNewt( dom, n );
                        n = (Node)n.getNextSibling();
                    }
                } else if( n.getNodeName().equals( "Nodes" ) ) {
                    ArrayList<Node> children = dom.childrenByTag( n, "Master" );
                    Node m = children.get( 0 );
                    Node v = dom.getAttribute( m, "value" );
                    master = v.getNodeValue();
                    InetAddress masters [] = InetAddress.getAllByName( master );
                    
                   System.out.println( "Master: " + master );

                    isMaster = false;
                    for( InetAddress i: masters ) {
                        for( InetAddress s: selfs ) {
                            if( s.getCanonicalHostName().equals( i.getCanonicalHostName() ) ) {
                                isMaster = true;
                            }
                        }
                    }

                    isMaster = false; 		//<----------------------------- Needed for running locally

                    
                    if(isMaster)
                    	System.out.println("I AM DA MASTER!");
                    
                    master = "http://" + master + ":8899/hessianrpc";

                    children = dom.childrenByTag( n, "Slave" );
                    for( Node c: children ) {
                        v = dom.getAttribute( c, "value" );
                        String slave = v.getNodeValue();
                        slaves.add( "http://" + v.getNodeValue() + ":8898/hessianrpc" );	//<----------------------------- Needed for running locally
                       System.out.println( "Slave: " + slave );
                        
                        InetAddress slaves [] = InetAddress.getAllByName( slave );
                        for( InetAddress i: slaves ) {
                            for( InetAddress s: selfs ) {
                                if( s.getCanonicalHostName().equals( i.getCanonicalHostName() ) ) {
                                    selfUrl = "http://" + v.getNodeValue() + ":8898/hessianrpc";	//<----------------------------- Needed for running locally
                                    isMaster=false;											//<----------------------------- Needed for running locally
                                }
                            }
                        }
                    }
                }else if( n.getNodeName().equals( "LogDir" ) ) {
                    Node v = dom.getAttribute( n, "value" );
                    logdir = v.getNodeValue();
                } else {
                }
            }
            case Node.TEXT_NODE:
            {
            }
            }
        } catch( Exception e ) {
            e.printStackTrace();
        }
    }

    public String getLogDir()
    {
        return logdir;
    }

    public synchronized int getNextActorID()
    {
        int aid = -1;
        
       

        NewtSelectStatementBuilder statement = new NewtSelectStatementBuilder();
        statement.addTable( "Newt", "stateIDs" );
        statement.addColumn( "ActorID" );
        statement.addColumn( "DummyIndex" );
        statement.setQuery( false );
        ResultSet rs = statement.execute();
        try {
            rs.next();
            aid = rs.getInt( 1 );
            rs.updateInt( 1, aid + 1 );
            rs.updateRow();
        } catch( Exception e ) {
            e.printStackTrace();
        }

        statement.close();
        return aid;
    }

    public synchronized int getNextSchemaID()
    {
        int sid = -1;
        
        NewtSelectStatementBuilder statement = new NewtSelectStatementBuilder();
        statement.addTable( "Newt", "stateIDs" );
        statement.addColumn( "SchemaID" );
        statement.addColumn( "DummyIndex" );
        statement.setQuery( false );
        ResultSet rs = statement.execute();
        try {
            rs.next();
            sid = rs.getInt( 1 );
            rs.updateInt( 1, sid + 1 );
            rs.updateRow();
        } catch( Exception e ) {
            e.printStackTrace();
        }

        statement.close();
        return sid;
    }

    public synchronized int getNextTraceID()
    {
        int tid = -1;
        
        NewtSelectStatementBuilder statement = new NewtSelectStatementBuilder();
        statement.addTable( "Newt", "stateIDs" );
        statement.addColumn( "TraceID" );
        statement.addColumn( "DummyIndex" );
        statement.setQuery( false );
        ResultSet rs = statement.execute();
        try {
            rs.next();
            tid = rs.getInt( 1 );
            rs.updateInt( 1, tid + 1 );
            rs.updateRow();
        } catch( Exception e ) {
            e.printStackTrace();
        }

        statement.close();
        return tid;
    }

    public void addActorGset( String atype, String ptype, String s, int sid, String stype, String dtype )
    {
        NewtInsertStatementBuilder statement = new NewtInsertStatementBuilder();
        statement.setTable( "Newt", "actorGset" );
        statement.addValues( new Object[] { atype, ptype, s, "", stype, dtype, sid, "" } );
        statement.setQuery();
        statement.execute();
        statement.close();
    }

    public synchronized int queryTraceActor( int traceID )
    {
        int aid = -1;
       
        NewtSelectStatementBuilder statement = new NewtSelectStatementBuilder();
        statement.addColumn( "*" );
        statement.addTable( "Trace", "traceActors" );
        statement.addAbsoluteCondition( "TraceID", traceID );
        statement.setQuery( false );
        ResultSet rs = statement.execute();
        try {
            rs.next();
            aid = rs.getInt( 2 );
        } catch( Exception e ) {
            e.printStackTrace();
        }

        statement.close();
        return aid;
    }

    public synchronized void addActorInstance( int aid, String aname, int pid, String atype, String tname, String tUrl, String committed, String rid, int sid, int did )
    {       
        NewtInsertStatementBuilder statement = new NewtInsertStatementBuilder();
        statement.setTable( "Newt", "actorInstances" );
        statement.addValues( new Object[] { aid, aname, pid, atype, tname, tUrl, committed, rid, sid, did } );
        statement.setQuery();
        statement.execute();
        statement.close();
    }
    
    public synchronized void addSubActorInstance( int aid, String aname, int pid, String atype, String tname, String tUrl, String committed, String rid, int sid, int did )
    {
        NewtInsertStatementBuilder statement = new NewtInsertStatementBuilder();
        statement.setTable( "Newt", "subActorInstances" );
        statement.addValues( new Object[] { aid, aname, pid, atype, tname, tUrl, committed, rid, sid, did } );
        statement.setQuery();
        statement.execute();
        statement.close();
    }    

    public synchronized void updateSourceOrDestinationActor( int aid, int otherAid, String columnName )
    {
        NewtUpdateStatementBuilder statement = new NewtUpdateStatementBuilder();
        statement.setTable( "Newt", "actorInstances" );
        statement.addColumn( columnName, otherAid+"" );
        statement.addCondition( "ActorID", aid );
        statement.setQuery( false );
        statement.execute();
        statement.close();
    }



    // Zhaomo
    public synchronized void updateSourceBySubactor( int aid, int otherAid)
    {
        // get parent's actor id
        NewtSelectStatementBuilder selectStatement = new NewtSelectStatementBuilder();
        selectStatement.addColumn("ParentID");
        selectStatement.addTable( "Newt", "subActorInstances" );
        selectStatement.addAbsoluteCondition( "ActorID", aid );
        selectStatement.setQuery( false );
        ResultSet rs = selectStatement.execute();
        int parentID = -1;
        try {
            rs.next();
            parentID = rs.getInt(1);
        } catch( Exception e ) {
            e.printStackTrace();
        }

        // get sourceActor list
        selectStatement = new NewtSelectStatementBuilder();
        selectStatement.addColumn("SourceActor");
        selectStatement.addTable( "Newt", "actorInstances" );
        selectStatement.addAbsoluteCondition( "ActorID", parentID);
        selectStatement.setQuery( false );
        rs = selectStatement.execute();
        String sourceList = "";
        try {
            rs.next();
            sourceList = rs.getString(1);
        } catch( Exception e ) {
            e.printStackTrace();
        }

        // add new source
        if (sourceList == null || sourceList.equals("") ||  sourceList.equals("-1"))
        	sourceList = otherAid + "";
        else
        	sourceList = sourceList + " " + otherAid;
        
       System.out.println("The sourcelist for actor " + parentID + " updated by subactor " + aid + " is " + sourceList);
        
        // update
        NewtUpdateStatementBuilder statement = new NewtUpdateStatementBuilder();
        statement.setTable( "Newt", "actorInstances" );
        statement.addColumn( "SourceActor", sourceList );
        statement.addCondition( "ActorID", parentID );
        statement.setQuery( false );
        statement.execute();
        statement.close();
    }




    public synchronized void addSchema( String atype, int sid, String sname, String tname )
    {
        NewtUpdateStatementBuilder updateStatement = new NewtUpdateStatementBuilder();
        updateStatement.setTable( "Newt", "actorGset" );
        updateStatement.addColumn( "SchemaName", sname );
        updateStatement.addColumn( "SchemaID", sid );
        updateStatement.addColumn( "SchemaTable", tname );
        updateStatement.addCondition( "ActorType", atype );
        updateStatement.setQuery( false );
        updateStatement.execute();
        updateStatement.close();

        NewtCreateStatementBuilder createStatement = new NewtCreateStatementBuilder();
        createStatement.setTable( "Newt", tname );
        createStatement.addColumn( "ColumnName", "varchar(255)", false );
        createStatement.addColumn( "ColumnType", "varchar(255)", true );
        createStatement.addColumn( "NewtDataType", "varchar(255)", true );
        createStatement.addColumn( "SqlDataType", "varchar(255)", true );
        createStatement.addColumn( "IndexLength", "int", true );
        createStatement.setPrimaryKey( "ColumnName" );
        createStatement.setQuery();
        createStatement.execute();
        createStatement.close();
    }

    public synchronized void addSchemaTableRow( int sid, String cname, String ctype, String ndtype, String sdtype, int indexlen )
    {
        String tname = null;
        
        NewtSelectStatementBuilder selectStatement = new NewtSelectStatementBuilder();
        selectStatement.addColumn( "*" );
        selectStatement.addTable( "Newt", "actorGset" );
        selectStatement.addAbsoluteCondition( "SchemaID", sid );
        selectStatement.setQuery( false );
        ResultSet rs = selectStatement.execute();
        try {
            rs.next();
            tname = rs.getString( 8 );
        } catch( Exception e ) {
            e.printStackTrace();
        }

        selectStatement.addTable( "Newt", tname );
        selectStatement.addColumn( "ColumnName" );
        selectStatement.addAbsoluteCondition( "ColumnName", cname );
        selectStatement.setQuery( false );
        rs = selectStatement.execute();
        try {
            if( rs.next() ) {
                return;
            }
        } catch( Exception e ) {
           System.out.println( e.getMessage() );
            e.printStackTrace();
        }

        NewtInsertStatementBuilder insertStatement = new NewtInsertStatementBuilder();
        insertStatement.setTable( "Newt", tname );
        insertStatement.addValues( new Object[] { cname, ctype, ndtype, sdtype, indexlen } );
        insertStatement.setQuery();
        insertStatement.execute();
        insertStatement.close();
    }

    /**
     * Update the row of aid in table (sub)ActorInstances and set the table name and peer URL columns
     * @param aid
     * @param actorTable
     * @param sid
     * @param peerIP
     * @param isSubActor
     * @return
     */
    public synchronized String createProvenanceTable( int aid, String actorTable, int sid, byte[] peerIP, boolean isSubActor) {
        String  stname = null;

        NewtSelectStatementBuilder selectStatement = new NewtSelectStatementBuilder();
        selectStatement.addColumn( "SchemaTable" );
        selectStatement.addTable( "Newt", "actorGset" );
        selectStatement.addAbsoluteCondition( "SchemaID", sid );
        selectStatement.setQuery( false );
        ResultSet rs = selectStatement.execute();
        try {
            rs.next();
            stname = rs.getString( 1 );
        } catch( Exception e ) {
            e.printStackTrace();
        }

        // Get the information about the columns for the actor and subActor
        selectStatement.addColumn( "*" );
        selectStatement.addTable( "Newt", stname );
        selectStatement.setQuery( false );
        rs = selectStatement.execute();

        String remoteUpdate = null;
        String actorUrl = null;
        //Create a table with columns that store the provenance for example Input and Output
        if(!isSubActor)
        {
	        NewtCreateStatementBuilder createStatement = new NewtCreateStatementBuilder();
	        createStatement.setTable( "Newt", actorTable );
	        try 
	        {
	            while( rs.next() ) 
	            {
	                createStatement.addColumn( rs.getString( 1 ), rs.getString( 4 ), false );
	                int indexLen = rs.getInt( 5 );
	                if( indexLen > 0 ) 
	                {
	                    createStatement.addIndex( rs.getString( 1 ), rs.getInt( 5 ) );
	                }
	            }
	        } 
	        catch( Exception e ) 
	        {
	            e.printStackTrace();
	        }
	        remoteUpdate = createStatement.getQuery();
	        createStatement.reset();
	        createStatement.close();
	        
	        if( remoteUpdate != null ) {
	            actorUrl = sendRemoteUpdate( remoteUpdate, actorTable, peerIP, isSubActor );
	        }
        }
        else if(isSubActor)
        {
	        //If subActor, create three tables
//        	StringBuilder createTable = new StringBuilder();
//	        createTable.append("Create table if not exists " + "Newt."+ actorTable+"_input " + " ( Input varbinary(16), Time bigint );" );
//	        createTable.append("Create table if not exists " + "Newt."+ actorTable+"_output " + " (Output varbinary(16), Time bigint )" );
	        //FIXME add reset
	        
//	        remoteUpdate = createTable.toString();
//        	remoteUpdate = "";
        	actorUrl = sendRemoteUpdate( "", String.valueOf(aid), peerIP, isSubActor );
        }
        selectStatement.close();

        NewtUpdateStatementBuilder updateStatement = new NewtUpdateStatementBuilder();
        if(isSubActor)
        {
        	updateStatement.setTable( "Newt", "subActorInstances" );
        }
        else	
        {
        	updateStatement.setTable( "Newt", "actorInstances" );
        }
        updateStatement.addCondition( "ActorID", aid );
        updateStatement.addColumn( "ActorTable", actorTable );
        updateStatement.addColumn( "ActorUrl", actorUrl );
        updateStatement.setQuery( false );
        updateStatement.execute();
        updateStatement.close();

        return actorUrl;
    }

    public synchronized String sendRemoteUpdate( String update, String filename, byte[] peerIP, boolean isSubActor )
    {
        String url = nextUrl(peerIP);
        try {
            RpcCommunication.getService( url ).createProvTable( update, filename, isSubActor );
        } catch( Exception e ) {
           System.out.println( e.getMessage() );
            e.printStackTrace();
        }
        return url;
    }
        
    /**
     * The given peerIP is the localhost IP address of the client. 
     * Look in the slaves if the same IP can be found. 
     * If not, assign as the peer a randomly chosen one from the slaves.
     * 
     * @param clientLocalIP The localhost IP of the client provided as a bytes[] converted to string
     * 
     * @return
     */
    public synchronized String nextUrl(byte[] clientLocalIP)
    {
         
         if (Configuration.localTablePlacement) 
         {
        	 for (String slave : slaves) 
        	 {
        		 URL slaveURL = null;
        		 try 
        		 {
        			 slaveURL = new URL(slave);
        		 } 
        		 catch (MalformedURLException e) 
        		 {
        			// System.out.println("Newt: Slave URL is malformed:"+slave);
        			 e.printStackTrace();
        		 }
        		 if (slaveURL!=null) 
        		 {
        			 byte[] slaveIP = null;
        			 try 
        			 {
        				 slaveIP = InetAddress.getByName(slaveURL.getHost()).getAddress();
        			 } 
        			 catch (UnknownHostException e) 
        			 {
        				// System.out.println("Newt: could not find IP for slave:"+slaveURL.getHost());
        				 e.printStackTrace();
        			 }
			
        			 if (Arrays.equals(slaveIP , clientLocalIP)) 
        			 {
        				 return slave;
        			 }
        		 }
	        	  
        	 }
         }
        
        // If no local peer can be found, we fall back to the default
        // policy.
        
        // XXX The default behavior currently is round-robin assignment.
        String url = slaves.get( nextSlave );
        nextSlave = ( nextSlave + 1 ) % slaves.size();

        return url;
    }
    
    public synchronized void addFileLocatable( int actorID, String locatable, boolean isInput, boolean isGhostable )
    {
        NewtInsertStatementBuilder insertStatement = new NewtInsertStatementBuilder();
        insertStatement.setTable( "Newt", "dataInstances" );
        insertStatement.addValue( actorID );
        insertStatement.addValue( locatable );
        if( isInput ) {
            insertStatement.addValue( "Read" );
        } else {
            insertStatement.addValue( "Write" );
        }
        insertStatement.addValue( (isGhostable ? "True" : "False") );
        insertStatement.setQuery();
        insertStatement.execute();
        insertStatement.close();
    }

    public void addProvenance(String tname, ProvenanceContainer provenance )
    {
        synchronized( provenanceData ) {
            provenanceData.add( new ProvenanceTask(provenance, tname, false ) );
            provenanceData.notifyAll();
        }
    }

    public void writeProvenance()
    {
        ProvenanceTask provenance;
        
        synchronized( provenanceData ) {
            while( provenanceData.size() == 0 ) {
                try {
                    provenanceData.wait();
                } catch( Exception e ) {
                }
            }

            provenance = provenanceData.get( 0 );
            provenanceData.remove( provenance );
        }

        try {
            if( provenance.isCommitTask() ) {
                synchronized( provenance ) {
                    provenance.setCommitted();
                    provenance.notifyAll();
                }
                return;
            }
            
            List<OutputStream> stream_list = null;
            synchronized( ProvenanceFiles ) {
            	Integer parentID = RpcCommunication.getService(master).getParentOfSubActorByTable(provenance.getTableName());
            	//System.out.println("In writeProvenance, parentID = " + parentID);
            	if(parentID != -1)
            	{
            		//Called from subActor
            		stream_list = ProvenanceFiles.get( parentID+"_subActors" );
            	}
            	else
            	{
            		stream_list = ProvenanceFiles.get( provenance.getTableName() );
            	}
            }
       
            assert(stream_list != null);
            ProvenanceContainer pContainer = provenance.getProvenance();
            for( int i = 0; i < pContainer.size(); i++ ) 
            {
            	if(stream_list.size() == 1)
            	{
            		pContainer.get( i ).writeExternal((ObjectOutputStream) stream_list.get(0) );
            	}
            	else if(stream_list.size() == 2)
            	{
            		
            		if(pContainer.get(i).getType() == ProvenanceItem.INPUT)
            		{
            			writeInSubActorFile((BufferedOutputStream)stream_list.get(0), pContainer.get(i));
            		}
            		else if(pContainer.get(i).getType() == ProvenanceItem.OUTPUT)
            		{
            			writeInSubActorFile((BufferedOutputStream)stream_list.get(1), pContainer.get(i));
            		}
//            		else if(pContainer.get(i).getType() == ProvenanceItem.RESET)
//            		{
//            			out = new BufferedOutputStream( new FileOutputStream(provenance.getTableName()+"_input",true));
//            			writeInSubActorFile(out, pContainer.get(i));
//            			out.close();
//            		}
//            		else if(pContainer.get(i).getType() == ProvenanceItem.RESET_INPUT)
//            		{
//            			out = new BufferedOutputStream( new FileOutputStream(provenance.getTableName()+"_input",true));
//            			writeInSubActorFile(out, pContainer.get(i));
//            			out.close();
//            		}
//            		else if(pContainer.get(i).getType() == ProvenanceItem.OUTPUT_RESET)
//            		{
//            			out = new BufferedOutputStream( new FileOutputStream(provenance.getTableName()+"_input",true));
//            			writeInSubActorFile(out, pContainer.get(i));
//            			out.close();
//            		}
            		
            	}
                
            }
        } catch( Exception e ) {
//           System.out.println( provenance.getTableName() + " " + e.getMessage() );
            e.printStackTrace();
        }
    }
    
    private synchronized void writeInSubActorFile(BufferedOutputStream out, ProvenanceItem item) throws IOException
    {
    	
        out.write( ByteArray.escapeBackslashes(item.primaryData));        
        out.write(ByteArray.escapeBackslashes(fieldTerminatorBytes));
        
        out.write(ByteArray.escapeBackslashes(Long.toString( item.timeTaken ).getBytes() ));
        
        out.write(ByteArray.escapeBackslashes(lineTerminatorBytes));
    }
    
    /** 
     * Create the log file tablename that will store the provenance items of an actor. 
     * If called from subActor, create three log files: filename_input, filename_output, filename_rest
     * 
     * @param update
     * @param filename
     */
    public void makeProvTable( String update, String filename, boolean isSubActor )
    {
    	
    	if(!update.equals(""))
    	{
    		//System.out.println("NewtState:makeProvTable, update=" + update);
	        NewtSqlStatementBuilder statement = new NewtSqlStatementBuilder( false );
	        statement.addStatement( update );
	        statement.execute();
	        statement.close();
    	}

        try 
        {
        	if(isSubActor)
        	{
        		
        		Integer parentID = RpcCommunication.getService( master ).getParentOfSubActorByID( filename );
        		
        		String parent_file = parentID+"_subActors"; 
        		
        		
        		synchronized( ProvenanceFiles ) 
	            {
	            	if(ProvenanceFiles.get(parent_file) == null)
	            	{
	            		ProvenanceFiles.put(parent_file, new ArrayList<OutputStream>());
	            		BufferedOutputStream b1 = new BufferedOutputStream( new FileOutputStream( tempNewtDirectory + parentID+"_subActors_input" ) ) ;
		        		BufferedOutputStream b2 = new BufferedOutputStream( new FileOutputStream( tempNewtDirectory + parentID+"_subActors_output" ) ) ;
		                ProvenanceFiles.get(parent_file).add(b1);
		                ProvenanceFiles.get(parent_file).add(b2 );
	            	}
	            	
	            }
        		
        		//Create three log files
//        		BufferedOutputStream b1 = new BufferedOutputStream( new FileOutputStream( tempNewtDirectory + filename+"_input" ) ) ;
//        		BufferedOutputStream b2 = new BufferedOutputStream( new FileOutputStream( tempNewtDirectory + filename+"_output" ) ) ;
//        		BufferedOutputStream b3 = new BufferedOutputStream( new FileOutputStream( tempNewtDirectory + filename+"_reset" ) );
//	            synchronized( ProvenanceFiles ) 
//	            {
//	            	if(ProvenanceFiles.get(filename) == null)
//	            	{
//	            		ProvenanceFiles.put(filename, new ArrayList<OutputStream>());
//	            	}
//	                ProvenanceFiles.get(filename).add(b1);
//	                ProvenanceFiles.get(filename).add(b2 );
//	                ProvenanceFiles.get(filename).add(b3 );
//	            }
        	}
        	else
        	{
	            ObjectOutputStream b = new ObjectOutputStream( new GZIPOutputStream( new BufferedOutputStream( new FileOutputStream( tempNewtDirectory + filename ) ) ) );
	            synchronized( ProvenanceFiles ) 
	            {
	                ProvenanceFiles.put( filename, new ArrayList<OutputStream>());
	                ProvenanceFiles.get(filename).add(b);
	            }
        	}
        } 
        catch( Exception e ) 
        {
            e.printStackTrace();
        }
    }
    
    public NewtMySql queryActorInstance( HashMap<String, Object[]> queryVars, boolean isAnd )
    {
        return queryTable( "Newt", "actorInstances", queryVars, isAnd );
    }
    
    public NewtMySql querySubActorInstance( HashMap<String, Object[]> queryVars, boolean isAnd )
    {
        return queryTable( "Newt", "subActorInstances", queryVars, isAnd );
    }    

    public NewtMySql queryActorGset( HashMap<String, Object[]> queryVars, boolean isAnd )
    {
        return queryTable( "Newt", "actorGset", queryVars, isAnd );
    }

    public String queryPeerUrl( int peerID )
    {
        String url = null;
        
        NewtSelectStatementBuilder statement = new NewtSelectStatementBuilder();
        statement.addTable( "Newt", "peers" );
        statement.addColumn( "PeerUrl" );
        statement.addAbsoluteCondition( "PeerID", peerID );
        statement.setQuery( false );
        ResultSet rs = statement.execute();

        try {
            if( rs.next() ) {
                url = rs.getString( 1 );
            }
        } catch( Exception e ) {
           System.out.println( e.getMessage() );
            e.printStackTrace();
        }

        statement.close();
        return url;
    }

    public int queryPeerID( String url )
    {
        int pid = -1;
        
        NewtSelectStatementBuilder statement = new NewtSelectStatementBuilder();
        statement.addTable( "Newt", "peers" );
        statement.addColumn( "PeerID" );
        statement.addAbsoluteCondition( "PeerUrl", url );
        statement.setQuery( false );
        ResultSet rs = statement.execute();

        try {
            if( rs.next() ) {
                pid = rs.getInt( 1 );
            }
        } catch( Exception e ) {
           System.out.println( e.getMessage() );
            e.printStackTrace();
        }

        statement.close();
        return pid;
    }

    public synchronized NewtMySql queryTable( String dbName, String tableName, HashMap<String, Object[]> queryVars, boolean isAnd )
    {
        NewtSelectStatementBuilder statement = new NewtSelectStatementBuilder();
        statement.addColumn( "*" );
        statement.addTable( dbName, tableName );
        if( queryVars != null && queryVars.size() > 0 ) {
            Set<Map.Entry<String, Object[]>> queryVarEntries = queryVars.entrySet();
            for( Map.Entry<String, Object[]> queryVar: queryVarEntries ) {
                statement.addAbsoluteConditions( queryVar.getKey(), queryVar.getValue() );
            }
        }
        statement.setQuery( isAnd );
        statement.execute();
        return statement.getContainer();
    }

    public void updateBatch( Vector<String> batchUpdate )
    {
        if( batchUpdate == null || batchUpdate.size() == 0 ) {
            return;
        }

        NewtSqlStatementBuilder statement = new NewtSqlStatementBuilder( false );
        statement.addStatements( batchUpdate );
        statement.execute();
        statement.close();
    }

    /**
     * Create a new ProvenanceTask that is a commitTask with table name = filename.
     * Wait until the commit status of this ProvenanceTask becomes true. 
     * It becomes true in method writeProvenance
     * 
     * @param filename The table name of an actor
     */
    public void commitFile(String filename )
    {
        ProvenanceContainer pContainer = new ProvenanceContainer( 0 );
        ProvenanceTask pCommitTask = new ProvenanceTask( pContainer, filename, true );
        synchronized( provenanceData ) {
            provenanceData.add( pCommitTask );
            provenanceData.notifyAll();
        }

        synchronized( pCommitTask ) {
            while( !pCommitTask.committed() ) {
                try {
                    pCommitTask.wait();
                } catch( Exception e ) {
                }
            }
        }
    }

    /**
     * Flush the log file
     * @param filename
     */
    public void finalize( String filename )
    { 
    	String file = filename;
    	
    	Integer parentID = RpcCommunication.getService(master).getActorIdOfParentOfSubActor(filename); 
    	if(parentID != -1)
    	{
    		//System.out.println("Flush log files for parent of subActors: " + filename);
    		file = parentID+"_subActors";    		
    	}
    	else
    	{
    		//System.out.println("Flush log files for actor " + filename);
    	}
		List<OutputStream> stream_list = null;
        synchronized( ProvenanceFiles ) 
        {
            stream_list = ProvenanceFiles.get( file );
            ProvenanceFiles.remove( file );
            if( stream_list != null ) 
            {
                try 
                {
                    for(OutputStream stream: stream_list)
                    {
                    	stream.flush();
                    	stream.close(); 
                    }
                } catch(Exception e ) {
                    e.printStackTrace();
                }
            }
        }
    	
        
        
    }

    public byte[] toAssociation( byte[] input, byte[] output, Long timeTaken )
    {
        byte[] timeTakenInBytes = Long.toString( timeTaken ).getBytes();
        byte[] association = new byte[ input.length + output.length + timeTakenInBytes.length + 2*fieldTerminatorBytes.length + lineTerminatorBytes.length ];
        
        int written = 0;
        System.arraycopy( input, 0, association, written, input.length );
        written += input.length; 
        System.arraycopy( fieldTerminatorBytes, 0, association, written, fieldTerminatorBytes.length );
        written += fieldTerminatorBytes.length; 
        System.arraycopy( output, 0, association, written, output.length );
        written += output.length; 
        System.arraycopy( fieldTerminatorBytes, 0, association, written, fieldTerminatorBytes.length );
        written += fieldTerminatorBytes.length; 
        System.arraycopy( timeTakenInBytes, 0, association, written, timeTakenInBytes.length );
        written += timeTakenInBytes.length; 
        System.arraycopy( lineTerminatorBytes, 0, association, written, lineTerminatorBytes.length );
        return association;
    }
    
    /**
     * Copy the contents of the old file (tablename) into the new file (tablename_final) 
     * @param oldFilename
     * @param newFilename
     */
    public void formatProvenanceFile( String oldFilename, String newFilename )
    {
        ObjectInputStream i = null;
        BufferedOutputStream o = null;

        try {
            i = new ObjectInputStream( new GZIPInputStream (new BufferedInputStream( new FileInputStream( tempNewtDirectory + oldFilename ) ) ) );
            o = new BufferedOutputStream( new FileOutputStream( tempNewtDirectory + newFilename ) );
            HashMap<String, ArrayList<byte[]>> inputBuffer = new HashMap<String, ArrayList<byte[]>>();
            ProvenanceItem pItem = new ProvenanceItem();
            while( true ) {
                pItem.readExternal( i );
                if( pItem.itemType == ProvenanceItem.ASSOCIATION ) {
                    o.write( ByteArray.escapeBackslashes( toAssociation( pItem.primaryData, pItem.tagOrSecondaryData, pItem.timeTaken ) ) );
                } else {
                    String tag = new String( pItem.tagOrSecondaryData );
                    ArrayList<byte[]> inputs = inputBuffer.get( tag );
                    if( inputs == null ) {
                        inputs = new ArrayList<byte[]>();
                        inputBuffer.put( tag, inputs );
                    }
                        
                    if( pItem.itemType == ProvenanceItem.RESET_INPUT ) {
                        inputs.clear();
                        inputs.add( pItem.primaryData );
                    } else if( pItem.itemType == ProvenanceItem.INPUT ) {
                        inputs.add( pItem.primaryData );
                    } else if( pItem.itemType == ProvenanceItem.OUTPUT_RESET ) {
                        for( byte[] input: inputs ) {
                            o.write( ByteArray.escapeBackslashes( toAssociation( input, pItem.primaryData, pItem.timeTaken ) ) );
                        }
                        inputs.clear();
                    } else if( pItem.itemType == ProvenanceItem.OUTPUT ) {
                        for( byte[] input: inputs ) {
                            o.write( ByteArray.escapeBackslashes( toAssociation( input, pItem.primaryData, pItem.timeTaken ) ) );
                        }
                    } else if( pItem.itemType == ProvenanceItem.RESET ) {
                        inputs.clear();
                    }
                }
            }
        } catch( EOFException e ) {
            try {
                o.close();
                i.close();
            } catch( Exception ex ) {
                System.err.println( ex.getMessage() );
                ex.printStackTrace();
            }
        } catch( Exception e ) {
            System.err.println( e.getMessage() );
            e.printStackTrace();
        }

        try {
            File f = new File( tempNewtDirectory + oldFilename );
            f.delete();
        } catch( Exception e ) {
            e.printStackTrace();
        }
    }
    
   
    public void loadDataFromSubActors(Integer actorID, String actorTable) 
    {
    	try {
    		BufferedOutputStream out = new BufferedOutputStream( new FileOutputStream( tempNewtDirectory+actorID+"_merged") );
    		
	    	//Sort files
	        Comparator<String> file_comparator = new Comparator<String>() 
    		{
	        	public int compare(String s, String other)
	        	{
	        		String[] fields_this = s.split(NewtState.fieldTerminator_ESCAPED);
	        		String[] fields_other = other.split(NewtState.fieldTerminator_ESCAPED);
	        		int this_index = fields_this[1].indexOf("##||||##");
	        		int other_index = fields_other[1].indexOf("##||||##");
	        		return Long.valueOf(new String(fields_this[1].substring(0,this_index))).compareTo(
	        				Long.valueOf(new String(fields_other[1].substring(0,other_index))));
	        		
	        	}
            };
            
//	    	List<File> temp_input = ExternalSort.sortInBatch(new File(tempNewtDirectory + actorTable + "_subActors_input"), file_comparator);
//	    	ExternalSort.mergeSortedFiles(temp_input, new File(tempNewtDirectory + actorTable + "_input_SORTED"), file_comparator);
//	    	
//	    	List<File> temp_output = ExternalSort.sortInBatch(new File(tempNewtDirectory + actorTable + "_subActors_output"), file_comparator);
//	    	ExternalSort.mergeSortedFiles(temp_output, new File(tempNewtDirectory + actorTable + "_output_SORTED"), file_comparator);
	    	

			// Merge files into final output by creating input-output associations
	    	String line = null;
	    	
	    	BufferedReader in_output = new BufferedReader(new FileReader(new File(tempNewtDirectory + actorID + "_subActors_output")));
	    	BufferedReader in_input = new BufferedReader(new FileReader(new File(tempNewtDirectory + actorID + "_subActors_input")));
	    	
	    	List<String> output = new ArrayList<String>();
	    	while((line = in_output.readLine()) != null )
	    	{
	    		output.add(line);
	    		System.out.println("NewtState::out::" + line);
	    	}
	    	Collections.sort(output,file_comparator);
	    	
	    	
	    	List<String> input = new ArrayList<String>();
	    	while((line = in_input.readLine()) != null )
	    	{
	    		input.add(line);
	    		System.out.println("NewtState::in::" + line);
	    	}
	    	Collections.sort(input,file_comparator);	    	
			in_input.close();
			in_output.close();
			
	    	for(String o: output )
	    	{
	    		String o_time = o.split(NewtState.fieldTerminator_ESCAPED)[1];
	    		int index = o_time.indexOf("##||||##");
	    		long out_time = Long.valueOf(o_time.substring(0,index));
	    		for(String i: input)
		    	{
	    			String i_time = i.split(NewtState.fieldTerminator_ESCAPED)[1];
	    			index = i_time.indexOf("##||||##");
	    			long in_time = Long.valueOf(i_time.substring(0,index));
		    		if(out_time >= in_time)
		    		{
		    			out.write(i.split(NewtState.fieldTerminator_ESCAPED)[0].getBytes());
						out.write(fieldTerminatorBytes);
						out.write(o.split(NewtState.fieldTerminator_ESCAPED)[0].getBytes());
						out.write(fieldTerminatorBytes);
						out.write(Long.toString( out_time ).getBytes());
						out.write(lineTerminatorBytes);	
		    		}
	    		}
	    		in_input.close();
	    	}
			

			out.close();
			
			// Load from file to database
			final String update = "load data infile '" + tempNewtDirectory + actorID + "_merged' into table Newt." + actorTable  
                                  + " fields terminated by '" + fieldTerminator + "' lines terminated by '" + lineTerminator + "'";
			try {
				Vector<String> batch = new Vector<String>();
				batch.add(update);
                updateBatch( batch );
            } catch( Exception e ) {
                e.printStackTrace();
            }

            // Zhaomo TEMP!!!!!!!!!!!!!!!!!!!!!
            // commitComplete( actorTable, false );            
           //System.out.println( "Finished populating in loadDataFromSubactors: " + actorTable );
			
    	}
    	catch(Exception e) {
    		e.printStackTrace();
    	}
    	
    }



    private void fetchAllTuples(Map<String,String> subActorsToTables, BufferedWriter out , int type ) throws IOException  
    {
		try 
		{
		
			StringBuilder query;
			
			//Select all inputs
			for(String table: subActorsToTables.keySet()) 
			{
				query = new StringBuilder();
				
				String peerUrl = subActorsToTables.get(table);
				NewtMySql peerMySql = new NewtMySql( Configuration.mysqlUser, Configuration.mysqlPassword, peerUrl,"Newt" ); //<----------------------------- Needed for running locally
				
				Statement stmt = peerMySql.getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY,
																   ResultSet.CONCUR_READ_ONLY);
				stmt.setFetchSize(100);
				
				if (type == 1) 
				{
					query.append("Select * from " + table + "_input ORDER BY Time " + " ; ");
				}
				else if(type == 2)
				{
					query.append("Select * from " + table + "_output ORDER BY Time " + " ; ");
				}

				
				stmt.execute( query.toString());
				ResultSet rs = stmt.getResultSet();
				while(rs.next()) 
				{
					byte[] input = rs.getBytes(1);
					long timestamp = rs.getLong(2);
					
					out.write(String.valueOf(timestamp));
					out.write(fieldTerminator);
					out.write(new String(input));
					out.write("\n");
				}
				stmt.close();
			}
			out.close();
		}
		catch(SQLException e) 
		{
			e.printStackTrace();
		}
	}    
    

    /**
     * Checks if the actor with the given table is the parent of subActor.
     * The check involves querying the table subActorInstances.
     * 
     * @param table
     * @return true if the actor with table is a parent of subActors, false otherwise.
     */
    public List isParentOfSubActors(String table)
    {
    	return RpcCommunication.getService( master ).isParentOfSubActors( table );
    }  
    
    
    public void commitComplete( String table, boolean isSubActor )
    {
//        try {
//            File f = new File( tempNewtDirectory + filename );
//            f.delete();
//        } catch( Exception e ) {
//            e.printStackTrace();
//        }
       
        try {
            RpcCommunication.getService( master ).finalizeCommit( table, isSubActor );
        } catch( Exception e ) {
           System.out.println( e.getMessage() );
            e.printStackTrace();
        }
    }

    /**
     * Set column Committed= True or Ghosting for the actor with the given table name
     * Select the actorID with the given table name. The table name is unique for all actors
     * 
     * @param table
     */
    public synchronized void finalizeCommit( String table , boolean isSubActor)
    {
        String commitStatus = "True";
        
        NewtSelectStatementBuilder selectStatement = new NewtSelectStatementBuilder();
        if(isSubActor)
        {
        	selectStatement.addTable( "Newt", "subActorInstances" );
        	selectStatement.addAbsoluteCondition( "subActorInstances.ActorTable", table );
        }
        else
        {
        	selectStatement.addTable( "Newt", "actorInstances" );
        	selectStatement.addAbsoluteCondition( "actorInstances.ActorTable", table );
        }
        selectStatement.addColumn( "ActorID" );
        
        selectStatement.setQuery( false );
        ResultSet rs = selectStatement.execute();
        int aid = -1;
        
        try {
            rs.next();
            aid = rs.getInt( 1 );
        } catch( Exception e ) {
           System.out.println( e.getMessage() );
            e.printStackTrace();
        }

        // Vicky: Don't know what the following code is doing. Hopefully not needed for graphlab
//        if( aid != -1 ) {
//        	
////        	System.out.println("Bummer! Called from subActor");
//            selectStatement.addColumn( "Locatable" );
//            selectStatement.addTable( "Newt", "dataInstances" );
//            selectStatement.addAbsoluteCondition( "ActorID", aid );
//            selectStatement.addAbsoluteCondition( "ReadOrWrite", "Read" );
//            selectStatement.addAbsoluteCondition( "Ghostable", "True" );
//            selectStatement.setQuery( true );
//            rs = selectStatement.execute();
//            String locatable = null;
//
//            try {
//                if( rs.next() ) {
//                    locatable = rs.getString( 1 );
//                }
//            } catch( Exception e ) {
//               System.out.println( e.getMessage() );
//                e.printStackTrace();
//            }
//
//            if( locatable != null ) {
//                selectStatement.addColumn( "ActorID" );
//                selectStatement.addTable( "Newt", "dataInstances" );
//                selectStatement.addAbsoluteCondition( "ReadOrWrite", "Write" );
//                selectStatement.addAbsoluteCondition( "Ghostable", "True" );
//                selectStatement.addAbsoluteCondition( "Locatable", locatable );
//                selectStatement.setQuery( true );
//                rs = selectStatement.execute();
//                int ghostAid = -1;
//
//                try {
//                    if( rs.next() ) {
//                        ghostAid = rs.getInt( 1 );
//                    }
//                } catch( Exception e ) {
//                   System.out.println( e.getMessage() );
//                    e.printStackTrace();
//                }
//
//                if( ghostAid != -1 ) {
//                   System.out.println( "Need to ghost reader: " + aid + " with writer: " + ghostAid );
//                    selectStatement.addTable( "Newt", "actorInstances" );
//                    selectStatement.addColumn( "ActorID" );
//                    selectStatement.addColumn( "ActorUrl" );
//                    selectStatement.addColumn( "ActorTable" );
//                    selectStatement.addAbsoluteCondition( "ActorID", ghostAid );
//                    selectStatement.addAbsoluteCondition( "ActorID", aid );
//                    selectStatement.setQuery( false );
//                    rs = selectStatement.execute();
//                    String ghostSourceUrl = null;
//                    String ghostSourceTable = null;
//                    String ghostDestinationUrl = null;
//                    String ghostDestinationTable = null;
//
//                    try {
//                        while( rs.next() ) {
//                            if( rs.getInt( 1 ) == aid ) {
//                                ghostDestinationUrl = rs.getString( 2 );
//                                ghostDestinationTable = rs.getString( 3 );
//                            } else {
//                                ghostSourceUrl = rs.getString( 2 );
//                                ghostSourceTable = rs.getString( 3 );
//                            }
//                        }
//                    } catch( Exception e ) {
//                       System.out.println( e.getMessage() );
//                        e.printStackTrace();
//                    }
//
//                    if( ghostDestinationUrl.equals( ghostSourceUrl ) ) {
//                        ghostDestinationUrl = "self";
//                    }
//
//                    try {
//                        RpcCommunication.getService( ghostSourceUrl ).sendGhostData( ghostSourceTable, ghostDestinationUrl, ghostDestinationTable );
//                        commitStatus = "Ghosting";
//                    } catch( Exception e ) {
//                       System.out.println( e.getMessage() );
//                        e.printStackTrace();
//                    }
//                }
//            }
//        }
        
        selectStatement.close();

        NewtUpdateStatementBuilder updateStatement = new NewtUpdateStatementBuilder();
        if(isSubActor)
        {
        	updateStatement.setTable( "Newt", "subActorInstances" );
        }
        else
        {
        	updateStatement.setTable( "Newt", "actorInstances" );
        }
        
        updateStatement.addColumn( "Committed", commitStatus );
        updateStatement.addCondition( "ActorTable", table );
        updateStatement.setQuery( false );
        updateStatement.execute();
        updateStatement.close();

        if( commitStatus.equals( "True" ) ) {
           //System.out.println( "Commit completed for: " + table );
        } else {
           System.out.println( "Ghosting for: " + table );
        }
    }

    public synchronized void ghost( Vector<String> ghostData, Vector<String> ghostMatch, String ghostSourceTable, String ghostDestinationTable )
    {
        NewtGhost ghoster = new NewtGhost( ghostData, ghostMatch, ghostSourceTable, ghostDestinationTable, this );
        ghoster.start();
    }

    public synchronized void sendGhostComplete( String sourceTable, String destinationTable, String ghostTable )
    {
        try {
            RpcCommunication.getService( master ).ghostComplete( sourceTable, destinationTable, ghostTable );
        } catch( Exception e ) {
           System.out.println( e.getMessage() );
            e.printStackTrace();
        }
    }

    /**
     * An actor can commit only when all its subActors have committed.
     * Need to implement this check.
     * 
     * @param actorID
     * @param isSubActor
     */
    public synchronized void commitActorInstance( int actorID, boolean isSubActor )
    {

    	//Delete all files of children actors whose commit status is empty
        NewtSelectStatementBuilder selectStatement = new NewtSelectStatementBuilder();
        selectStatement.addColumns( new String[] { "ActorTable", "ActorUrl" } );
        if(isSubActor)
        {
        	selectStatement.addTable( "Newt", "subActorInstances" );
        }
        else
        {
        	selectStatement.addTable( "Newt", "actorInstances" );
        }
        selectStatement.addAbsoluteCondition( "ParentID", actorID );
        selectStatement.addAbsoluteCondition( "Committed", "" );
        selectStatement.setQuery( true );
        ResultSet rs = selectStatement.execute();
        HashMap<String, String> cleanupFiles = new HashMap<String, String>();
        try {
            while( rs.next() ) {
                cleanupFiles.put( rs.getString( 1 ), rs.getString( 2 ) );
            }
        } catch( Exception e ) {
            e.printStackTrace();
        }

        // Update table actorInstances. Update the row of actorID and set the column Committed = Populating
        NewtSqlStatementBuilder statement = new NewtSqlStatementBuilder();
        NewtUpdateStatementBuilder updateStatement = new NewtUpdateStatementBuilder();
        if(isSubActor)
        {
        	updateStatement.setTable( "Newt", "subActorInstances" );
        }
        else
        {
        	updateStatement.setTable( "Newt", "actorInstances" );
        }
        updateStatement.addColumn( "Committed", "Populating" );
        updateStatement.addCondition( "ActorID", actorID );
        updateStatement.setQuery( false, statement );
        
        //Delete from table actorInstances. Delete the rows with ParentID=actorID and Committed=""
        NewtDeleteStatementBuilder deleteStatement = new NewtDeleteStatementBuilder();
        if(isSubActor)
        {
        	deleteStatement.setTable( "Newt", "subActorInstances" );
        }
        else
        {
        	deleteStatement.setTable( "Newt", "actorInstances" );
        }
        deleteStatement.addCondition( "ParentID", actorID );
        deleteStatement.addCondition( "Committed", "" );
        deleteStatement.setQuery( true, statement );
        statement.execute();
        statement.close();

        // Select from tables actorInstances and actorGset the actorURL and the actor table name.
        if(isSubActor)
        {
        	selectStatement.addColumns( new String[] { "subActorInstances.ActorUrl", "subActorInstances.ActorTable" } );
        	selectStatement.addTable( "Newt", "subActorInstances" );
        	selectStatement.addAbsoluteCondition( "subActorInstances", "ActorID", actorID );
        }
        else
        {
        	selectStatement.addColumns( new String[] { "actorInstances.ActorUrl", "actorInstances.ActorTable" } );
        	selectStatement.addTable( "Newt", "actorInstances" );
        	selectStatement.addAbsoluteCondition( "actorInstances", "ActorID", actorID );
        }
        
        // FIXME Why do we need the join with the actorGset? <--------------------
//        selectStatement.addTable( "Newt", "actorGset" );
//        selectStatement.addJoinCondition( "actorGset", "ActorType", "actorInstances", "ActorType" );
        selectStatement.setQuery( true );
        rs = selectStatement.execute();
        String actorUrl = null;
        String actorTable = null;
        try {
            rs.next();
            actorUrl = rs.getString( 1 );
            actorTable = rs.getString( 2 );
        } catch( Exception e ) {
            e.printStackTrace();
        }
        selectStatement.close();

        sendRemoteCleanup( cleanupFiles );
        sendRemoteLoadData( actorTable, actorUrl, isSubActor );
    }

    public void sendRemoteLoadData( String actorTable, String actorUrl, boolean isSubActor )
    {
        try {
            RpcCommunication.getService( actorUrl ).loadData( actorTable, isSubActor );
        } catch( Exception e ) {
           System.out.println( e.getMessage() );
            e.printStackTrace();
        }
    }

    public synchronized void sendRemoteCleanup( HashMap<String, String> cleanupFiles )
    {
        Set entries = cleanupFiles.entrySet();
        for( int i = 0; i < slaves.size(); i++ ) {
            String url = slaves.get( i );
            Vector<String> tables = new Vector<String>();

            for( Object o: entries ) {
                Map.Entry e = (Map.Entry) o;
                if( url.equals( (String) e.getValue() ) ) {
                    tables.add( (String) e.getKey() );
                }
            }

            if( tables.size() > 0 ) {
                try {
                    RpcCommunication.getService( url ).cleanFiles( tables );
                } catch( Exception e ) {
                   System.out.println( e.getMessage() );
                    e.printStackTrace();
                }
            }
        }
    }

    public void cleanFiles( Vector<String> files )
    {
        NewtSqlStatementBuilder statement = new NewtSqlStatementBuilder( false );
        for( Object f: files )
        {
            String file = (String) f;
            finalize( file );

            try {
                boolean success = (new File( tempNewtDirectory + file )).delete();
                if( success ) {
                   System.out.println( "Successfully deleted uncommitted file: " + file );
                } else {
                   System.out.println( "Failed to delete uncommitted file: " + file );
                }
                statement.addStatement( "drop table if exists Newt." + file );
            } catch( Exception e ) {
                e.printStackTrace();
            }
        }

        statement.execute();
        statement.close();
    }

    public synchronized int registerTrace( int tid, String status, int caid, String tableName, Vector<String> completedNodes )
    {
        NewtInsertStatementBuilder statement = new NewtInsertStatementBuilder();
        statement.setTable( "Trace", "traceActors" );
        statement.addValues( new Object[] { tid, caid, status, tableName } );
        statement.setQuery();
        statement.execute();
        statement.close();

        if( !status.equals( "Incorrect" ) ) {
            completedTraces.put( tid, completedNodes );
        }

        return slaves.size();
    }

    public synchronized void sendTrace( int tid, 
                                        HashMap<String, HashMap<String, Vector<Vector>>> traceQuery,
                                        HashMap<String, Vector<String>> tableSources,
                                        Vector data,
                                        Vector locatableData )
    {
        for( int i = 0; i < slaves.size(); i++ ) {
            try {
                RpcCommunication.getService( slaves.get( i ) ).traceLocal( tid, traceQuery, tableSources, data, locatableData );
            } catch( Exception e ) {
               System.out.println( e.getMessage() );
                e.printStackTrace();
            }
        }
    }

    public int traceLocal( int tid,
                         HashMap<String, HashMap<String, Vector<Vector>>> traceQuery,
                         HashMap<String, Vector<String>> tableSources,
                         Vector data,
                         Vector locatableData )
    {
        synchronized( traceResults ) {
            if( traceResults.get( tid ) == null ) {
                HashMap<String, String[]> traceInstanceResults = new HashMap<String, String[]>();
                traceResults.put( tid, traceInstanceResults );
            }
        }

        NewtTrace newtTrace = new NewtTrace( tid, data, locatableData, traceQuery, tableSources, this, selfUrl );
        synchronized( traceInstances ) {
            traceInstances.put( tid, newtTrace );
        }

        try {
            newtTrace.start();
        } catch( Exception e ) {
           System.out.println( e.getMessage() );
            e.printStackTrace();
        }

        return tid;
    }

    public HashMap<String, String[]> getTraceResultsMap( int traceID )
    {
        HashMap<String, String[]> traceInstanceResults = null;
        synchronized( traceResults ) {
            traceInstanceResults = traceResults.get( traceID );
        }

        return traceInstanceResults;
    }

    public synchronized void forwardTraceResults( Vector<String> targets, 
                                                  int tid, 
                                                  String[] resultTable,
                                                  Vector results )
    {
        for( String url: targets ) {
            try {
                RpcCommunication.getService( url ).continueTrace( tid, (String)resultTable[ 0 ], (String)resultTable[ 1 ], results  );
               System.out.println( "Forwarded result: (" + tid + ", " + resultTable[ 0 ] + ", " + resultTable[ 1 ] + ") to " + url );
            } catch( Exception e ) {
               System.out.println( e.getMessage() );
                e.printStackTrace();
            }
        }
    }

    public synchronized void continueTrace( int tid, String resultTable, String resultDataType, Vector results )
    {
        try {
       System.out.println( "Continuing: " + tid + ", " + resultTable + ", " + resultDataType );
        NewtCreateStatementBuilder createStatement = new NewtCreateStatementBuilder();
        createStatement.setTable( "Trace", resultTable );
        createStatement.addColumn( "Result", resultDataType, true );
        createStatement.setQuery();
        createStatement.execute();
        createStatement.close();
        
        NewtBulkInsertStatementBuilder bulkInsertStatement = new NewtBulkInsertStatementBuilder();
        bulkInsertStatement.setTable( "Trace", resultTable );
        bulkInsertStatement.setRowLength( 1 );
        for( Object o: results ) {
            bulkInsertStatement.addRow( new Object[] { o } );
        }
        bulkInsertStatement.setQuery();
        bulkInsertStatement.execute();
        bulkInsertStatement.close();
        
        HashMap<String, String[]> traceInstanceResults = null;
        synchronized( traceResults ) {
            if( traceResults.get( tid ) == null ) {
                traceInstanceResults = new HashMap<String, String[]>();
                traceResults.put( tid, traceInstanceResults );
            } else {
                traceInstanceResults = traceResults.get( tid );
            }
        }

        String[] splits = resultTable.split( "\\$" );
        if( splits.length != 3 ) {
           System.out.println( "Table name not found. Discarding result..." );
        } else {
            String sourceTable = splits[ 1 ];
            NewtTrace newtTrace = null; 
            synchronized( traceInstances ) {
                newtTrace = traceInstances.get( tid );
            }
            synchronized( traceInstanceResults ) {
                traceInstanceResults.put( sourceTable, new String[] { "Trace." + resultTable, resultDataType } );
            }
           System.out.println( "Recieved trace results for traceID: " + tid + " table: " + resultTable );
        }
        }catch( Exception e ) {
           System.out.println( e.getMessage() );
            e.printStackTrace();
        }
    }

    public void notifyTraceComplete( int tid )
    {
        try {
            RpcCommunication.getService( master ).traceComplete( tid, selfUrl );
        } catch( Exception e ) {
           System.out.println( e.getMessage() );
            e.printStackTrace();
        }
    }

    public synchronized void traceComplete( int tid, String slave )
    {
       System.out.println( "Trace: " + tid + " completed on: " + slave );
        Vector<String> completedNodes = completedTraces.get( tid );
        synchronized( completedNodes ) {
            completedNodes.add( slave );
            if( completedNodes.containsAll( slaves ) ) {
                NewtUpdateStatementBuilder statement = new NewtUpdateStatementBuilder();
                statement.setTable( "Trace", "traceActors" );
                statement.addColumn( "Status", "Complete" );
                statement.addCondition( "TraceID", tid );
                statement.setQuery( false );
                statement.execute();
                statement.close();

               System.out.println( "Trace: " + tid + " complete" );
                completedNodes.notifyAll();
            }
        }
    }

    public synchronized HashSet getReplayFilter( int tid, String tableName )
    {
        HashMap<String, String[]> traceInstanceResults = null; 
        synchronized( traceResults ) {
            traceInstanceResults = traceResults.get( tid );
        }

        if( traceInstanceResults == null ) {
            return null;
        }

        String traceResultTable = null;
        String traceResultDataType = null;
        synchronized( traceInstanceResults ) {
            traceResultTable = traceInstanceResults.get( tableName )[ 0 ];
            traceResultDataType = traceInstanceResults.get( tableName )[ 1 ];
        }

        if( traceResultTable == null ) {
            return null;
        }

        NewtSelectStatementBuilder statement = new NewtSelectStatementBuilder();
        statement.addTable( traceResultTable );
        statement.addColumn( "Result" );
        statement.setQuery( false );
        ResultSet rs = statement.execute();

        HashSet filter = new HashSet();
        int count = 0;
        try {
            if( traceResultDataType.startsWith( "varchar" ) ) {
                while( rs.next() ) {
                    filter.add( rs.getString( 1 ) );
                    count++;
                }
            } else if( traceResultDataType.startsWith( "varbinary" ) ) {
                while( rs.next() ) {
                    filter.add( new ByteArray( rs.getBytes( 1 ) ) );
                    count++;
                }
            }
        } catch( Exception e ) {
            e.printStackTrace();
            return null;
        }

        statement.close();
        System.err.println( "Total replay records: " + count );
        return filter;
    }
}
