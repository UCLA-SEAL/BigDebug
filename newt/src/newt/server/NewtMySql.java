package newt.server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Vector;
import java.net.InetAddress;

import org.apache.log4j.Logger;

import newt.common.*;

public class NewtMySql {
    private Connection          connect = null;
    private Statement           statement = null;
    private PreparedStatement   preparedStatement = null;
    private ResultSet           resultSet = null;
    private String              user = null;
    private String              password = null;
    private String              DBName = null;
    
    private static Logger logger = Logger.getLogger(NewtMySql.class);

    public NewtMySql( String user, String password )
    {
        this.user = user;
        this.password = password;
        try {
            Class.forName( "com.mysql.jdbc.Driver" );
            StringBuffer sb = new StringBuffer( 255 );
            sb.append( "jdbc:mysql://localhost/?user=" );
            sb.append( user );
            sb.append( "&password=");
			sb.append( password == null ? "" : password );
            sb.append( "&allowMultiQueries=true" );
			//System.out.println(sb);
            connect = DriverManager.getConnection( sb.toString() );
            statement = connect.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                 ResultSet.CONCUR_UPDATABLE );
        } catch( Exception e ) {
            e.printStackTrace();
        }
    }

    protected NewtMySql( String user, String password, String DBName )
    {
        this.user = user;
        this.password = password;
        this.DBName = DBName;
        try {
            Class.forName( "com.mysql.jdbc.Driver" );
            StringBuffer sb = new StringBuffer( 255 );
            sb.append( "jdbc:mysql://localhost/" );
            sb.append( DBName );
            sb.append( "?user=" );
            sb.append( user );
            sb.append( "&password=" );
            sb.append( password == null ? "" : password );
            sb.append("&allowMultiQueries=true");
            connect = DriverManager.getConnection( sb.toString() );
            statement = connect.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                 ResultSet.CONCUR_UPDATABLE );
        } catch( Exception e ) {
            e.printStackTrace();
        }
    }
    
    protected NewtMySql( String user, String password, String hostname, String DBName )
    {
        this.user = user;
        this.password = password;
        this.DBName = DBName;
        try {
            Class.forName( "com.mysql.jdbc.Driver" );
            StringBuffer sb = new StringBuffer( 255 );
            //sb.append( "jdbc:mysql://169.228.66.101:3306/" );
            String h = (hostname.split(":/*"))[1];
            sb.append( "jdbc:mysql://" + h + ":3306/" ); 
            sb.append( DBName );
            sb.append( "?user=" );
            sb.append( user );
            sb.append( "&password=" );
            sb.append( password == null ? "" : password );
            sb.append("&allowMultiQueries=true");

           //System.out.println("sb = " + sb);

            connect = DriverManager.getConnection( sb.toString() );
            statement = connect.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                 ResultSet.CONCUR_UPDATABLE );
        } catch( Exception e ) {
            e.printStackTrace();
        }
    }    

    public void init( boolean isMaster, String DBName ) 
    {
        this.DBName = DBName;

        if( Configuration.cleanDB ) { 
           System.out.println( "Dropping old Newt database" );

            try {
                statement.executeUpdate( "drop database if exists " + DBName );
            } catch( Exception e ) {
                e.printStackTrace();
            }

            try {
                statement.executeUpdate( "drop database if exists " + "Trace");
            } catch( Exception e ) {
                e.printStackTrace();
            }
        }

       System.out.println( "Initing Newt Database..." );
    
        try {
            statement.executeUpdate( "create database if not exists " + DBName + " character set = 'latin1' collate = 'latin1_general_cs'" );
            statement.executeUpdate( "create database if not exists " + "Trace" + " character set = 'latin1' collate = 'latin1_general_cs'" );
        } catch( Exception e ) {
            e.printStackTrace();
            System.exit( 1 );
        }

        try {
            connect = DriverManager.getConnection( "jdbc:mysql://localhost/" + DBName + "?" + "user=" + user + "&password=" + (password == null ? "" : password) );
            statement = connect.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                 ResultSet.CONCUR_UPDATABLE );
        } catch( Exception e ) {
            e.printStackTrace();
        }

        if( isMaster ) {
            try {
                statement.executeUpdate( "create table if not exists actorGset ( ActorType varchar(255) not null, ParentType varchar(255), IsStatic varchar(10), SchemaName varchar(255), SourceActorType varchar(255), DestinationActorType varchar(255), SchemaID int, SchemaTable varchar(255), primary key (ActorType) )" );
                ResultSet rs = statement.executeQuery( "select * from actorGset where ActorType = 'GHOST'" );
                if( !rs.next() ) {
                    statement.executeUpdate( "insert into actorGset values( 'GHOST', 'MRJob', 'True', 'GHOST_table', 'FileLocatable', 'FileLocatable', 0, 'GHOST_table_logical' )" );
                }
            } catch( Exception e ) {
                e.printStackTrace();
            }
        
            try {
//                statement.executeUpdate( "create table if not exists actorInstances ( ActorID int not null, ActorName varchar(255), ParentID int, ActorType varchar(255), ActorTable varchar(255), ActorPeer int not null , Committed varchar(10), RelativeID varchar(255), SourceActor int, DestinationActor int, primary key (ActorID) )" );
                statement.executeUpdate( "create table if not exists actorInstances ( ActorID int not null, ActorName varchar(255), ParentID int, ActorType varchar(255), ActorTable varchar(255), ActorUrl varchar(255) , Committed varchar(10), RelativeID varchar(255), SourceActor varchar(255), DestinationActor int, primary key (ActorID) )" );
            } catch( Exception e ) {
                e.printStackTrace();
            }

            try {
                statement.executeUpdate( "create table if not exists dataInstances ( ActorID int not null, Locatable varchar(255), ReadOrWrite varchar(10), Ghostable varchar(10) )" );
            } catch( Exception e ) {
                e.printStackTrace();
            }
            
            try {
                statement.executeUpdate( "create table if not exists Trace.traceActors ( TraceID int not null, ActorID int, Status varchar(20), TraceTable varchar(255), primary key (TraceID) )" );
            } catch( Exception e ) {
                e.printStackTrace();
            }

            try {
                statement.executeUpdate( "create table if not exists peers ( PeerID int not null, PeerUrl varchar(255), primary key (PeerID) )" );
            } catch( Exception e ) {
                e.printStackTrace();
            }

            try {
                statement.executeUpdate( "create table if not exists stateIDs ( DummyIndex int not null, actorID int not null, schemaID int not null, traceID int not null, peerID int not null, primary key (DummyIndex) )" );
                ResultSet rs = statement.executeQuery( "select * from stateIDs" );
                if( !rs.next() ) {
                    statement.executeUpdate( "insert into stateIDs values( 0, 0, 1, 0, 0 )" );
                }
            } catch( Exception e ) {
                e.printStackTrace();
            }
            try {
              statement.executeUpdate( "create table if not exists subActorInstances ( ActorID int not null, ActorName varchar(255), ParentID int, ActorType varchar(255), ActorTable varchar(255), ActorUrl varchar(255) , Committed varchar(10), RelativeID varchar(255), SourceActor int, DestinationActor int, primary key (ActorID) )" );
          } catch( Exception e ) {
              e.printStackTrace();
          }
        }
    }

    public ResultSet getResultSet()
    {
        return resultSet;
    }

    public ResultSet executeQuery( String query ) 
    {
        try {
            resultSet = statement.executeQuery( query );
        } catch( Exception e ) {
            logger.error( "Error in executing query: " + query );
            e.printStackTrace();
        }
        return resultSet;
    }

    public ResultSet executeQuery( String query, Vector params ) 
    {
        try {
            preparedStatement = connect.prepareStatement( query );

            int i = 1;
            for( Object o: params ) {
                if( o instanceof String ) {
                    preparedStatement.setString( i++, (String) o );
                } else {
                    preparedStatement.setBytes( i++, (byte[])o );
                }
            }
            resultSet = preparedStatement.executeQuery();
        } catch( Exception e ) {
        	logger.error( "Error in executing query: " + query );
            e.printStackTrace();
        }
        return resultSet;
    }

    public int executeUpdate( String update, Vector params ) 
    {
        int result = -1;
        try {
            preparedStatement = connect.prepareStatement( update );

            int i = 1;
            for( Object o: params ) {
                if( o instanceof String ) {
                    preparedStatement.setString( i++, (String) o );
                } else {
                    preparedStatement.setBytes( i++, (byte[])o );
                }
            }
            preparedStatement.executeUpdate();
        } catch( Exception e ) {
        	logger.error( "Error in executing update: " + update );
            e.printStackTrace();
        }

        return result;
    }

    public void executeBatchUpdate( String update, Vector<Vector> rows )
    {
        try {
            preparedStatement = connect.prepareStatement( update );

            for( Vector params: rows ) {
                int i = 1;
                for( Object o: params ) {
                    if( o instanceof String ) {
                        preparedStatement.setString( i++, (String) o );
                    } else {
                        preparedStatement.setBytes( i++, (byte[])o );
                    }
                }
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        } catch( Exception e ) {
        	logger.error( "Error in executing update: " + update );
            e.printStackTrace();
        }
    }

    public void executeBatchUpdate( Vector<String> updates )
    {
        try {
            for( String u: updates ) {
                statement.addBatch( u );
            }
            statement.executeBatch();
        } catch( Exception e ) {
            System.err.println( "Error in executing batch updates: " );
            for( String u: updates ) {
            	logger.error( u );
            }
            e.printStackTrace();
        }
    }

    public int executeUpdate( String update )
    {
        int result = -1;
        System.out.println("MySQL update: " + update);
        try {
            result = statement.executeUpdate( update );
        } catch( Exception e ) {
        	logger.error( "Error in executing update: " + update );
            e.printStackTrace();
        }

        return result;
    }

    public int executeUpdate( String update, Vector variables, Vector columns )
    {
        int result = -1;
        try {
            preparedStatement = connect.prepareStatement( update );
       
            int i = 1;
            for( Object o: variables ) {
                if( o instanceof String ) {
                    preparedStatement.setString( (Integer)columns.get( i++ ), (String) o );
                } else {
                    preparedStatement.setBytes( (Integer)columns.get( i++ ), (byte[])o );
                }
            }
            result = preparedStatement.executeUpdate();
        } catch( Exception e ) {
        	logger.error( "Error in executing update: " + update );
            e.printStackTrace();
        }

        return result;
    }
            
    private synchronized void writeMetaData( ResultSet resultSet ) throws SQLException {
       System.out.println( "The columns in the table are: " );
       System.out.println( "Table: " + resultSet.getMetaData().getTableName( 1 ) );
        for( int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++ ){
           System.out.println( "Column " + i  + " " + resultSet.getMetaData().getColumnName( i ) );
        }
    }

    private synchronized void writeResultSet( ResultSet resultSet ) throws SQLException {
        while( resultSet.next() ) {
            for( int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++ ) {
                String c = resultSet.getMetaData().getColumnName( i );
                String v = resultSet.getString( c );
               System.out.println( c + ": " + v );
            }
        }
    }

    // Very important! Need to close the resultSet!!
    public synchronized void close() {
        try {
            if( resultSet != null ) {
                resultSet.close();
            }
            if( statement != null ) {
                statement.close();
            }
            if( connect != null ) {
                connect.close();
            }
        } catch( Exception e ) {
        }
    }
    
    public Connection getConnection()
    {
    	return connect;
    }
}

