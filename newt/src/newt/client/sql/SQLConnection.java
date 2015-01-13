/*
 * Created on Apr 30, 2011
 *
 */
package newt.client.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLConnection {

    private Connection conn;
    private Statement statement;
    
    public SQLConnection(String host, String DBName, String user, String password) {
	try {
	    conn = DriverManager.getConnection(
		    "jdbc:mysql://"+host+"/" + DBName + "?" + "user=" + user + "&password=");
	    statement = conn.createStatement(
		    ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE );
	} catch( Exception e ) {
	    e.printStackTrace();
	}
    }
    
    public ResultSet executeQuery(String query) {
        //System.out.println( "Query: " + query );
	ResultSet resultSet = null;
        try {
            resultSet = statement.executeQuery( query );
            resultSet.beforeFirst();
        } catch( Exception e ) {
            e.printStackTrace();
        }
        return resultSet;
    }
    
    public void close() throws SQLException {
	conn.close();
    }
}
