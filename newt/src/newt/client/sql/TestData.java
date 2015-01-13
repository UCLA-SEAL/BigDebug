/*
 * Created on Apr 30, 2011
 *
 */
package newt.client.sql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Vector;

import newt.client.NewtClient;
import newt.client.NewtClient.Mode;
import newt.common.Configuration;

public class TestData {

    public static void main(String args[]) throws SQLException {
	
	if (args.length<2) {
	   System.out.println("Usage: java TestData <hostname> <table name>");
	    return;
	}
	
	String hostname = args[0];
	String tableName = args[1];
	
	SQLConnection metaDataServer = 
	    new SQLConnection(hostname, "Newt", Configuration.mysqlUser, Configuration.mysqlPassword);
	
	DataQuery dQuery = new DataQuery(metaDataServer);
	
	ArrayList<DataQuery.DataEntry> c = dQuery.getData(tableName);
	
	System.out.println("Found "+c.size()+" entries in table "+tableName);
	
	DataQuery.DataEntry e = c.get(0);	
	byte[] o = e.getOutput();
	
	NewtClient client = new NewtClient( Mode.CAPTURE);
	int traceID;
	
	Vector<byte[]> traceRecords = new Vector<byte[]>();
	traceRecords.add(o);
	traceID = client.trace(traceRecords, "backward", 10, 8);
	System.out.println("Trace ID:"+traceID);
    }
}
