/*
 * Created on May 1, 2011
 *
 */
package newt.client.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

public class DataQuery {

private SQLConnection metaDataServer;
    
    public DataQuery(SQLConnection metaDataServer) {
	this.metaDataServer = metaDataServer;
    }
    
    public ArrayList<DataEntry> getData(String tableName) throws SQLException {
	ArrayList<DataEntry> list = new ArrayList<DataEntry>();
	String query = "select * from "+tableName;
	ResultSet rs = metaDataServer.executeQuery(query);
	while (rs.next()) {
	    list.add(new DataEntry(
		    rs.getBytes(DataEntry.COL_INPUT), 
		    rs.getBytes(DataEntry.COL_OUTPUT)));
	}
	return list;
    }
    
    public class DataEntry {
	public static final String COL_INPUT = "Input";
	public static final String COL_OUTPUT = "Output";
	
	private byte[] input;
	private byte[] output;
	
	public DataEntry(byte[] input, byte[] output) {
	    this.input = Arrays.copyOf(input, input.length);
	    this.output = Arrays.copyOf(output, output.length);
	}
	
	public byte[] getInput() {
	    return input;
	}
	
	public byte[] getOutput() {
	    return output;
	}
    }
}
