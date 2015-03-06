/*
 * Created on Apr 30, 2011
 *
 */
package newt.client.sql;

import java.sql.SQLException;
import java.util.Collection;

import newt.common.Configuration;

public class Test {

    public static void main(String args[]) throws SQLException {
	
	if (args.length<1) {
	   System.out.println("Usage: java Test <hostname>");
	    return;
	}
	
	String hostname = args[0];
	
	SQLConnection metaDataServer = 
	    new SQLConnection(hostname, "Newt", Configuration.mysqlUser, Configuration.mysqlPassword);
	
	MetadataQuery mQuery = new MetadataQuery(metaDataServer);
	
	Collection<MetadataQuery.ActorEntry> c = 
	    mQuery.getActors(MRMetaData.ActorType_RW);
	
	for (MetadataQuery.ActorEntry e : c) {
	   System.out.println(e);
	   System.out.println(e.getTableLocationIP());
	}
    }
}
