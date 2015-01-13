/*
 * Created on Apr 28, 2011
 *
 */
package newt.client.query;

public class BasicProvenanceQuery implements ReplayProvenanceQuery {

    public BasicProvenanceQuery() {}
    
    public ProvenanceContainer query(String actorID, String replayID) throws Exception {
	
	ProvenanceContainer pContainer = new SetContainer();
	
	// XXX DEN: The container should be created by querying the Newt server.
//	RecordSpecification r = new HDFSFileLocation("hdfs://compute-2-9:54310/input/file", 0, 0, 8); 
	String loc = "hdfs://compute-2-9:54310/wikidata/big/wikipedia_chunk_01;0;0;39";
	RecordSpecification r1 = 
//	    new HashRecordSpecification("hdfs://compute-2-9:54310/input/file;0;0;8"); 
	    new HashRecordSpecification(loc);
	((SetContainer)pContainer).add(r1);
	System.out.println("DEN: Adding record to container:" + r1+ "   loc:"+loc);
	
	// XXX DEN: The container should be created by querying the Newt server.
//	RecordSpecification r = new KeyValueRecord<String, String>("foo", "1");
	RecordSpecification r2 = new HashRecordSpecification("{foo,1}");
	((SetContainer)pContainer).add(r2);
	System.out.println("DEN: Adding record to container:" + r2);
	return pContainer;
    }

}
