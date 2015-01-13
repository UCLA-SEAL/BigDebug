package newt.test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;

import newt.actor.KeyValuePair;
import newt.client.NewtClient;
import newt.client.NewtClient.Mode;
import newt.client.NewtStageClient;
import newt.common.Configuration;
import newt.server.NewtMySql;

public class VickyTraceTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) 
	{

        String testActorType = "TestActor";
        String testActorSchema = "TestActor_Table";
        String testActorSchemaTable = "TestActor_Logical";
        
        
/*        String testActorType = "PregelActor";
        String testActorSchema = "PregelActor_Table";
        String testActorSchemaTable = "PregelActor_Logical";
*/
        String universeName = "NewtTestUniverse";
        String rootActorName = "NewtTestRoot";
        int universeID = -1;
        int rootActorID = -1;
        int testActorSchemaID = -1;

        String testActorsGset = "<?xml version=\"1.0\"?>\n" +
                                "<Universe name=\"" + universeName + "\">\n" +
                                "\t<RootActor name=\"" + rootActorName + "\">\n" +
                                "\t\t<" + testActorType + " input=\"\" output=\"\"/>" +
                                "\t</RootActor>\n" + 
                                "</Universe>";
        String testActorsSchemas = "<?xml version=\"1.0\"?>\n" +
                                   "<Provenance>\n" +
                                   "\t<Schema name=\"" + testActorSchema + "\" actor=\"" + testActorType + "\">\n" +
                                   "\t\t<Input key=\"true\" type=\"input\" datatype=\"KeyValuePair\" />\n" +
                                   "\t\t<Output type=\"output\" datatype=\"KeyValuePair\" />\n" +
                                   "\t</Schema>\n" +
                                   "</Provenance>";
    
//COMMENT - BEGIN                    
/*        NewtClient newtClient = new NewtClient( Mode.CAPTURE );
        universeID = NewtClient.getUniverseID( universeName );
        rootActorID = newtClient.register( rootActorName, universeID );
        if( universeID == -1 || rootActorID == -1 ) {
           System.out.println( "Error: Unable to register Universe or rootActor. Exiting..." );
            System.exit( 0 );
        }
        
        // Setting up actors containment relationships and provenance tables. 
        int result = newtClient.setProvenanceHierarchy( testActorsGset );
        if( result != 0 ) {
           System.out.println( "Error: Failed to register actor hierarchy. Exiting..." );
            System.exit( 0 );
        }

        result = newtClient.setProvenanceSchemas( testActorsSchemas );
        if( result != 0 ) {
           System.out.println( "Error: Failed to register tables. Exiting..." );
            System.exit( 0 );
        }
  
        testActorSchemaID = NewtClient.getSchemaID( universeID, testActorSchema );
        if( testActorSchemaID == -1 ) {
           System.out.println( "Error: Failed to retrieve schema ID, provenance information may be registered incorrectly. Exiting..." );
            System.exit( 0 );
        }

        // Create parent actor that contains subActors
        NewtClient actor1 = new NewtClient(Mode.CAPTURE,rootActorID,"first", testActorType, "", false);//976//3
        actor1.setSchemaID(testActorSchemaID);
        actor1.setTableName("first_table");
        NewtStageClient actor1_stage = new NewtStageClient<KeyValuePair, KeyValuePair>(actor1);
        actor1.addSourceOrDestinationActor(actor1.getActorID()+1, false);
        System.out.println("Actor1 Id: " + actor1.getActorID() + " , ParentId: " + rootActorID);
        
        NewtClient actor2 = new NewtClient(Mode.CAPTURE,rootActorID, "second", testActorType, "", false);//978//5
        actor2.setSchemaID(testActorSchemaID);
        actor2.setTableName("second_table");        
        NewtStageClient actor2_stage = new NewtStageClient<KeyValuePair<String,String>, KeyValuePair<String,String>>(actor2);
        actor2.addSourceOrDestinationActor(actor2.getActorID()-1, true);
        actor2.addSourceOrDestinationActor(actor2.getActorID()+1, false);
        System.out.println("Actor2 Id: " + actor2.getActorID() + " , ParentId: " + rootActorID);
        
        NewtClient actor3= new NewtClient(Mode.CAPTURE, rootActorID, "third", testActorType, "", false);//7
        actor3.setSchemaID(testActorSchemaID);
        actor3.setTableName("third_table");
        NewtStageClient actor3_stage = new NewtStageClient<KeyValuePair<String,String>, KeyValuePair<String,String>>(actor3);
        actor3.addSourceOrDestinationActor(actor3.getActorID()-1, true);
        System.out.println("Actor3 Id: " + actor3.getActorID() + " , ParentId: " + rootActorID);

                         

        //Add provenance
        actor1_stage.addInput(new KeyValuePair<String,String>( "k1", "v1"));
        actor1_stage.addInput(new KeyValuePair<String,String>( "k2", "v2"));
        actor1_stage.addOutput(new KeyValuePair<String,String>( "k3", "v3"));
        actor1_stage.addOutput(new KeyValuePair<String,String>( "k8", "v8"));
                
        actor2_stage.addInput(new KeyValuePair<String,String>( "k3", "v3"));
        actor2_stage.addInput(new KeyValuePair<String,String>( "k4", "v4"));
        actor2_stage.addOutput(new KeyValuePair<String,String>( "k5", "v5"));
        actor2_stage.addOutput(new KeyValuePair<String,String>( "k6", "v6"));
        
        actor3_stage.addInput(new KeyValuePair<String,String>( "k5", "v5"));
        actor3_stage.addInput(new KeyValuePair<String,String>( "k6", "v6"));
        actor3_stage.addOutput(new KeyValuePair<String,String>( "k9", "v9"));
        actor3_stage.addOutput(new KeyValuePair<String,String>( "k10", "v10"));

        actor1.commit();
        actor2.commit();   
        actor3.commit();
        newtClient.rootCommit();
       System.out.println("All actors have commited");
*///COMMENT - END
		
        /******************* TRACING *****************/
        Vector data = new Vector();
		NewtMySql mySql = new NewtMySql(Configuration.mysqlUser, Configuration.mysqlPassword);
		
//		ResultSet rs = mySql.executeQuery("Select output from Newt.first_table limit 1");
//		ResultSet rs = mySql.executeQuery("Select output from Newt.MRTRedTable_0_2 limit 1");
		ResultSet rs = mySql.executeQuery("Select output from Newt.OJTab_0_97678a4e_6429_4c82_9a49_6e3393e5f7b9 limit 1");
		
		try {
			while(rs.next())
			{
				data.add(rs.getBytes(1));
				break;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
//		int replayTraceID = NewtClient.trace(data, "backward", 1, 44, "VertexActor");
//		int replayTraceID = NewtClient.trace(data, "backward", 977, 978, "TestActor");
		int replayTraceID = NewtClient.trace(data, "backward", 1, 13, "PregelActor");
		
		NewtClient.printTraceResults( replayTraceID, "OJAct_0_97678a4e_6429_4c82_9a49_6e3393e5f7b9" );
//		NewtClient.printTraceResults( replayTraceID, "MRTRedAct_0_2" );
//		int replayTraceID = NewtClient.trace(data, "backward", 1, 2, "TestActor");
//		NewtClient.printTraceResults( replayTraceID, "first" );
	}

}
