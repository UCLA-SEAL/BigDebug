package newt.test;

import newt.actor.KeyValuePair;
import newt.client.NewtClient;
import newt.client.NewtClient.Mode;

public class ClientCreationTiming 
{
    static String testActorType = "TestActor";
    static String testActorSchema = "TestActor_Table";
    static String testActorSchemaTable = "TestActor_Logical";
    static String universeName = "NewtTestUniverse";
    static String rootActorName = "NewtTestRoot";
    static int universeID = -1;
    static int rootActorID = -1;
    static int testActorSchemaID = -1;
    

	public static void createClient(Integer i)
	{
        NewtClient actor = new NewtClient(Mode.CAPTURE,rootActorID,"actor_"+i, testActorType, "", false);
        actor.setSchemaID(testActorSchemaID);
        actor.setTableName("table_"+i);
        actor.addInput(new KeyValuePair<String,String>( "k1", "v1"),"");
        actor.addOutput(new KeyValuePair<String,String>( "k2", "v2"),"");
        actor.commit();
	}
	
    public static void main( String[] args ) throws InterruptedException
    {
        NewtClient newtClient = null;

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
    
                    
        newtClient = new NewtClient( Mode.CAPTURE );
        universeID = NewtClient.getUniverseID( universeName );
        rootActorID = newtClient.register( rootActorName, universeID );
        if( universeID == -1 || rootActorID == -1 ) {
           System.out.println( "Error: Unable to register Universe or rootActor. Exiting..." );
            System.exit( 0 );
        }
        
        /* Setting up actors containment relationships and provenance tables. */
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

        int numClients = 300;
        long start = System.currentTimeMillis();
        for(int i=0; i<numClients; i++)
        {
        	createClient(i);
        }
        long end = System.currentTimeMillis();
        
        System.out.println("Creation of [" +numClients + "] actors takes " + (end-start) + "ms");
        
        System.exit(1);
    }
	
	
	
}
