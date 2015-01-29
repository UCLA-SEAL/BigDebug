package newt.test;

import newt.actor.KeyValuePair;
import newt.client.NewtClient;
import newt.client.NewtClient.Mode;
import newt.client.NewtStageClient;

public class VickyCaptureTest {
    
    
    public static void main( String[] args ) throws InterruptedException
    {

    
        String testActorType = "TestActor";
        String testActorSchema = "TestActor_Table";
        String testActorSchemaTable = "TestActor_Logical";
        String universeName = "NewtTestUniverse";
        String rootActorName = "NewtTestRoot";
        int universeID = -1;
        int rootActorID = -1;
        int testActorSchemaID = -1;
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

        System.out.println( "newtClient.setProvenanceHierarchy( testActorsGset ) COMPLETE" );
        
        result = newtClient.setProvenanceSchemas( testActorsSchemas );
        if( result != 0 ) {
           System.out.println( "Error: Failed to register tables. Exiting..." );
            System.exit( 0 );
        }
        System.out.println( "newtClient.setProvenanceSchemas( testActorsSchemas ) COMPLETE" );
  
        testActorSchemaID = NewtClient.getSchemaID( universeID, testActorSchema );
        if( testActorSchemaID == -1 ) {
           System.out.println( "Error: Failed to retrieve schema ID, provenance information may be registered incorrectly. Exiting..." );
            System.exit( 0 );
        }
        System.out.println( "NewtClient.getSchemaID( universeID, testActorSchema ) COMPLETE" );

        // Create parent actor that contains subActors
        NewtClient parent_actor = new NewtClient(Mode.CAPTURE,rootActorID,"parent", testActorType, "", false);
        parent_actor.setSchemaID(testActorSchemaID);
        parent_actor.setTableName("parent_actor_table");
        NewtStageClient parent_stage = new NewtStageClient<KeyValuePair, KeyValuePair>(parent_actor);
        System.out.println( "parent_stage COMPLETE" );
        
        //Create subActors
        NewtClient actorX = new NewtClient<KeyValuePair, KeyValuePair>(Mode.CAPTURE, parent_actor.getActorID(), "actorX", testActorType, "", false);
        actorX.setSchemaID(testActorSchemaID);
        actorX.setTableName("PregelOJTab_2_fa4bb9dd_5c13_4961_af6c_27bd6bdb99f4");
        actorX.addInput(new KeyValuePair<String, String>("1","2"), "");
        actorX.addOutput(new KeyValuePair<String, String>("2","4"), "");
        actorX.commit();
        System.out.println( "actorX COMPLETE" );
      
        //Create subActors
        NewtClient actor1 = new NewtClient(Mode.CAPTURE, parent_actor.getActorID(), "actor11", testActorType, "", true);
        actor1.setSchemaID(testActorSchemaID);
        actor1.setTableName("actor1_table");
        NewtStageClient<KeyValuePair<String,String>, KeyValuePair<String,String>> actor1_stage = new NewtStageClient<KeyValuePair<String,String>, KeyValuePair<String,String>>(actor1);
        System.out.println( "actor1_stage COMPLETE" );

        NewtClient actor2 = new NewtClient(Mode.CAPTURE, parent_actor.getActorID(), "actor12", testActorType, "", true);
        actor2.setSchemaID(testActorSchemaID);
        actor2.setTableName("actor2_table");
        NewtStageClient<KeyValuePair<String,String>, KeyValuePair<String,String>> actor2_stage = new NewtStageClient<KeyValuePair<String,String>, KeyValuePair<String,String>>(actor2);
        System.out.println( "actor2_stage COMPLETE" );

        NewtClient actor3 = new NewtClient(Mode.CAPTURE, parent_actor.getActorID(), "actor13", testActorType, "", true);
        actor3.setSchemaID(testActorSchemaID);
        actor3.setTableName("actor3_table");
        NewtStageClient<KeyValuePair<String,String>, KeyValuePair<String,String>> actor3_stage = new NewtStageClient<KeyValuePair<String,String>, KeyValuePair<String,String>>(actor3);
        System.out.println( "actor3_stage COMPLETE" );

        NewtClient actor4 = new NewtClient(Mode.CAPTURE, parent_actor.getActorID(), "actor14", testActorType, "", true);
        actor4.setSchemaID(testActorSchemaID);
        actor4.setTableName("actor4_table");
        NewtStageClient<KeyValuePair<String,String>, KeyValuePair<String,String>> actor4_stage = new NewtStageClient<KeyValuePair<String,String>, KeyValuePair<String,String>>(actor4);
        System.out.println( "actor4_stage COMPLETE" );

        //Add provenance
        actor1_stage.addInput(new KeyValuePair<String,String>( "k1", "v1"));
        Thread.sleep(200);
        System.out.println( "actor1_stage.addInput#1 COMPLETE" );

        actor1_stage.addInput(new KeyValuePair<String,String>( "k2", "v2"));
        Thread.sleep(200);
        System.out.println( "actor1_stage.addInput#2 COMPLETE" );

        actor2_stage.addInput(new KeyValuePair<String,String>( "k3", "v3"));
        Thread.sleep(200);
        System.out.println( "actor2_stage.addInput#1 COMPLETE" );

        actor2_stage.addInput(new KeyValuePair<String,String>( "k4", "v4"));
        Thread.sleep(200);
        System.out.println( "actor2_stage.addInput#2 COMPLETE" );

        actor1_stage.addInput(new KeyValuePair<String,String>( "k5", "v5"));
        Thread.sleep(200);
        System.out.println( "actor1_stage.addInput#3 COMPLETE" );

        actor1_stage.addInput(new KeyValuePair<String,String>( "k6", "v6"));
        Thread.sleep(200);
        System.out.println( "actor1_stage.addInput#4 COMPLETE" );

         
        actor1.commit();
        System.out.println( "actor1_commit COMPLETE" );
        actor2.commit();
        System.out.println( "actor2_commit COMPLETE" );
        

        actor3_stage.addOutput(new KeyValuePair<String,String>( "k7", "v7"));
        Thread.sleep(200);
        System.out.println( "actor3_stage.addInput#1 COMPLETE" );
        
        actor3_stage.addOutput(new KeyValuePair<String,String>( "k8", "v8"));
        Thread.sleep(200);
        System.out.println( "actor3_stage.addInput#2 COMPLETE" );
        actor4_stage.addOutput(new KeyValuePair<String,String>( "k9", "v9"));
        Thread.sleep(200);
        System.out.println( "actor4_stage.addInput#1 COMPLETE" );

        actor4_stage.addOutput(new KeyValuePair<String,String>( "k10", "v10"));
        Thread.sleep(200);
        System.out.println( "actor4_stage.addInput#2 COMPLETE" );

        actor3_stage.addOutput(new KeyValuePair<String,String>( "k11", "v11"));
        Thread.sleep(200);
        System.out.println( "actor3_stage.addInput#3 COMPLETE" );

        actor4_stage.addOutput(new KeyValuePair<String,String>( "k12", "v12"));
        Thread.sleep(200);
        System.out.println( "actor4_stage.addInput#3 COMPLETE" );

     
        actor3.commit();
        System.out.println( "actor3_commit COMPLETE" );

        actor4.commit();   
        System.out.println( "actor4_commit COMPLETE" );

        parent_actor.commit();
        System.out.println("All actors have commited");
        
        //System.out.println("commit job acotr");
        
        newtClient.rootCommit();
        System.out.println("Root commit done");


    }
        
}
