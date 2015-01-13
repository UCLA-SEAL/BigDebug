package newt.test;

import java.lang.*;
import java.util.*;
import java.io.*;
import java.security.*;

import newt.client.*;
import newt.client.NewtClient.*;
import newt.actor.*;

public class NewtTest extends Thread {
    boolean provenanceCapture = false;
    NewtClient<KeyValuePair, KeyValuePair> newtClient = null;
    NewtStageClient<KeyValuePair, KeyValuePair> newtStageClient = null;
    String actorType = null;
    int actorID = -1;
    int schemaID = -1;
    int parentID = -1;
    int universeID = -1;

    int numRecords = 0;

    char[] array = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 
                     'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
                     'u', 'v', 'w', 'x', 'y', 'z', 'a', 'b', 'c', 'd', 
                     'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 
                     'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 
                     'y', 'z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 
                     'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 
                     's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '?', '"',
                     ';', ':', '-', '_', '!', ' ', ' ', ' ', ' ', ' ',
                     ' ', ' ', ' ', 'e', 'a', 'i', 'o', 'u', 'e', 'a',
                     'i', 'o', 'u', 'E', 'A', 'I', 'O', 'U' };
    int range = array.length - 1;
    int recordSize = 40;
    Random generator = new Random();

    public static void main( String[] args )
    {
        int numActors = 1;
        int numRecordsPerActor = 10000;
    
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
    
        if( args.length > 0 && args.length != 2 ) {
            System.err.println( "Usage: [<num_actors> <records_per_actor>]" );
            System.exit( 0 );
        } else if( args.length > 0 ) {
            numActors = Integer.parseInt( args[ 0 ] );
            numRecordsPerActor = Integer.parseInt( args[ 1 ] );
        }
            
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

        long normalRunningTime = startTestActorsWithoutNewt( numActors, numRecordsPerActor );
       System.out.println( "Normal running time: " + normalRunningTime + " ms" );
        long instrumentedRunningTime = startTestActorsWithNewt( numActors, numRecordsPerActor, testActorType, testActorSchemaID, universeID, rootActorID );
       System.out.println( "Instrumented running time: " + instrumentedRunningTime + " ms" );
        float timeOverhead = ((float) instrumentedRunningTime) * 100 / normalRunningTime;
       System.out.println( "Time overhead for " + numActors + " actors producing " + numRecordsPerActor + " records: " + timeOverhead );
        System.exit( 0 );
    }

    public static long startTestActorsWithoutNewt( int numActors, int numRecordsPerActor )
    {
        NewtTest[] testActors = new NewtTest[ numActors ];
        for( int i = 0; i < numActors; i++ ) {
            testActors[ i ] = new NewtTest( numRecordsPerActor );
        }

        long startTime = System.currentTimeMillis();
        for( NewtTest testActor: testActors ) {
            if( testActor == null ) {
               System.out.println( "blah " );
            }
            testActor.start();
        }

        boolean done = false;
        while( !done ) {
            try {
                for( NewtTest testActor: testActors ) {
                    testActor.join();
                }
                done = true;
            } catch( Exception e ) {
            }
        }

        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }

    public static long startTestActorsWithNewt( int numActors, int numRecordsPerActor, String testActorType, int testActorSchemaID, int universeID, int rootActorID )
    {
        NewtTest[] testActors = new NewtTest[ numActors ]; 
        for( int i = 0; i < numActors; i++ ) {
            testActors[ i ] = new NewtTest( numRecordsPerActor, testActorType, testActorSchemaID, universeID, rootActorID );
        }

        long startTime = System.currentTimeMillis();
        for( NewtTest testActor: testActors ) {
            testActor.run();
        }

        boolean done = false;
        while( !done ) {
            try {
                for( NewtTest testActor: testActors ) {
                    testActor.join();
                }
                done = true;
            } catch( Exception e ) {
            }
        }

        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }

    public NewtTest( int numRecords )
    {
        this.numRecords = numRecords;
        provenanceCapture = false;
    }

    public NewtTest( int numRecords, String actorType, int schemaID, int universeID, int parentID )
    {
        this.actorType = actorType;
        this.schemaID = schemaID;
        this.universeID = universeID;
        this.parentID = parentID;
        this.numRecords = numRecords;
        provenanceCapture = true;
    }

    public void run()
    {
        try {
            long registerTime = 0;
            if( provenanceCapture ) {
                long startTime = System.currentTimeMillis();
                newtClient = new NewtClient<KeyValuePair, KeyValuePair>(Mode.CAPTURE, parentID, getName(), actorType, "",false );
                newtClient.setSchemaID(schemaID);
                newtClient.setTableName(getName().replace( '-', '_' ) + "_table");
                actorID = newtClient.getActorID();
                if( actorID == -1 ) {
                   System.out.println( "Thread " + getName() + " failed to register with Newt server. Exiting..." );
                    return;
                }
                newtStageClient = new NewtStageClient( newtClient );
                registerTime = System.currentTimeMillis() - startTime;
            }

            char inputFile[] = new char[ recordSize ];
            char inputKey[] = new char[ recordSize ];
            char inputValue[] = new char[ recordSize ];
            int inputChunk = 0;
            int inputBegin = 0;
            int inputLength = 0;
            char outputFile[] = new char[ recordSize ];
            char outputKey[] = new char[ recordSize ];
            char outputValue[] = new char[ recordSize ];
            int outputChunk = 0;
            int outputBegin = 0;
            int outputLength = 0;

            String inputFileString = new String( inputFile );
            String outputFileString = new String( outputFile );
   
            inputChunk = generator.nextInt( 5000 );
            outputChunk = generator.nextInt( 5000 );
            inputBegin = generator.nextInt( 65535 );
            outputBegin = generator.nextInt( 65535 );
            inputLength = generator.nextInt( 255 );
            outputLength = generator.nextInt( 255 );

                for( int j = 0; j < recordSize; j++ ) {
                    inputFile[ j ] = array[ generator.nextInt( range ) ];
                    inputKey[ j ] = array[ generator.nextInt( range ) ];
                    inputValue[ j ] = array[ generator.nextInt( range ) ];
                    outputFile[ j ] = array[ generator.nextInt( range ) ];
                    outputKey[ j ] = array[ generator.nextInt( range ) ];
                    outputValue[ j ] = array[ generator.nextInt( range ) ];
                }
                
                /*String inputKeyString = new String( inputKey );
                String inputValueString = new String( inputValue );
                String outputKeyString = new String( outputKey );
                String outputValueString = new String( outputValue );*/
                String inputKeyString1 = "tag1_ik1";
                String inputValueString1 = "_iv1";
                String outputKeyString1 = "tag1_ok1";
                String outputValueString1 = "_ov1";
                String inputKeyString2 = "tag2_ik2";
                String inputValueString2 = "_iv2";
                String outputKeyString2 = "tag2_ok2";
                String outputValueString2 = "_ov2";

                if( provenanceCapture ) {
                    newtStageClient.addTaggedInput( "tag1", new KeyValuePair( inputKeyString1, inputValueString1 ) );
                    newtStageClient.addTaggedInput( "tag2", new KeyValuePair( inputKeyString2, inputValueString2 ) );
                }

            for( int i = 0; i < numRecords; i++ ) {
                if( provenanceCapture ) {
                    newtStageClient.addTaggedOutput( "tag1", new KeyValuePair( outputKeyString1, outputValueString1 ) );
                    newtStageClient.addTaggedOutput( "tag2", new KeyValuePair( outputKeyString2, outputValueString2 ) );
//                  newtClient.addProvenance( new KeyValuePair( inputKeyString, inputValueString ), new KeyValuePair( outputKeyString, outputValueString ) );
//                  newtClient.addProvenance( new FileLocatable( inputFileString, true, inputChunk, inputBegin, inputLength, true ),
//                                              new FileLocatable( outputFileString, true, outputChunk, outputBegin, outputLength, true ) );
                }
            }

            if( provenanceCapture ) {
                newtClient.commit();
            }
            return;
        } catch( Exception e ) {
           System.out.println( e.getMessage() );
            e.printStackTrace();
        }
    }
}
