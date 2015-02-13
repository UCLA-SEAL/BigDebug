package newt.test;

import newt.actor.StringProvenance;
import newt.client.NewtClient;
import newt.client.NewtStageClient;

import java.util.List;


/**
 * Created by kshitij on 1/9/15.
 */
public class NewtWrapper {
    //common members
    static String testActorType = "Test";
    static int testActorSchemaID=-1;
    static int rootActorID =-1;
    //TODO Change this variable to use a boolean flag for toggle once the finalCommit TODO is resolved
    static int initCount =0;
    static NewtClient newtClient,parent_actor;

    //instance specific
    NewtClient  HadoopRDDActor;
    NewtStageClient HadoopRDDActor_stage;
    int instanceId = -1;

    public NewtWrapper(int id)
    {
        //TODO Ksh to ensure client is initialised only once
        if(++initCount == 1)
        {
            SingleInit();
        }

        init(id);
    }

    public static void SingleInit()
    {
        //TODO Ksh
        System.out.println("Single Init");

        //String testActorType = "Test";
        String testActorSchema = "HadoopActor_Table";
        String testActorSchemaTable = "HadoopActor_Logical";
        String universeName = "NewtTestUniverse";
        String rootActorName = "NewtTestRoot";

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

        newtClient = new NewtClient(NewtClient.Mode.CAPTURE);
        int universeID = NewtClient.getUniverseID(universeName);
        rootActorID = newtClient.register(rootActorName, universeID);
        if (universeID == -1 || rootActorID == -1) {
            System.out.println("Error: Unable to register Universe or rootActor. Exiting...");
            System.exit(0);
        }

    /* Setting up actors containment relationships and provenance tables. */
        int result = newtClient.setProvenanceHierarchy(testActorsGset);
        if (result != 0) {
            System.out.println("Error: Failed to register actor hierarchy. Exiting...");
            System.exit(0);
        }

        int result2 = newtClient.setProvenanceSchemas(testActorsSchemas);
        if (result2 != 0) {
            System.out.println("Error: Failed to register tables. Exiting...");
            System.exit(0);
        }

        testActorSchemaID = NewtClient.getSchemaID(universeID, testActorSchema);
        if (testActorSchemaID == -1) {
            System.out.println("Error: Failed to retrieve schema ID, provenance information may be registered incorrectly. Exiting...");
            System.exit(0);
        }
        //System.out.println( "NewtClient.getSchemaID( universeID, testActorSchema ) COMPLETE" );

        // Create parent actor that contains subActors
        parent_actor = new NewtClient(NewtClient.Mode.CAPTURE, rootActorID, "parent", testActorType, "", false);
        parent_actor.setSchemaID(testActorSchemaID);
        parent_actor.setTableName("parent_actor_table");
        NewtStageClient parent_stage = new NewtStageClient<StringProvenance, StringProvenance>(parent_actor);
    }

    public void init(int id) {
        instanceId = id;
        String HadoopRDDActorName = "HadoopActor" + id;
        String HadoopRDDActorTableName = "HadoopActorTable" + id;
        HadoopRDDActor = new NewtClient(NewtClient.Mode.CAPTURE, parent_actor.getActorID(), HadoopRDDActorName, testActorType, "", false);
        HadoopRDDActor.setSchemaID(testActorSchemaID);
        HadoopRDDActor.setTableName(HadoopRDDActorTableName);
        HadoopRDDActor_stage = new NewtStageClient<StringProvenance, StringProvenance>(HadoopRDDActor);
        System.out.println("Created Actor : "+ id);
    }

    public void add(String output,List<String> input)
    {
        //HadoopRDDActor_stage.addInput(new KeyValuePair<String, String>(output.toString(), output.toString()));
        //HadoopRDDActor_stage.addOutput(new KeyValuePair<String,String>(output.toString(),output.toString()));
        for(String item : input)
        {
            HadoopRDDActor_stage.addInput(new StringProvenance(item));
            HadoopRDDActor_stage.addOutputAndFlush(new StringProvenance(output));
        }

        //System.out.println("Added " + output + " using Actor : "+ this.getId());
    }

    public void addLink(int sourceId,boolean source)
    {
        this.HadoopRDDActor.addSourceOrDestinationActor(sourceId,source);
    }

    public int getActorID()
    {
        return this.HadoopRDDActor.getActorID();
    }

    public void commit()
    {
        HadoopRDDActor.commit();
        System.out.println("Hadoop Actor Commit complete : "+instanceId);
    }

    public static void finalCommit()
    {
        parent_actor.commit();
        newtClient.rootCommit();
        System.out.println("Final Commit complete");

    }

    public int getId()
    {
        return instanceId;
    }
}

