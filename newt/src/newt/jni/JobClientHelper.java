package newt.jni;

import newt.client.*;
import java.io.IOException;
import newt.client.NewtClient.Mode;
import java.util.HashMap;
import java.util.ArrayList;


//temp
import java.io.PrintWriter;



class JobClientHelper {
  
  static NewtClient jobClient;
  public static int actorID = -999;
  //public static HashMap<String, Integer> schemas = new HashMap<String, Integer>();
  //public static HashMap<String, Integer> vertices = new HashMap<String, Integer>();
  //public static HashMap<String, ArrayList<Integer>> sources = new HashMap<String, ArrayList<Integer>>();
  static int vertexSchemaID = -1; 
    
  static final String universeName = "GraphLabUniverse";
  static final String rootActorName = "JobActor";
  static final String actorType = "VertexActor";
  static final String actorSchema = "VertexActor_Table";

  static final String actorsGset = "<?xml version=\"1.0\"?>\n" +
                                   "  <Universe name=\"" + universeName + "\">\n" +
                                   "    <RootActor name=\"" + rootActorName + "\">\n" +
                                   "      <" + actorType + " input=\"\" output=\"\"/>" +
                                   "    </RootActor>\n" + 
                                   "  </Universe>";

  static final String actorsSchemas = "<?xml version=\"1.0\"?>\n" +
                                      "  <Provenance>\n" +
                                      "    <Schema name=\"" + actorSchema + "\" actor=\"" + actorType + "\">\n" +
                                      "      <Input type=\"input\" datatype=\"GraphLocatable\" />\n" +
                                      "      <Output type=\"output\" datatype=\"GraphLocatable\" />\n" +
                                      "    </Schema>\n" +
                                      "  </Provenance>";



  static int create() throws Exception {

    jobClient = new NewtClient(Mode.CAPTURE);
    int universeID =  NewtClient.getUniverseID(universeName);  
    if (universeID == -1) 
       throw new IOException("NewtProvenance: Error: Unable to get UniverseID.");    

    actorID = jobClient.register(rootActorName, universeID);
    if (actorID == -1) 
       throw new IOException("NewtProvenance: Error: Unable to get actorID.");    
     
    int result = jobClient.setProvenanceHierarchy(actorsGset);
    if (result != 0)
       throw new IOException("NewtProvenance: Error: Unable to set gset.");    
    
    result = jobClient.setProvenanceSchemas(actorsSchemas);
    if (result != 0) 
       throw new IOException("NewtProvenance: Error: Unable to set schemas.");    

    vertexSchemaID = NewtClient.getSchemaID(universeID, actorSchema);
    if (vertexSchemaID == -1)
       throw new IOException("NewtProvenance: Error: Unable to get SchemaID.");    

    return actorID;    
  }

  static int getSchemaID() {
    return vertexSchemaID;
  }
   
  
  static void commit() {
   System.out.println("job client commit!");
    jobClient.rootCommit(); 
  }

}
