package newt.jni;

import newt.client.*;
import newt.actor.GraphLocatable;
import java.io.IOException;
import newt.client.NewtClient.Mode;
import java.util.ArrayList;
import java.util.Hashtable;

class VertexClientHelper {
  static final String actorType = "VertexActor";
  NewtClient vertexClient;
  int actorID;

  public VertexClientHelper (int vertexID, int version, int jobActorID, int schemaID) throws IOException {

    String actorName = "vertex"+vertexID+"version"+version;
    vertexClient = new NewtClient<GraphLocatable, GraphLocatable>(Mode.CAPTURE, jobActorID, actorName,
                                                                  actorType, "RELATIVE_ID_NOT_USED", false);
    vertexClient.setSchemaID(schemaID);
    actorID = vertexClient.getActorID(); 
    vertexClient.setTableName("actor" + actorID + "_table");
  }

  public int getActorID() {
    return actorID;
  }

  public void commit() throws IOException {
    vertexClient.commit();
   System.out.println("OOOOOOOOOOOOOOOOOOOOO  suzie OOOOOOOOOOOOOOOOOOOOOOOOOOOo");
  }

  
  /*
  public static int create(int vertexID, int version, int jobActorID, int schemaID) throws IOException {

    String actorName = "vertex"+vertexID+"version"+version+"_vertex";
    // Warning: may change the 2nd parameter to the real job actor ID
    NewtClient vertexClient = new NewtClient<GraphLocatable, GraphLocatable>(Mode.CAPTURE, -1, actorName,
                                                                             actorType, "RELATIVE_ID_NOT_USED", false);

    // Warning: need to find a way to pass schema ID
    vertexClient.setSchemaID(schemaID);
    int actorID = vertexClient.getActorID(); 
    vertexClient.setTableName(actorID + "_table");

    vertexClients.put(new Key(vertexID, version), vertexClient);

    return actorID;
  }

  public static void commit(int vertexID, int version) throws IOException {

    // commit 
    Key key = new Key(vertexID, version);
    NewtClient vertexClient = vertexClients.get(key);
    vertexClient.commit();
   
   System.out.println("OOOOOOOOOOOOOOOOOOOOO  suzie OOOOOOOOOOOOOOOOOOOOOOOOOOOo");
    vertexClients.remove(key); 
  }
  */
}
