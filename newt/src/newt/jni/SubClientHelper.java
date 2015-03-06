package newt.jni;

import newt.client.*;
import newt.actor.GraphLocatable;
import java.io.IOException;
import newt.client.NewtClient.Mode;
import java.util.Hashtable;
  
class SubClientHelper {
  static final String actorType = "SubActor";
  //static Hashtable<Key, NewtClient> subClients = new Hashtable<Key, NewtClient>();
  //static Hashtable<Key, NewtStageClient> subStageClients = new Hashtable<Key, NewtStageClient>();
  NewtClient subClient;
  NewtStageClient subStageClient;

  public SubClientHelper(int vertexID, int version, int parentActorID, int schemaID, int pid, int type) throws Exception{
    String actorName = "sub_vertex"+vertexID+"_version"+version+"_proc"+pid+"_type"+type;
    subClient = new NewtClient<GraphLocatable, GraphLocatable>(Mode.CAPTURE, parentActorID, actorName, 
                                                               actorType, "", true);
    if (subClient.getActorID() == -1) 
       throw new IOException("NewtProvenance: Error: Couldn't add sub actor.");

    subClient.setSchemaID(schemaID);
    subClient.setTableName("subactor" + subClient.getActorID()+"_table");
    subStageClient = new NewtStageClient<GraphLocatable, GraphLocatable>(subClient);
  }

  public void addInput(int from, int to, int version) {
    subStageClient.addInput(new GraphLocatable(from, to, version));   
  }

  public void addOutput(int from, int to, int version) {
    subStageClient.addOutput(new GraphLocatable(from, to, version));
  }

  public void commit() {
    subClient.commit();
  }

  public void addSourceBySubactor(int parentID) {
	subClient.addSourceBySubactor(parentID);
  }  
  
  /*
  public static void create(int vertexID, int version, int parentActorID, int schemaID, int pid, int type) throws IOException {

    String actorName = "sub_vertex"+vertexID+"_version"+version+"_proc"+pid+"_type"+type;
    NewtClient subClient = new NewtClient<GraphLocatable, GraphLocatable>(Mode.CAPTURE, parentActorID, actorName, 
                                                                          actorType, "", true);

    if (subClient.getActorID() == -1) 
       throw new IOException("NewtProvenance: Error: Couldn't add sub actor.");

    subClient.setSchemaID(schemaID);
    subClient.setTableName(subClient.getActorID()+"_table");
    NewtStageClient subStageClient = new NewtStageClient<GraphLocatable, GraphLocatable>(subClient);
   
    // temp
    subClient.commit();

    // add client and stage client to the hashtables
    Key k = new Key(vertexID, version, pid, type);
    subClients.put(k, subClient);
    subStageClients.put(k, subStageClient);
  }

  

  

  public static void commit(int kid, int kv, int kpid, int ktype) {
    
    Key key = new Key(kid, kv, kpid, ktype);
    NewtClient subClient = subClients.get(key);
    subClient.commit();

    subClients.remove(key);
    subStageClients.remove(key);
    
  }
  */
}
