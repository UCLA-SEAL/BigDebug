package newt.client.query;

/**
 * Interface to the Newt server for replay provenance. An imlementation
 * must return a ProvenanceContiner for a specific actor and a specific
 * replay execution.
 */
public interface ReplayProvenanceQuery {

    ProvenanceContainer query(String actorID, String replayID) throws Exception;
}
