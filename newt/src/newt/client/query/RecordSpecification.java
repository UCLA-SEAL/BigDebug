package newt.client.query;

/**
 * Implementations of RecordSpecification must implement the Object
 * methods hashCode() and equals()
 *
 */
public interface RecordSpecification {

    public int hash();
    
    public boolean equals(Object other);
}
