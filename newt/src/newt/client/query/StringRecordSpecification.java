/*
 * Created on Apr 28, 2011
 *
 */
package newt.client.query;

/**
 * Represents a record that's simply identified by a string
 *
 */
public class StringRecordSpecification implements RecordSpecification {
    private String id;
    
    public StringRecordSpecification(String s) {
	this.id = s;
    }
    
    public StringRecordSpecification(RecordSpecification r) {
	id = r.toString();
    }
    
    public String toString() {
	return id;
    }
    
    public int hash() {
	return id.hashCode();
    }

    @Override
    public int hashCode() {
	return id.hashCode();
    }
    
    @Override
    public boolean equals(Object other) {
	return id.equals(((StringRecordSpecification)other).id);
    }
}
