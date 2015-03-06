package newt.client.query;

import java.util.Arrays;

import newt.client.Hash;

/**
 * Represents a record that is identified by a hash. In this implementation
 * we create the hash using the MD5 algorithm. The has is represented as a
 * byte array.
 *
 */
public class HashRecordSpecification implements RecordSpecification {
    private byte[] hash;
    private String s_hash = null;

    public HashRecordSpecification(byte[] hash) {
	this.hash = Arrays.copyOf(hash, hash.length);
    }
    
    public HashRecordSpecification(RecordSpecification r) {
	this.hash = Hash.digest(r.toString());
    }
    
    public HashRecordSpecification(String s) {
	this.hash = Hash.digest(s);
    }
    
    public int hashCode() {
	return Arrays.hashCode(hash);
    }
    
    public boolean equals(Object other) {
	return Arrays.equals(hash, ((HashRecordSpecification)other).hash);
    }
    
    public int hash() {
	return hashCode();
    }
    
    public String toString() {
	if (s_hash==null) {
	    s_hash = new String(hash);
	}
	return s_hash;
    }
}
