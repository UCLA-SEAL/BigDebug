package newt.client.query;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


public class SetContainer implements ProvenanceContainer,Iterable<RecordSpecification> {
    
    private Set<RecordSpecification> set;
    
    public SetContainer() {
	this.set = new HashSet<RecordSpecification>();
    }
    
    public void add(RecordSpecification r) {
	set.add(r);
    }
    
    public boolean contains(RecordSpecification r) {
	return set.contains(r);
    }

    public int size() {
	return set.size();
    }

    public Iterator<RecordSpecification> iterator() {
	return set.iterator();
    }
}
