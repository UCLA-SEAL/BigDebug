package newt.client.query;

public class FileLocation implements RecordSpecification {
    String path;
    long offset;
    long length;
    
    public FileLocation(String path, long offset, long length) {
	this.path = path;
	this.offset = offset;
	this.length = length;
    }
    
    public int hash() {
	return (path+":"+offset+":"+length).hashCode();
    }

}
