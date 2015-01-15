package newt.actor;

/**
 * Created by kshitij on 1/14/15.
 */

public class StringProvenance implements ProvenanceDataType<String> {
    String provenance;
    
    public StringProvenance(String value)
    {
        provenance = value;
    }

    public String toString()
    {
        return provenance;
    }

    public String toProvenance()
    {
        return provenance;
    }

    public byte[] getBytes()
    {
        return provenance.getBytes();
    }
}
