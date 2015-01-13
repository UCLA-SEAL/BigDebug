package newt.actor;

import java.io.*;

public interface ProvenanceDataType<T> extends Serializable {
    public T toProvenance();
    public byte[] getBytes();
    public String toString();
}
