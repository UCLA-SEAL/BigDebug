package newt.actor;

import newt.common.*;

public class KeyValuePair<K,V> implements ProvenanceDataType<byte[]> {
    static String   defaultSeparator = ",";
    static String   defaultEncloseBegin = "{";
    static String   defaultEncloseEnd = "}";
    
    String  key = null;
    String  value = null;
    String  separator = defaultSeparator;
    String  encloseBegin = defaultEncloseBegin;
    String  encloseEnd = defaultEncloseEnd;

    public KeyValuePair( K key, V value )
    {
//        this.key = key.toString();
	this.key = key!=null?key.toString():"NULL";
        this.value = value.toString();
    }

    public KeyValuePair( K key, V value, String separator )
    {
        this( key, value );
        this.separator = separator;
    }

    public KeyValuePair( K key, V value, String encloseBegin, String encloseEnd )
    {
        this( key, value );
        this.encloseBegin = encloseBegin;
        this.encloseEnd = encloseEnd;
    }

    public KeyValuePair( K key, V value, String separator, String encloseBegin, String encloseEnd )
    {
        this( key, value );
        this.separator = separator;
        this.encloseBegin = encloseBegin;
        this.encloseEnd = encloseEnd;
    }

    public String toString() 
    {
        StringBuffer sb = new StringBuffer();
        sb.append( encloseBegin );
        sb.append( key );
        sb.append( separator );
        sb.append( value );
        sb.append( encloseEnd );
        return sb.toString();
    }

    public byte[] toProvenance()
    {
        //byte[] digest = Digest.digest( toString() );
        byte[] digest = toString().getBytes();
        return digest;
    }

    public byte[] getBytes()
    {
        return toProvenance();
/*       int hashCode = key.hashCode()+31*value.hashCode();
       return new byte[] {
            (byte)(hashCode >>> 24),
            (byte)(hashCode >>> 16),
            (byte)(hashCode >>> 8),
            (byte)hashCode};
*/    }
}
