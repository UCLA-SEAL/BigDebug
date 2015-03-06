package newt.actor;

import newt.common.*;

public class GraphLocatable implements ProvenanceDataType<byte[]> {
    static String   defaultSeparator = ",";
    static String   defaultEncloseBegin = "{";
    static String   defaultEncloseEnd = "}";
    
    int vid_from;
    int vid_to;
    int version;

    String  separator = defaultSeparator;
    String  encloseBegin = defaultEncloseBegin;
    String  encloseEnd = defaultEncloseEnd;

    public GraphLocatable(int vid_from, int vid_to, int version)
    {
//        this.key = key.toString();
          this.vid_from = vid_from;
          this.vid_to = vid_to;
          this.version = version;
    }

/*
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
*/

    public String toString() 
    {
        StringBuffer sb = new StringBuffer();
   
        sb.append( encloseBegin );        
        sb.append( vid_from );
        sb.append( separator );

        sb.append( vid_to );
        sb.append( separator );

        sb.append( version );
        sb.append( encloseEnd );
        return sb.toString();      

    }

    public byte[] toProvenance()
    {    
        // Zhaomo: TEMP
        //byte[] digest = Digest.digest( toString() );
        byte[] digest = toString().getBytes();
        return digest;
    }

    public byte[] getBytes()
    {
        return toProvenance();
    }
}
