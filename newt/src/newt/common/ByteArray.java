package newt.common;

import java.util.Arrays;
import java.io.*;

public class ByteArray implements Serializable {
    static byte escape = (byte)'\\';
    byte[]      array = null;
    
    public ByteArray( byte[] array )
    {
        this.array = array;
    }

    public byte[] getBytes()
    {
        return array;
    }

    public boolean equals( Object o )
    {
        if( o instanceof ByteArray ) {
            return Arrays.equals( array, ((ByteArray) o).getBytes() );
        } else if( o instanceof byte[] ) {
            return Arrays.equals( array, (byte[]) o );
        } else {
            return false;
        }
    }

    public int hashCode()
    {
        return Arrays.hashCode( array );
    }

    public static byte[] escapeBackslashes( byte[] byteArray )
    {
        byte[] escapedByteArray = new byte[ 2 * byteArray.length ];
        int i = 0;
        for( byte b: byteArray ) {
            if( b == escape ) {
                escapedByteArray[ i ] = escape;
                escapedByteArray[ i + 1 ] = b;
                i += 2;
            } else {
                escapedByteArray[ i ] = b;
                i++;
            }
        }

        return Arrays.copyOf( escapedByteArray, i );
    }
}

