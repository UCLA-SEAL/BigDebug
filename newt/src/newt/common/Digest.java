package newt.common;

import java.security.*;
import java.util.Arrays;

public class Digest {
    static MessageDigest    algorithm = null;
    static byte             underscore = (byte)'_';
    static byte             space = (byte)' ';
    static int              digestLength = 0;

    static 
    {
        try {
            algorithm = MessageDigest.getInstance( "MD5" );
            digestLength = algorithm.getDigestLength();
        } catch( NoSuchAlgorithmException nsae ) {
            System.err.println( nsae.getMessage() );
        }
    }
    
    public synchronized static byte[] digest( byte[] array, int start, int len) {
     	algorithm.update( array, start, len );
        byte[] mDigest = algorithm.digest();
        if( mDigest[ digestLength - 1 ] == space ) {
            mDigest[ digestLength - 1 ] = underscore;
        }
        return mDigest;	
    }
    
    public synchronized static byte[] digest( byte[] array )
    {
    	algorithm.update( array );
        byte[] mDigest = algorithm.digest();
        if( mDigest[ digestLength - 1 ] == space ) {
            mDigest[ digestLength - 1 ] = underscore;
        }
        return mDigest;
    }
    
    public synchronized static byte[] digest( String message )
    {
        algorithm.update( message.getBytes() );
        byte[] mDigest = algorithm.digest();
        if( mDigest[ digestLength - 1 ] == space ) {
            mDigest[ digestLength - 1 ] = underscore;
        }
        return mDigest;
    }

    public static int getDigestLength()
    {
        return digestLength;
    }
}        
