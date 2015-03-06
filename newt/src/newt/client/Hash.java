/*
 * Created on Apr 29, 2011
 *
 */
package newt.client;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hash {
    public static byte [] digest( String message )
    {
        byte [] mBytes = message.getBytes();
        byte [] mDigest = null;
        
        try{
            MessageDigest algorithm = MessageDigest.getInstance( "MD5" );
            algorithm.reset();
            algorithm.update( mBytes );
            mDigest = algorithm.digest();
        } catch( NoSuchAlgorithmException nsae ) {
            System.err.println( nsae.getMessage() );
        }

        return mDigest;
    }

}
