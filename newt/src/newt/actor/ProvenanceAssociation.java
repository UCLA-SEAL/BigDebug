package newt.actor;

import java.io.*;
import java.lang.*;
import java.util.*;

public class ProvenanceAssociation implements Externalizable {
    public static byte[] fieldTerminator = new String( "##||\t||##" ).getBytes();
    public static byte[] lineTerminator = new String( "##||||##\n" ).getBytes();

    byte[] input;
    byte[] output;
    long timeTaken;

    public ProvenanceAssociation()
    {
    }

    public ProvenanceAssociation( byte[] input, byte[] output, long timeTaken )
    {
        this.input = input;
        this.output = output;
        this.timeTaken = timeTaken;
    }

    public void writeExternal( ObjectOutput out ) throws IOException
    {
        out.writeInt( input.length );
        out.write( input );
        out.writeInt( output.length );
        out.write( output );
        out.writeLong( timeTaken );
    }

    public void readExternal( ObjectInput in ) throws IOException, ClassNotFoundException
    {
        input = new byte[ in.readInt() ];
        in.readFully( input );
        output = new byte[ in.readInt() ];
        in.readFully( output );
        timeTaken = in.readLong();
    }

    public byte[] toAssociation()
    {
        return toAssociation( input, output, timeTaken );
    }
    
    public static byte[] toAssociation( byte[] input, byte[] output, Long timeTaken )
    {
        byte[] timeTakenInBytes = Long.toString( timeTaken ).getBytes();
        byte[] association = new byte[ input.length + output.length + timeTakenInBytes.length + 2*fieldTerminator.length + lineTerminator.length ];
        
        int written = 0;
        System.arraycopy( input, 0, association, written, input.length );
        written += input.length; 
        System.arraycopy( fieldTerminator, 0, association, written, fieldTerminator.length );
        written += fieldTerminator.length; 
        System.arraycopy( output, 0, association, written, output.length );
        written += output.length; 
        System.arraycopy( fieldTerminator, 0, association, written, fieldTerminator.length );
        written += fieldTerminator.length; 
        System.arraycopy( timeTakenInBytes, 0, association, written, timeTakenInBytes.length );
        written += timeTakenInBytes.length; 
        System.arraycopy( lineTerminator, 0, association, written, lineTerminator.length );
        return association;
    }
}
