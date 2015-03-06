package newt.actor;

import java.io.*;
import java.lang.*;
import java.util.*;

public class ProvenanceItem implements Externalizable {
    public static final int ASSOCIATION = 0;
    public static final int INPUT = 1;
    public static final int RESET_INPUT = 2;
    public static final int OUTPUT = 3;
    public static final int OUTPUT_RESET = 4;
    public static final int RESET = 5;

    public int itemType;
    public byte[] primaryData;
    public byte[] tagOrSecondaryData;
    public long timeTaken;

    public ProvenanceItem()
    {
    }

    public ProvenanceItem( int itemType, byte[] primaryData, byte[] tagOrSecondaryData, long timeTaken )
    {
        this.itemType = itemType;
        this.primaryData = primaryData;
        this.tagOrSecondaryData = tagOrSecondaryData;
        this.timeTaken = timeTaken;
    }

    public void writeExternal( ObjectOutput out ) throws IOException
    {
        out.writeInt( itemType );
        out.writeInt( primaryData == null ? 0 : primaryData.length );
        out.write( primaryData );
        out.writeInt( tagOrSecondaryData == null ? 0 : tagOrSecondaryData.length );
        out.write( tagOrSecondaryData );
        out.writeLong( timeTaken );
    }

    public void readExternal( ObjectInput in ) throws IOException, ClassNotFoundException
    {
        itemType = in.readInt();
        primaryData = new byte[ in.readInt() ];
        in.readFully( primaryData );
        tagOrSecondaryData = new byte[ in.readInt() ];
        in.readFully( tagOrSecondaryData );
        timeTaken = in.readLong();
    }
  
    public int getType()
    {
    	return this.itemType;
    }
}
