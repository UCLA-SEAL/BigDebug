package newt.actor;

import java.io.*;
import java.lang.*;
import java.util.*;

public class ProvenanceContainer implements Externalizable {
    int maxSize;
    int size;
    ProvenanceItem[] pContainer;
    
    public ProvenanceContainer( int maxSize )
    {
        this.maxSize = maxSize;
        size = 0;
        pContainer = new ProvenanceItem[ maxSize ];
    }

    public void writeExternal( ObjectOutput out ) throws IOException
    {
        out.writeInt( size );
        if( size > 0 ) {
            for( int i = 0; i < size; i++ ) {
                out.writeObject( pContainer[ i ] );
            }
        }
    }

    public void readExternal( ObjectInput in ) throws IOException, ClassNotFoundException
    {
        size = in.readInt();
        if( size > 0 ) {
            pContainer = new ProvenanceItem[ size ];
            for( int i = 0; i < size; i++ ) {
                pContainer[ i ] = (ProvenanceItem)in.readObject();
            }
        }
    }

    public void add( ProvenanceItem association )
    {
        pContainer[ size ] = association;
        size++;
    }

    public int size()
    {
        return size;
    }

    public ProvenanceItem get( int index )
    {
        return pContainer[ index ];
    }
}
