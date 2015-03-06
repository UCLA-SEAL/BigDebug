package newt.server;

import java.lang.*;
import java.util.*;
import java.io.*;

import newt.client.query.*;

public class TraceBloomFilter {
    int traceID = -1;
    HashMap<String, HashMap<String, BloomFilter>> actorFilters = null;

    public TraceBloomFilter( int traceID )
    {
        this.traceID = traceID;
        actorFilters = new HashMap<String, HashMap<String, BloomFilter>>();
    }

    public void addFilter( BloomFilter filter, String relativeID, String atype )
    {
        if( actorFilters.get( atype ) == null )
        {
            actorFilters.put( atype, new HashMap<String, BloomFilter>() );
        }
        
        if( actorFilters.get( atype ).get( relativeID ) == null )
        {
            HashMap<String, BloomFilter> map = actorFilters.get( atype );
            map.put( relativeID, filter );
        }
        else
        {
            actorFilters.get( atype ).get( relativeID ).union( filter );
        }        
    }

    public HashMap<String, BloomFilter> getFilters( String atype )
    {
        return actorFilters.get( atype );
    }
}
