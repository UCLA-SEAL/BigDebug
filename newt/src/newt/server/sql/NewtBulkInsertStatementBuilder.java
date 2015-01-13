package newt.server.sql;

import java.lang.*;
import java.util.*;

import java.sql.ResultSet;

public class NewtBulkInsertStatementBuilder extends NewtSqlStatementBuilder {
    String  table = null;
    int     rowLength = 0;
    
    public NewtBulkInsertStatementBuilder()
    {
        super( false );
    }

    public void setTable( String DBName, String tableName )
    {
        table = DBName + "." + tableName;
    }

    public void setTables( String DBTableName )
    {
        table = DBTableName;
    }

    public void setRowLength( int rowLength )
    {
        this.rowLength = rowLength;
    }

    public void addRow( Object[] row )
    {
        addParam( new Vector( Arrays.asList( row ) ) );
    }

    public void setQuery()
    {
        if( rowLength > 0 && table != null ) {
            setStatement( toString() );
        }
        rowLength = 0;
        table = null;
    }

    public String toString()
    {
        String update = "insert into " + table + " values ( ";
        for( int i = 0; i < rowLength; i++ ) {
            update += "?, ";
        }
        update = update.substring( 0, update.length() - 2 );
        update += " )";

        return update;
    }
}
