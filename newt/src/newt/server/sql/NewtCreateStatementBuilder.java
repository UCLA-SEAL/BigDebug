package newt.server.sql;

import java.lang.*;
import java.util.*;

import java.sql.ResultSet;

public class NewtCreateStatementBuilder extends NewtSqlStatementBuilder {
    String          table = null;
    Vector<String>  columns = null;
    Vector<String>  indexes = null;
    String          primaryKey = null;
    
    public NewtCreateStatementBuilder()
    {
        super( false );
        columns = new Vector<String>();
        indexes = new Vector<String>();
    }

    public void setTable( String DBName, String tableName )
    {
        table = DBName + "." + tableName;
    }

    public void setTables( String DBTableName )
    {
        table = DBTableName;
    }

    public void addColumn( String columnName, String dataType, boolean nullable )
    {
        String column = columnName + " " + dataType;
        if( !nullable ) {
            column += " not null";
        }
        columns.add( column ); 
    }

    public void addColumn( String[] columns )
    {
        this.columns.addAll( Arrays.asList( columns ) );
    }

    public void addIndex( String columnName )
    {
        indexes.add( "index using btree( " + columnName + " )" );
    }

    public void addIndex( String columnName, int prefixLength )
    {
        addIndex( columnName + "(" + prefixLength + ")" );
    }

    public void setPrimaryKey( String columnName )
    {
        primaryKey = columnName;
    }

    public String getQuery()
    {
        if( columns.size() > 0 && table != null ) {
            return toString();
        }
        return null;
    }

    public void setQuery()
    {
        setQuery( null );
    }

    public void setQuery( NewtSqlStatementBuilder statement )
    {
        if( columns.size() > 0 && table != null ) {
            if( statement != null ) {
                statement.addStatement( toString() );
            } else {
                addStatement( toString() );
            }
        }
        columns.clear();
        indexes.clear();
        table = null;
        primaryKey = null;
    }

    public void reset()
    {
        columns.clear();
        indexes.clear();
        table = null;
        primaryKey = null;
    }

    public String toString()
    {
        String update = "create table if not exists " + table + " ( ";
        for( String column: columns ) {
            update += column + ", ";
        }
        update = update.substring( 0, update.length() - 2 );
        for( String index: indexes ) {
            update += ", " + index;
        }
        if( primaryKey != null ) {
            update += ", primary key( " + primaryKey + " )";
        }
        update += " )";

        return update;
    }
}
