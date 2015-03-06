package newt.server.sql;

import java.lang.*;
import java.util.*;

import java.sql.ResultSet;

public class NewtInsertStatementBuilder extends NewtSqlStatementBuilder {
    String          table = null;
    Vector<String>  values = null;
    
    public NewtInsertStatementBuilder() 
    {
        super( false );
        values = new Vector<String>();
    }

    public void setTable( String DBName, String tableName )
    {
        table = DBName + "." + tableName;
    }

    public void setTables( String DBTableName )
    {
        table = DBTableName;
    }

    public void addValue( String value )
    {
        values.add( "'" + value + "'" );
    }

    public void addValue( Integer value )
    {
        values.add( value.toString() );
    }

    public void addValues( Object[] values )
    {
        for( Object value: values ) {
            if( value instanceof String ) {
                addValue( (String) value );
            } else if( value instanceof Integer ) {
                addValue( (Integer) value );
            }
        }
    }

    public void setQuery()
    {
        setQuery( null );
    }

    public void setQuery( NewtSqlStatementBuilder statement )
    {
        if( values.size() > 0 && table != null ) {
            if( statement != null ) {
                statement.addStatement( toString() );
            } else {
                addStatement( toString() );
            }
        }
        values.clear();
        table = null;
    }

    public String toString()
    {
        String update = "insert into " + table + " values ( ";
        for( String value: values ) {
            update += value + ", ";
        }
        update = update.substring( 0, update.length() - 2 );
        update += " )";

        return update;
    }
}
