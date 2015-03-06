package newt.server.sql;

import java.lang.*;
import java.util.*;

import java.sql.ResultSet;

public class NewtDeleteStatementBuilder extends NewtSqlStatementBuilder {
    String              table = null;
    Vector<String>      conditions = null;
    boolean             isAnd = true;
    
    public NewtDeleteStatementBuilder() 
    {
        super( false );
        conditions = new Vector<String>();
    }

    public void setTable( String DBName, String tableName )
    {
        table = DBName + "." + tableName;
    }

    public void setTable( String DBTableNames )
    {
        table = DBTableNames;
    }

    public void addCondition( String columnName, String value )
    {
        conditions.add( columnName + " = '" + value + "'" );
    }

    public void addCondition( String tableName, String columnName, String value )
    {
        addCondition( tableName + "." + columnName, value );
    }

    public void addCondition( String columnName, int value )
    {
        conditions.add( columnName + " = " + value );
    }

    public void addCondition( String tableName, String columnName, int value )
    {
        addCondition( tableName + "." + columnName, value );
    }

    public void addConditions( String columnName, Object[] values )
    {
        String condition = "";
        for( Object value: values ) {
            if( value instanceof String ) {
                condition += columnName + " = '" + value + "' or ";
            } else if( value instanceof Integer ) {
                condition += columnName + " = " + value + " or ";
            }
        }
        condition = condition.substring( 0, condition.length() - 4 );
        if( values.length > 1 ) {
            condition = "( " + condition + " )";
        }
        conditions.add( condition );
    }

    public void addCondition( String tableName, String columnName, Object[] values )
    {
        addConditions( tableName + "." + columnName, values );
    }

    public void setQuery( boolean isAnd )
    {
        setQuery( isAnd, null );
    }

    public void setQuery( boolean isAnd, NewtSqlStatementBuilder statement )
    {
        this.isAnd = isAnd;
        if( conditions.size() > 0 && table != null ) {
            if( statement != null ) {
                statement.addStatement( toString() );
            } else {
                addStatement( toString() );
            }
        }
        reset();
    }

    public void reset()
    {
        table = null;
        conditions.clear();
    }

    public String toString()
    {
        String update = "delete from " + table + " where ";
        for( String condition: conditions ) {
            update += condition + (isAnd ? " and " : " or ");
        }
        update = update.substring( 0, (isAnd ? update.length() - 5 : update.length() - 4) );

        return update;
    }
}
