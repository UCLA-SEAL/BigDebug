package newt.server.sql;

import java.lang.*;
import java.util.*;

import java.sql.ResultSet;

public class NewtUpdateStatementBuilder extends NewtSqlStatementBuilder {
    String              table = null;
    Vector<String>      columns = null;
    Vector<String>      conditions = null;
    boolean             isAnd = true;
    
    public NewtUpdateStatementBuilder()
    {
        super( false );
        columns = new Vector<String>();
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

    public void addColumn( String columnName, String value )
    {
        columns.add( columnName + " = '" + value + "'" );
    }

    public void addColumn( String tableName, String columnName, String value )
    {
        addColumn( tableName + "." + columnName, value );
    }

    public void addColumn( String columnName, int value )
    {
        columns.add( columnName + " = " + value );
    }

    public void addColumn( String tableName, String columnName, int value )
    {
        addColumn( tableName + "." + columnName, value );
    }

    public void addColumns( HashMap<String, String> tableColumnNameValues )
    {
        Set tableColumnNameValueEntries = tableColumnNameValues.entrySet();
        for( Object o: tableColumnNameValueEntries ) {
            Map.Entry m = (Map.Entry) o;
            addColumn( (String) m.getKey(), (String) m.getValue() );
        }
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
        if( columns.size() > 0 && table != null ) {
            if( statement != null ) {
                statement.addStatement( toString() );
            } else {
                addStatement( toString() );
            }
        }
        table = null;
        columns.clear();
        conditions.clear();
    }

    public String toString()
    {
        String update = "update " + table + " set ";
        for( String column: columns ) {
            update += column + ", ";
        }
        update = update.substring( 0, update.length() - 2 );
        if( conditions.size() > 0 ) {
            update += " where ";
            for( String condition: conditions ) {
                update += condition + (isAnd ? " and " : " or ");
            }
            update = update.substring( 0, (isAnd ? update.length() - 5 : update.length() - 4) );
        }

        return update;
    }
}
