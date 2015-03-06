package newt.server.sql;

import java.lang.*;
import java.util.*;

import java.sql.ResultSet;

public class NewtSelectStatementBuilder extends NewtSqlStatementBuilder {
    Vector<String>      tables = null;
    Vector<String>      columns = null;
    Vector<String>      conditions = null;
    boolean             isAnd = true;
    
    public NewtSelectStatementBuilder()
    {
        super( true );
        tables = new Vector<String>();
        columns = new Vector<String>();
        conditions = new Vector<String>();
    }

    public NewtSelectStatementBuilder( boolean isQuery )
    {
        super( isQuery );
        tables = new Vector<String>();
        columns = new Vector<String>();
        conditions = new Vector<String>();
    }

    public void addTable( String DBName, String tableName )
    {
        tables.add( DBName + "." + tableName );
    }

    public void addTable( String DBTableName )
    {
        tables.add( DBTableName );
    }

    public void addTables( String[] DBTableNames )
    {
        tables.addAll( Arrays.asList( DBTableNames ) );
    }

    public void addColumn( String columnName )
    {
        columns.add( columnName );
    }

    public void addColumn( String tableName, String columnName )
    {
        addColumn( tableName + "." + columnName );
    }

    public void addColumns( String[] tableColumnNames )
    {
        columns.addAll( Arrays.asList( tableColumnNames ) );
    }

    public void addAsColumn( String columnName, String asName )
    {
        columns.add( columnName + " as " + asName );
    }

    public void addAsColumn( String tableName, String columnName, String asName )
    {
        addAsColumn( tableName + "." + columnName, asName );
    }

    public void addAsColumns( HashMap<String, String> tableColumnAsNames )
    {
        Set tableColumnAsNameEntries = tableColumnAsNames.entrySet();
        for( Object o: tableColumnAsNameEntries ) {
            Map.Entry m = (Map.Entry) o;
            addAsColumn( (String) m.getKey(), (String) m.getValue() );
        }
    }

    public void addAbsoluteCondition( String columnName, String value )
    {
        conditions.add( columnName + " = '" + value + "'" );
    }

    public void addAbsoluteCondition( String tableName, String columnName, String value )
    {
        addAbsoluteCondition( tableName + "." + columnName, value );
    }

    public void addAbsoluteCondition( String columnName, int value )
    {
        conditions.add( columnName + " = " + value );
    }

    public void addAbsoluteCondition( String tableName, String columnName, int value )
    {
        addAbsoluteCondition( tableName + "." + columnName, value );
    }

    public void addAbsoluteConditions( String columnName, Object[] values )
    {
        String condition = "";
        for( Object value: values ) {
            if( value instanceof String ) {
                condition += columnName + " = '" + (String) value + "' or ";
            } else if( value instanceof Integer ) {
                condition += columnName + " = " + (Integer) value + " or ";
            }
        }

        condition = condition.substring( 0, condition.length() - 4 );
        if( values.length > 1 ) {
            condition = "( " + condition + " )";
        }
        conditions.add( condition );
    }

    public void addAbsoluteCondition( String tableName, String columnName, Object[] values )
    {
        addAbsoluteConditions( tableName + "." + columnName, values );
    }

    public void addJoinCondition( String firstTableName, String firstColumnName, String secondTableName, String secondColumnName )
    {
        conditions.add( firstTableName + "." + firstColumnName + " = " + secondTableName + "." + secondColumnName );
    }

    public void addJoinCondition( String firstTableName, String firstColumnName, String[] secondTableColumnNames )
    {
        String condition = "";
        for( String secondTableColumnName: secondTableColumnNames ) {
            condition +=firstTableName + "." + firstColumnName + " = " + secondTableColumnName + " or ";
        }
        condition = condition.substring( 0, condition.length() - 4 );
        if( secondTableColumnNames.length > 1 ) {
            condition = "( " + condition + " )";
        }
        conditions.add( condition );
    }

    protected String getQuery( boolean isAnd )
    {
        this.isAnd = isAnd;
        if( columns.size() > 0 && tables.size() > 0 ) {
            return toString();
        }
        return null;
    }

    public void setQuery( boolean isAnd )
    {
        setQuery( isAnd, null );
    }

    public void setQuery( boolean isAnd, NewtSqlStatementBuilder statement )
    {
        this.isAnd = isAnd;
        if( columns.size() > 0 && tables.size() > 0 ) {
                if( statement != null ) {
                    statement.addStatement( toString() );
                } else {
                    addStatement( toString() );
                }
        }
        reset();
    }

    protected void reset()
    {
        tables.clear();
        columns.clear();
        conditions.clear();
    }

    public String toString()
    {
        String query = "select distinct ";
        for( String column: columns ) {
            query += column + ", ";
        }
        query = query.substring( 0, query.length() - 2 );
        query += " from ";
        for( String table: tables ) {
            query += table + ", ";
        }
        query = query.substring( 0, query.length() - 2 );
        if( conditions.size() > 0 ) {
            query += " where ";
            for( String condition: conditions ) {
                query += condition + (isAnd ? " and " : " or ");
            }
            query = query.substring( 0, (isAnd ? query.length() - 5 : query.length() - 4) );
        }

        return query;
    }
}
