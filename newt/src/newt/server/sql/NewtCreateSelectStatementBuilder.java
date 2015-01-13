package newt.server.sql;

import java.lang.*;
import java.util.*;

import java.sql.ResultSet;

public class NewtCreateSelectStatementBuilder extends NewtSelectStatementBuilder {
    String              table = null;
    
    public NewtCreateSelectStatementBuilder()
    {
        super( false );
    }

    public void setTable( String DBName, String tableName )
    {
        table = DBName + "." + tableName;
    }

    public void setTable( String DBTableName )
    {
        table = DBTableName;
    }

    public void setQuery( boolean isAnd )
    {
        setQuery( isAnd, null );
    }

    public void setQuery( boolean isAnd, NewtSqlStatementBuilder statement )
    {
        if( table != null ) {
            String selectQuery = super.getQuery( isAnd );
            if( selectQuery != null ) {
                if( statement != null ) {
                    statement.addStatement( toString() + selectQuery );
                } else {
                    addStatement( toString() + selectQuery );
                }
            }
        }
        table = null;
        super.reset();
    }

    public String toString()
    {
        String update = "create table " + table + " as ";
        return update;
    }
}
