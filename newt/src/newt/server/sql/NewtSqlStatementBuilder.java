package newt.server.sql;

import java.lang.*;
import java.util.*;

import java.sql.ResultSet;

import newt.common.Configuration;
import newt.server.NewtMySql;

public class NewtSqlStatementBuilder {
    private NewtMySql           newtMysql = null;
    protected int               numStatements = 0;
    protected boolean           isQuery = true;
    protected String            query = null;
    protected String            update = null;
    protected Vector<String>    statements = null;
    protected Vector<Vector>    params = null;
    
    public NewtSqlStatementBuilder()
    {
        newtMysql = new NewtMySql( Configuration.mysqlUser, Configuration.mysqlPassword );
        statements = new Vector<String>();
        params = new Vector<Vector>();
    }

    public NewtSqlStatementBuilder( boolean isQuery )
    {
        this();
        this.isQuery = isQuery;
    }

    public ResultSet execute()
    {
        ResultSet rs = null;
        if( numStatements < 1 ) {
        } else if( numStatements == 1 ) {
            if( isQuery && query != null ) {
                if( params != null && params.size() == 1 ) {
                    rs = newtMysql.executeQuery( query, params.get( 0 ) );
                   System.out.println(query);
                } else {
                    rs = newtMysql.executeQuery( query );
                   System.out.println(query);
                }
            } else if( update != null ) {
                if( params != null && params.size() == 1 ) {
                    newtMysql.executeUpdate( update, params.get( 0 ) );
                   System.out.println(update);
                } else {
                    newtMysql.executeUpdate( update );
                   System.out.println(update);
                }
            }
        } else if( statements != null && statements.size() > 0 ) {
            newtMysql.executeBatchUpdate( statements );
           System.out.println(statements);
        } else if( params != null && params.size() > 1 && !isQuery ) {
            newtMysql.executeBatchUpdate( update, params );
           System.out.println(update);
        }

        numStatements = 0;
        params.clear();
        statements.clear();
        query = null;
        update = null;
        return rs;
    }

    public NewtMySql getContainer()
    {
        return newtMysql;
    }

    public void setStatement( String statement )
    {
        if( isQuery ) {
            query = statement;
        } else {
            update = statement;
        }
    }

    public void addStatement( String statement )
    {
        numStatements++;
        setStatement( statement );
        statements.add( statement );
    }

    public void addStatements( Vector<String> statements )
    {
        numStatements += statements.size();
        this.statements.addAll( statements );
        setStatement( (String) statements.lastElement() );
    }

    public void addParam( Vector row )
    {
        numStatements++;
        params.add( row );
    }

    public void close()
    {
        newtMysql.close();
    }
}
