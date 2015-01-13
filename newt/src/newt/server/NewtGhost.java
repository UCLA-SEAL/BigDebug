package newt.server;

import java.lang.*;
import java.util.*;
import java.io.*;

import newt.actor.*;
import newt.server.sql.*;

public class NewtGhost extends Thread {
    Vector<String> ghostSource = null;
    Vector<String> ghostDestination = null;
    String ghostTable = null;
    String sourceTable = null;
    String destinationTable = null;
    NewtState newtState = null;
    
    public NewtGhost( Vector<String> ghostSource, Vector<String> ghostDestination, String sourceTable, String destinationTable, NewtState newtState )
    {
        this.ghostSource = ghostSource;
        this.ghostDestination = ghostDestination;
        this.destinationTable = destinationTable;
        this.sourceTable = sourceTable;
        this.ghostTable = "ghost_" + destinationTable;
        this.newtState = newtState;
    }

    public void run()
    {
        TreeSet<FileLocatable> source = new TreeSet<FileLocatable>();
        for( String l: ghostSource ) {
            source.add( FileLocatable.resolve( l ) );
        }
        
        TreeSet<FileLocatable> destination = new TreeSet<FileLocatable>();
        for( String l: ghostDestination ) {
            destination.add( FileLocatable.resolve( l ) );
        }

        NewtCreateStatementBuilder createStatement = new NewtCreateStatementBuilder();
        createStatement.setTable( "Newt", ghostTable );
        createStatement.addColumn( "Input", "varchar(255)", false );
        createStatement.addIndex( "Input", 32 );
        createStatement.addColumn( "Output", "varchar(255)", false );
        createStatement.addIndex( "Output", 32 );
        createStatement.setQuery();
        createStatement.execute();
        createStatement.close();

        ghost( source, destination );
       System.out.println( "Finished ghosting: " + ghostTable );

        newtState.sendGhostComplete( sourceTable, destinationTable, ghostTable );
    }

    
    public void ghost( TreeSet<FileLocatable> source, TreeSet<FileLocatable> destination )
    {
        NewtBulkInsertStatementBuilder bulkInsertStatement = new NewtBulkInsertStatementBuilder();
        bulkInsertStatement.setTable( "Newt", ghostTable );
        bulkInsertStatement.setRowLength( 2 );
        
        Iterator<FileLocatable> i = source.iterator();
        Iterator<FileLocatable> j = destination.iterator();
        if( i.hasNext() ) {
            FileLocatable l = i.next();
            while( j.hasNext() ) {
                FileLocatable f = j.next();
                int result = l.overlaps( f );
                while( result == 0 ) {
                    bulkInsertStatement.addRow( new Object[] { l.toString(), f.toString() } );
                    if( j.hasNext() ) {
                        f = j.next();
                        result = l.overlaps( f );
                    } else {
                        break;
                    }
                }
                if( result <= 0 ) {
                    i.remove();
                    if( i.hasNext() ) {
                        l = i.next();
                        j = destination.iterator();
                        continue;
                    } else {
                        break;
                    }
                }
                if( result > 0 ) {
                    j.remove();
                    continue;
                }
            }
        }

        bulkInsertStatement.setQuery();
        bulkInsertStatement.execute();
        bulkInsertStatement.close();
    }
}
