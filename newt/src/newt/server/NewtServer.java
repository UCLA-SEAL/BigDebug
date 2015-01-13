package newt.server;

import java.net.InetAddress;

import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.*;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import newt.common.*;

public class NewtServer {

    private Server server;

    public NewtServer() {
        server = new Server( port );
    }

    public void setHandler( ContextHandlerCollection contexts ) {
        server.setHandler( contexts );
    }

    public void start() throws Exception {
        server.start();
    }

    public void stop() throws Exception {
        server.stop();
        server.join();
    }

    public boolean isStarted() {
        return server.isStarted();
    }

    public boolean isStopped() {
        return server.isStopped();
    }

    private static final int    port = Configuration.masterPort;
    private static NewtState    newtState = null;

    protected static void initNewtServer() throws Exception
    {
        newtState = NewtState.getInstance();
    }

    public static void main( String[] args )
    {
        try {
            if( args.length > 0 ) {
                if( args[ 0 ].equals( "clean" ) ) {
                    Configuration.cleanDB = true;
                }
            } else {
                Configuration.cleanDB = false;
            }
            NewtServer.initNewtServer();
            NewtServer webServer = new NewtServer();
            ServletContextHandler context = new ServletContextHandler( ServletContextHandler.SESSIONS );
            context.setContextPath( "/" );
            context.addServlet( "newt.server.NewtHandler","/hessianrpc" );
            webServer.server.setHandler( context );
            webServer.start();
        } catch( Exception e ) {
            System.err.println( e.getMessage() );
            e.printStackTrace();
            System.out.println( "Error in starting Newt. Aborting..." );
            System.exit( 1 );
        }

       System.out.println( "Newt Server successfully started!" );
    }
}

