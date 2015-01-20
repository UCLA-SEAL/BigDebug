package newt.server;

import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.*;
import org.eclipse.jetty.servlet.ServletContextHandler;

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

    private static  int    port = -1; //Configuration.masterPort;

    //TODO Ksh
    private static void UpdateConfig(String mode) {
        Configuration.masterPort = 8898;
        Configuration.isMaster = false;
        if(mode.equals("master"))
        {
            Configuration.masterPort = 8899;
            Configuration.isMaster = true;
        }
        port = Configuration.masterPort;
        //return Configuration.masterPort;
    }

    private static NewtState    newtState = null;

    protected static void initNewtServer() throws Exception
    {
        newtState = NewtState.getInstance();
    }

    protected static void initNewtServer(String mode) throws Exception
    {
        UpdateConfig(mode);
        newtState = NewtState.getInstance();
    }

    public static void main( String[] args )
    {
        try {
            if( args.length > 0 ) {

                if( args[ 0 ].equals( "clean" ) ) {
                    Configuration.cleanDB = true;
                }
                else {
                    Configuration.cleanDB = false;
                }

            }

            //TODO Ksh Added logic to control master and peer mode via argument
            if(args.length > 1)
                NewtServer.initNewtServer(args[1]);
            NewtServer webServer = new NewtServer();
            ServletContextHandler context = new ServletContextHandler( ServletContextHandler.SESSIONS );
            context.setContextPath( "/" );
            context.addServlet("newt.server.NewtHandler", "/hessianrpc");
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

