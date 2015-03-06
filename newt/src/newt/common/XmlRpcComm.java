package newt.common;

import java.lang.*;
import java.util.*;
import java.io.*;
import java.security.*;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;
import org.apache.xmlrpc.client.XmlRpcCommonsTransportFactory;
import org.apache.xmlrpc.client.util.ClientFactory;
import org.apache.xmlrpc.XmlRpcRequest;

public class XmlRpcComm {
    static class XmlRpcAsyncCallback implements org.apache.xmlrpc.client.AsyncCallback {
        public void handleResult(XmlRpcRequest pRequest, Object pResult)
        {
        }

        public void handleError(XmlRpcRequest pRequest, Throwable pError)
        {
            System.err.println( "Error in xmlrpc: " + pError.getMessage() );
        }
    }
    
    public static XmlRpcClient connectClient( String url )
    throws XmlRpcException, MalformedURLException
    {
        /* Create configuration. */
        XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();

        config.setServerURL( new URL( url ) );
        config.setEnabledForExtensions( true );  
        config.setConnectionTimeout( 60 * 1000 );
        config.setReplyTimeout( 60 * 1000 );

        XmlRpcClient client = new XmlRpcClient();
        /* Use Commons HttpClient as transport. */
        client.setTransportFactory( new XmlRpcCommonsTransportFactory( client ) );
        /* Set configuration. */
        client.setConfig( config );

        return client;
    }

    public static Object executeXmlRpc( XmlRpcClient client, String method, Object[] params )
    throws XmlRpcException
    {
        return client.execute( method, params );
    }
    
    public static Object executeXmlRpc( String url, String method, Object[] params )
    throws XmlRpcException, MalformedURLException
    {
        XmlRpcClient client = connectClient( url );
        return client.execute( method, params );
    }

    public static void executeAsyncXmlRpc( XmlRpcClient client, String method, Object[] params )
    throws XmlRpcException
    {
        client.executeAsync( method, params, new XmlRpcAsyncCallback() );
    }

    public static void executeAsyncXmlRpc( String url, String method, Object[] params )
    throws XmlRpcException, MalformedURLException
    {
        XmlRpcClient client = connectClient( url );
        client.executeAsync( method, params, new XmlRpcAsyncCallback() );
    }
}
