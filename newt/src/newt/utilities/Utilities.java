package newt.utilities;

import java.util.ArrayList;

import newt.client.NewtXmlDOM;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

public class Utilities 
{
	public static String GetTempDirectory()
	{
		String tempDir = "/tmp";
        String newtHome = System.getenv( "NEWT_HOME" );
        if( newtHome == null ) {
           System.out.println( "NewtClient::Configuration not found. Export NEWT_HOME." );
            System.exit( 1 );
        }
	    String configFile = newtHome + "/" + "NewtConfig.xml";
        Node root = null;
        NewtXmlDOM dom = new NewtXmlDOM();
		
        try 
		{
            Document doc = dom.doc( configFile );
            root = dom.root( doc );
			Node n = root;
			if(n.getNodeType() == Node.ELEMENT_NODE)
			{
				//try to find master ip
               if( n.getNodeName().equals( "Configuration" ) ) 
			   {
                    n = n.getFirstChild();
                    while( n != null ) 
					{
						if( n.getNodeName().equals( "TempDir" ) ) {
							Node v = dom.getAttribute( n, "value" );
							tempDir = v.getNodeValue();
							break;
						}
                        n = (Node)n.getNextSibling();
                    }
                }
			}
			
			//System.out.println("Utilities::GetTempDirectory::TempDir=" + tempDir);
        } 
		catch( Exception e ) 
		{
           System.out.println( "Utilities::GetTempDirectory - Failed to process config file." );
           System.exit( 1 );
        }

		return tempDir;

	}
	
	public static String GetMySqlUser()
	{
		String mySqlUser = "root";
        String newtHome = System.getenv( "NEWT_HOME" );
        if( newtHome == null ) {
           System.out.println( "NewtClient::Configuration not found. Export NEWT_HOME." );
            System.exit( 1 );
        }
	    String configFile = newtHome + "/" + "NewtConfig.xml";
        Node root = null;
        NewtXmlDOM dom = new NewtXmlDOM();

        try 
		{
            Document doc = dom.doc( configFile );
            root = dom.root( doc );
			Node n = root;
			if(n.getNodeType() == Node.ELEMENT_NODE)
			{
				//try to find master ip
		        if( n.getNodeName().equals( "Mysql" ) ) {
		            ArrayList<Node> children = dom.childrenByTag( n, "User" );
		            Node m = children.get( 0 );
		            Node v = dom.getAttribute( m, "value" );
		            mySqlUser = v.getNodeValue();
		        }
			}
		}
        catch(Exception e)
        {
            System.out.println( "Utilities::GetMySqlUser - Failed to process config file." );
            System.exit( 1 );        	
        }
        
		//System.out.println("Utilities::GetMySqlUser::MySqlUser=" + mySqlUser);
        return mySqlUser;
	}
	
	public static String GetMySqlPassword()
	{
		String mySqlPassword = "root";
        String newtHome = System.getenv( "NEWT_HOME" );
        if( newtHome == null ) 
        {
           System.out.println( "NewtClient::Configuration not found. Export NEWT_HOME." );
           System.exit( 1 );
        }
        
	    String configFile = newtHome + "/" + "NewtConfig.xml";
        Node root = null;
        NewtXmlDOM dom = new NewtXmlDOM();

        try 
		{
            Document doc = dom.doc( configFile );
            root = dom.root( doc );
			Node n = root;
			if(n.getNodeType() == Node.ELEMENT_NODE)
			{
				//try to find master ip
		        if( n.getNodeName().equals( "Mysql" ) ) 
		        {		            
		            ArrayList<Node> children = dom.childrenByTag( n, "Password" );
		            Node m = children.get( 0 );
		            Node v = dom.getAttribute( m, "value" );
		            mySqlPassword = v.getNodeValue();
		        }
			}
		}
        catch(Exception e)
        {
            System.out.println( "Utilities::GetMySqlPassword - Failed to process config file." );
            System.exit( 1 );        	
        }
        
		//System.out.println("Utilities::GetMySqlPassword::MySqlPassword=" + mySqlPassword);
        return mySqlPassword;
	}
	
	public static String GetDefaultMasterIPFromConfig()
	{
        String newtHome = System.getenv( "NEWT_HOME" );
        if( newtHome == null ) {
           System.out.println( "NewtClient::Configuration not found. Export NEWT_HOME." );
            System.exit( 1 );
        }

	    String configFile = newtHome + "/" + "NewtConfig.xml";
        Node root = null;
        NewtXmlDOM dom = new NewtXmlDOM();
		String masterIP = "0.0.0.0";
		
        try 
		{
            Document doc = dom.doc( configFile );
            root = dom.root( doc );
			Node n = root;
			if(n.getNodeType() == Node.ELEMENT_NODE)
			{
				//try to find master ip
               if( n.getNodeName().equals( "Configuration" ) ) 
			   {
                    n = n.getFirstChild();
                    while( n != null ) 
					{
						if( n.getNodeName().equals( "Nodes" ) ) {
							ArrayList<Node> children = dom.childrenByTag( n, "Master" );
							Node m = children.get( 0 );
							Node v = dom.getAttribute( m, "value" );
							masterIP = v.getNodeValue();
							break;
						}
                        n = (Node)n.getNextSibling();
                    }
                }
			}
			
			//System.out.println("NewtClient::GetMasterIPFromConfig::IP=" + masterIP);
        } 
		catch( Exception e ) 
		{
           System.out.println( "Failed to process config file." );
           System.exit( 1 );
        }
		
		return masterIP;
	}

}
