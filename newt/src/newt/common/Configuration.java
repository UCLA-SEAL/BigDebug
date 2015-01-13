/*
 * Very simple configuration system.  One file.  One Object.  Can be used for client and server. 
 *
 */ 

package newt.common;

import java.util.HashMap;
import java.util.Vector;
import java.util.Iterator;
import java.io.*;
import java.net.InetAddress;

import newt.utilities.Utilities;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.PropertyConfigurator;

public class Configuration {
	
    private static String configFilename = null;
    private static HashMap javaPathIndex;
    private static HashMap classPathIndex;
    private static HashMap newtPathIndex;
    private static String[] ipPrefixes;
    private static boolean isConfigured;
    public static boolean doServerCalls = true;
    public static String newtPath = ".";
    public static String masterHostName = "localhost";
	public static int masterPort = 8899; //8899 for master //<----------------------------- Needed for running locally. Set when running peer
    //public static int masterPort = 8898; //8898 for peer //<----------------------------- Needed for running locally. Set when running peer
    public static String rsrcFileName = "/conf/resources.cfg"; // all 3 relative to newt.cfg values
    public static String logFileName = "/log/newt.log";
    public static String tempDir = Utilities.GetTempDirectory();
    public static String clientLogDir = tempDir + "/newt/client/log";
    public static String log4jName = "/conf/log4j.cfg";
    public static boolean cleanDB = false;
    public static String sqlName = "";
    public static String sqlPassword = "";
    public static String sshName = "";
    public static String sshIdentity = "";
    public static String sshKnownHosts = "";
    private static InetAddress localHostAddress;    
    public static String mysqlUser = Utilities.GetMySqlUser();
    public static String mysqlPassword = Utilities.GetMySqlPassword();

    /**
     * If set to true, Newt will attempt to place provenance tables on the
     * same machine as the actor.
     */
    public static final boolean localTablePlacement = true;


    /**
     * The default constructor assume a default location for
     * the configuration file.
     */
    public Configuration() {
	configFilename = "./conf/newt.cfg";
	config();
    }
    
    /**
     * This constructor uses a user-supplied location for the
     * configuration file.
     */    
    public Configuration(String confFile) {
    	configFilename = confFile;
	    config();
    }
    
    private void config() {
    	int i = 0;
	    localHostAddress = null;
    	isConfigured = false;
	    javaPathIndex = new HashMap();
    	classPathIndex = new HashMap();
	    newtPathIndex = new HashMap();
    	parseConfigFile();
	    initPaths();
    	initLogging();
	    if (isConfigured) {
	        ipPrefixes = new String[javaPathIndex.size()];
    	    Iterator ipPrefixesIt = javaPathIndex.keySet().iterator();
	        while (ipPrefixesIt.hasNext())
		        ipPrefixes[i++] = (String)ipPrefixesIt.next();
    	} else {
	       System.out.println("(Newt): Newt was not configured properly. Check entries in ./newt.cfg");
    	}
	    findLocalHost();    
    }
    

    /**
     * On this host, append paths with variable
     * newtPath
     */
    public static void initPaths() {
    	rsrcFileName = new String(newtPath+rsrcFileName);
	    logFileName = new String(newtPath+logFileName);
    	clientLogDir = new String(newtPath+clientLogDir);
	    log4jName = new String(newtPath+log4jName);
    }


    /**
     * Initialize the logging mechanism.
     * This handles log creation for all inheriters.
     */
    public static void initLogging(){
//        if (Logger.getRoot().getAllAppenders() instanceof NullEnumeration) {
	    if (new File(Configuration.log4jName).exists()){
		    PropertyConfigurator pc = new PropertyConfigurator();
    		pc.configureAndWatch (Configuration.log4jName, 10*1000 /* check every 10 s */);
	    } else {
	    	System.out.println("Logging unable to use file:"+Configuration.log4jName);
		    PatternLayout pl = new PatternLayout ("%r [%t] %c: %m\n");
    		ConsoleAppender ca = new ConsoleAppender (pl);
	    	Logger.getRoot ().addAppender (ca);
		    Logger.getRoot ().setLevel (Level.ERROR);
	    }
//	} 
    }
	       
    /**
     * Parse the configuration file and initialize the configuration
     * variables.
     */
    private static void parseConfigFile() {
	int lineNum = 0;
	BufferedReader reader = null;
	try {
	    reader = new BufferedReader( new FileReader (new File( configFilename)));
	} catch (FileNotFoundException e) {
	   System.out.println("(Newt): Cannot find newt.configuration: "+configFilename);
	    return;
	}
	String line = null;
	String[] token;
	try { 
	    while ((line = reader.readLine()) != null) {
		line = line.trim();
		if ((line.length()>0 && line.charAt(0) == '#') || line.equals("")) {
		    lineNum++;
		    continue;
		}
		token = line.split("\\s+");
		if (token.length == 1) {
		    /*
		    if (token[0].equals("syncless")) {
			syncless = true;
			lineNum++;
			continue;
		    }
		    */
		}
		if (token.length == 2) {
		    if (token[0].equals("localhost")) {
			localHostAddress = InetAddress.getByName(token[1]);
			lineNum++;
			continue;
		    }
		    if (token[0].equals("sshName")) {
			sshName = new String(token[1]);
			lineNum++;
			continue;
		    }
		    if (token[0].equals("rsrcFileName")) {
			rsrcFileName = new String(token[1]);
			lineNum++;
			continue;
		    }
		    if (token[0].equals("logFileName")) {
			logFileName = new String(token[1]);
			lineNum++;
			continue;
		    }
		    if (token[0].equals("masterHostName")) {
			masterHostName = new String(token[1]);
			lineNum++;
			continue;
		    }
		    if (token[0].equals("masterPort")) {
			masterPort = new Integer(token[1]).intValue();
			lineNum++;
			continue;
		    }
		    if (token[0].equals("clientLogDir")) {
			clientLogDir = new String(token[1]);
			lineNum++;
			continue;
		    }
		    if (token[0].equals("sshIdentity")) {
			sshIdentity = new String(token[1]);
			lineNum++;
			continue;
		    }
		    if (token[0].equals("sshKnownHosts")) {
			sshKnownHosts = new String(token[1]);
			lineNum++;
			continue;
		    }
		}
		if (token.length != 4) {
		   System.out.println("(Newt): Line "+lineNum+" in "+configFilename+" is not well formatted.");
		    lineNum++;
		    continue;
		}
		//System.out.println("Adding net:"+token[0]);
		javaPathIndex.put(token[0], token[1]);
		classPathIndex.put(token[0], token[2]);
		newtPathIndex.put(token[0], token[3]);
		isConfigured = true;
	    }
	    reader.close();
	} catch (IOException ioe) {
	   System.out.println("(Newt): Exception while reading configuration file:"+ioe);
	}
    }

    private static String getPrefix(String ip) {
	if (ipPrefixes == null)
	    return null;
	for (int i = 0; i < ipPrefixes.length; i++){
	    if (ip.startsWith(ipPrefixes[i])) {
		return ipPrefixes[i];
	    }
	}
	return null;
    }
		
    private static void findLocalHost() {
	if (localHostAddress != null) {	
	    return; // config file entry
	}
	try {
	    localHostAddress = InetAddress.getLocalHost();	
	}catch (Exception e) {
	   System.out.println("(Newt): Error while getting information about network interfaces on the local machine:"+e);
	}
	System.out.println("(Newt): Configuration: localhost:"+localHostAddress);
    }

    public static void setLocalHost(InetAddress ip) {
	localHostAddress = ip;
	System.out.println("(Newt): Configuration: localhost:"+localHostAddress);
    }
	
    public static InetAddress getLocalHostAddress() {
	return localHostAddress;
    }
		
    public boolean isConfigured() {
	return isConfigured;
    }
	
    public String getClassPath(String ip) {
	return (String)classPathIndex.get(getPrefix(ip));
    }
	
    public String getJavaPath(String ip) {
	return (String)javaPathIndex.get(getPrefix(ip));
    }

    public String getNewtPath(String ip) {
	return (String)newtPathIndex.get(getPrefix(ip));
    }
	
}
