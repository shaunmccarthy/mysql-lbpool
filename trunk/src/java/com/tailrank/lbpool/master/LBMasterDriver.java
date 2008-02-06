package com.tailrank.lbpool;

import java.net.*;
import java.sql.*;
import java.util.*;
import java.util.regex.*;

import org.apache.log4j.*;

/**
 * Driver which watches for IOExceptions when working with the master and
 * reconnect them based on a rendezvous service.
 * 
 * CREATE DATABASE lbpool;
 * 
 * USE lbpool;
 * 
 * CREATE TABLE MASTER
 * (
 * ID INTEGER NOT NULL AUTO_INCREMENT,
 * POOLNAME VARCHAR (40) NOT NULL,
 * HOSTNAME VARCHAR (40) NOT NULL,
 * OFFLINE BIT default 0,
 * PRIMARY KEY(ID),
 * UNIQUE (POOLNAME(40), HOSTNAME(40)),
 * INDEX HOSTNAME (HOSTNAME(40))
 * );
 * 
 */
public class LBMasterDriver implements Driver {

    public static Logger log = Logger.getLogger( LBMasterDriver.class );

    /**
     * True if we should debug this driver. 
     */
    public static boolean DEBUG = false;

    /**
     * Internal (real) JDBC driver.
     */
    private Driver delegate = null;

    //register the driver with the DriverManager
	static {

		try {
			java.sql.DriverManager.registerDriver( new LBMasterDriver() );
		} catch (SQLException E) {
            //probably will never happen in production
			throw new RuntimeException("Can't register driver!");
		}

	}

	/**
	 * Construct a new driver and register it with DriverManager
	 */
	public LBMasterDriver() throws SQLException {}

    /**
     * Given an lbmaster: URL we first connect to the rendezvous service to find
     * the real master mapping to use.  
     */
    public Connection connect( String url, java.util.Properties info )
        throws SQLException {
        
        Pattern p = Pattern.compile( "lbmaster:([^:]+):([^:]+):([^?]+)(.*)" );
        Matcher m = p.matcher( url );
        
        if ( ! m.find() ) {
            return null;
        }

        String host         = m.group( 2 );
        String database     = m.group( 3 );
        String connect_url  = m.group( 4 );

        String user         = (String)info.get( "user" );
        String password     = (String)info.get( "password" );
        
        // Connection conn     = getRealConnection( real_host, profile );

        // FIXME:
        return null;

    }
    
    public boolean acceptsURL( String url ) throws SQLException {
        return url.startsWith( "lbmaster:" );

    }

    public DriverPropertyInfo[] getPropertyInfo(String url, java.util.Properties info)
        throws SQLException {

        if ( delegate != null ) {
            return delegate.getPropertyInfo( url, info );
        }

        //not ready to use yet.  Not sure if this will cause a problem.
        return null;
        
    }

    public int getMajorVersion() {
        return 1;
    }

    public int getMinorVersion() {
        return 0;
    }

    public boolean jdbcCompliant() {
        return true;
    }

    // **** static methods ******************************************************

    protected static void debug( String msg ) {
        
        if ( DEBUG ) {
            System.out.println( "lbmaster: " + msg );
        }

    }

    /** 
     * Given the name of a DNS host to query determine the best MySQL slave to
     * use for this connection.
     */
    protected String getRealMasterHost( String name ) {

        return null;

    }

    /**
     * Get a connection to a MySQL server but totally bypass the normal
     * DriverManager infrastructure.  There's a deadlock condition that can
     * occur in JDK 1.4 where the DriverManager synchronizes and then attempts
     * to connect to itself.
     */
    static Connection getMySQLConnection( String url, String user, String password ) 
        throws SQLException {
        
        java.util.Properties info = new java.util.Properties();
        
        if (user != null) 
            info.put("user", user);

        if (password != null)
            info.put("password", password);

        com.mysql.jdbc.NonRegisteringDriver driver = 
            new com.mysql.jdbc.NonRegisteringDriver();

        return driver.connect( url, info );

    }

}

