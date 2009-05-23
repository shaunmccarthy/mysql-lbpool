package com.tailrank.lbpool;

import java.net.*;
import java.sql.*;
import java.util.*;
import java.util.regex.*;

import org.apache.log4j.*;

//import org.apache.commons.benchmark.*;

/**
 * The lbpool project provides a load balancing JDBC driver for use with DB
 * connection pools.  It wraps a normal JDBC driver providing reconnect
 * semantics in the event of additional hardware availability, partial system
 * failure, or uneven load distribution.  It also evenly distributes all new
 * connections among slave DB servers in a given pool.  Each time connect() is
 * called it will attempt too use the best server with the least system load.
 * 
 * The biggest scalability issue with large applications that are mostly READ
 * bound is the number of transactions per second that the disks in your cluster
 * can handle.  You can generally solve this in two ways.
 * 
 * 1. Buy bigger and faster disks with expensive RAID controllers.
 * 2. Buy CHEAP hardware on CHEAP disks but lots of machines.
 * 
 * We prefer the cheap hardware approach and lbpool enables this functionality.
 * 
 * Even if you *did* manage to use cheap hardware most load balancing hardware
 * is expensive, requires a redundant balancer (if it were to fail), and seldom
 * has native support for MySQL.
 * 
 * The lbpool driver addresses all these needs.
 * 
 * The original solution was designed for use within MySQL replication clusters.
 * This generally involves a master server handling all writes with a series of
 * slaves which handle all reads.  In this situation we could have hundreds of
 * slaves and lbpool would load balance queries among the boxes.  If you need
 * more read performance just buy more boxes.
 * 
 * If any of them fail it won't hurt your application because lbpool will simply
 * block for a few seconds and move your queries over to a new production
 * server.
 * 
 * While currently designed for MySQL this could easily be updated to support
 * PostgresQL or any other DB that supports replication.
 * 
 * ** Scalability
 * 
 * In the current mechanism the driver should be able to scale to dozens and
 * hundreds of slave servers.  Generally speaking though everything has a limit
 * to which it can scale and lbpool is no exception.  
 * 
 * For 99% of deployed applications we should be able to scale very well outside
 * of the box.  The NDB cluster package from MySQL seems like the way forward
 * but until MySQL 5.1 is available with disk support this solution will be out
 * of the price range of most customers since it requires an data be held in
 * memory.
 *
 * Even with NDB there's a need to pool the NDB SQL nodes and even though the
 * data nodes support failover the client still connects to one SQL node. If
 * this were to crash the client would be offline.  Using lbpool along with NDB
 * would solve this problem but to date it hasn't been done.
 * 
 * ** Load balancing policy
 * 
 * When the client first boots we attempt to connect to the host with the best
 * chance of running our queries efficiently.  We use the number of concurrent
 * connections, load, and slave status to accomplish this.  We keep using this
 * host until our policy system determines that its no longer beneficial to do
 * so.  We use the following tests to determine if a host can is offline and can
 * no longer can be used.
 * 
 *  1. It physically goes away (hardware issue, or network connectivity issue)
 *  
 *  2. Replication breaks (from SHOW SLAVE STATUS output)
 * 
 *  3. Replication falls behind.
 * 
 *  4. The load on the box is too high.
 * 
 *  5. Has too many connections.
 * 
 * Most of the time the load on the box will be to high and we'll simply pick
 * another box and start running queries there.  After a few minutes the box
 * will probably correct and the system will continue functioning properly.
 * 
 *******************************************************************************
 * Features
 * 
 * - Runtime rebalancing.  If other JDBC drivers disconnect and leave the
 *   cluster unbalanced we attempt to rebalance the cluster.  Hosts are also
 *   rebalanced if they go offline and then become available again.  This helps
 *   evenly distribute load across teh cluster.
 * 
 * - Even if ALL the servers in your cluster go offline the driver can handle
 *   this and will block until at least ONE machine comes back online.  This
 *   means your application will just freeze and not break throwing
 *   SQLExceptions from broken servers.
 * 
 * - If a machine is flagged offline, existing connections will be disconnected
 *   from this slave and reconnected to another production machine (even if they
 *   are sitting idle on an webserver for hours).
 * 
 * - Since the configuration is dynamic (and done on the master) machines can be
 *   added or removed at anytime.  This can allow you to take a machine offline,
 *   reconfigure mysql (ALTER/REPAIR a table, etc), and then put it back online
 *   when maintenance is complete.
 * 
 * - If a slave physically goes offline it won't interrrupt service.  All
 *   internal exceptions are caught and the query is replayed on another server.
 * 
 * - If replication breaks or falls behind the existing connections will be
 *   re-connected to working MySQL boxes without any interruption in service.
 * 
 * - DB servers can be flagged offline and within the polling interval (default
 *   of 2 minutes) they will be removed from production.  This will allow you to
 *   run ALTER TABLE or REPAIR on production machines without affecting clients.
 *   Just remove them from production one at a time and your system shouldn't be
 *   affected.
 * 
 *******************************************************************************
 * Configuration 
 * 
 * 1. Create a db.mydomain.com DNS record with the IP address of your Master DB
 *    server
 *
 * 2. Update your JDBC driver URL to use the new syntax:
 * 
 *    lbpool:com.mysql.jdbc.Driver:db.tailrank.com:blogindex
 *
 *    You'll probably also need to add holdResultsOpenOverStatementClose in your
 *    connections string.  This is a feature I introduced as a patch and Mark
 *    Matthews re-implemented in the mainline JDBC driver.  Since MySQL 4.1
 *    results sets are just byte arrays we can cheat a bit and allow use of
 *    result sets even after a JDBC statement has been closed.
 *  
 * 3. Grant the MySQL REPLICATION CLIENT permission to the MySQL user the JDBC
 *    driver will connect as:
 * 
 *    GRANT REPLICATION CLIENT ON *.* TO 'blogindex'@'%.tailrank.com';
 *
 *******************************************************************************
 * Schema
 *******************************************************************************
 * 
 * The following schema needs to be setup on the master.  This tells lbpool
 * which hosts to use in production and allows you to flag hosts to go offline
 * if necessary. Note that it will take 60 seconds or so for lbpool to stop
 * using this host so its generally a good idea to flag if as offline and then
 * wait until no other lbpool instances are using it.
 * 
 * On your master exec:
 * 
 * CREATE DATABASE lbpool;
 * 
 * USE lbpool;
 * 
 * CREATE TABLE HOST
 * (
 *  ID INTEGER NOT NULL AUTO_INCREMENT,
 *  POOLNAME VARCHAR (40) NOT NULL,
 *  HOSTNAME VARCHAR (40) NOT NULL,
 *  OFFLINE BIT default 0,
 *  PRIMARY KEY(ID),
 *  UNIQUE (POOLNAME(40), HOSTNAME(40)),
 *  INDEX HOSTNAME (HOSTNAME(40))
 *  );
 * 
 * FIXME: add MAX_CONNECTIONS here... we don't want individual machines to get
 * overwhelmed.
 *
 * GRANT SELECT ON lbpool.* TO 'myuser'@'%.mydomain.com' IDENTIFIED BY 'mypasswd' ;
 *  
 * FLUSH PRIVILEGES;
 * 
 * **** 
 * Adding hosts
 * 
 * You can then run the following SQL to add hosts to your cluster:
 * 
 * USE lbpool;
 * INSERT INTO HOST (POOLNAME, HOSTNAME) VALUES ( 'web.db.mydomain.com', 'db3.mydomain.com' );
 * 
 * **** 
 * Marking hosts offline:
 * 
 * UPDATE HOST SET OFFLINE=1 WHERE HOSTNAME = 'db3.mydomain.com';
 * 
 *******************************************************************************
 * Future Development:
 * 
 * **
 * 
 * While very advanced the lbpool driver isn't perfect. If you're working with
 * highly heterogenous hardware the driver will attempt evenly distribute load
 * across the servers.  We need to build a load monitoring agent which will run
 * on each of the hosts and update the master schema.  This way the drivers can
 * throttle a host thats experiencing high load.
 *
 * To avoid this problme we advice you:
 * 
 * - Don't run custom SQL on a production load balance server.  Anything that
 *   will increase system load will affect cluster performance.  For example if
 *   you have a 3rd party application running it could cause system load to rise
 *   which will hurt the balancing algorithsm.  For example if you run 'find /'
 *   on a box thats production it will hurt the balanced performance.
 * 
 * - Support a new LOADFACTOR column so that we can distribute load by a factor
 *   so that certain hosts take more load.
 * 
 * ** 
 *
 * It would be nice to have event based (instead of poll based) monitoring of
 * slave status.  I'd like to know the SECOND a slave breaks so that I can mark
 * that host offline.  I'll probably need support from the MySQL server for this
 * to work.
 * 
 *******************************************************************************
 * 
 * Support for MySQL multi-master replication:
 * 
 * See:
 * http://dev.mysql.com/doc/refman/5.0/en/replication-auto-increment.html
 * 
 * For more info.
 * 
 * 
 * Integrate commons-benchmark so I can export performance stats from this
 * package for monitoring purposes.
 * 
 * @author Kevin Burton (burton@tailrank.com)
 * 
 */

/**
 * 
 * TODO:
 *
 * - Sun Sep 30 2007 08:53 PM (burton@tailrank.com): new HEARTBEAT table which
 *   is a MEMORY table which allows EACH host to
 *
 *   It should probably look like:
 * 
 * 
 * - Measure mean throughput of executing SQL with the benchmark package.  If
 *   the profile of a certain box seems too high we should attempt to throttle
 *   it a bit.
 * 
 * - Right now load is NOT a factor in selecting the best host. I'm not even
 *   sure if this is used right now.
 * 
 * - Reconnect will NEVER happen if the load is TOO HIGH on a box.. we won't
 *   even have semantics for this.  Add it in the poller.
 * 
 * - I can simulate this by running a SELECT * FROM PENDING_POST_NODE and
 *   dumping it into a file.
 * 
 * - How do we do monitoring on this code?  If boxes are internally
 *   marked offline we need to send out a SMS page.
 * 
 * - Computing the load is a BIT difficult ...I can't use the TOTAL load because
 *   one or two connections might be taking a long time but not really hurting
 *   overall system performance.  I guess I should really assume if 80% of the
 *   current connections are > N seconds that its falling behind... how do I get
 *   this number to scale though.
 * 
 **** HISTORY ******************************************************************
 * 
 * - If a server is ADDED/REMOVED from production the load won't be re-balanced
 *   correctly
 * 
 * - When a box is flagged offline....... how quickly are the connections
 *   re-established and in WHAT thread?  it would be NICE if they were used
 *   right THEN.
 * 
 * - Strange networking tests we need to do:
 * 
 * - Test to make sure the lbdriver marks boxes offline when replication breaks.
 * 
 * - Mangled SQL won't take the whole cluster offline
 * 
 * - NO IP ADDRESSES(should work)
 * 
 * - Broken IP addresses (wrong servers)
 * - will behave as no servers
 * 
 * - Unable to connect to any hosts
 * - will spin on existing connections and throw exceptions on new connections.
 * 
 * - Hosts break at runtime.
 * 
 * - I did an /etc/init.d/mysql stop with one connection running against it
 * and it had NO problems... threw NO exceptions and just kept running fine.
 * 
 * - A host is REMOVED ... I think that if this happens existing boxes would STILL
 * be used which is a problem. The would need to be marked offline and not used
 * anymore.  This depends on the DNS configuration bug though.
 * 
 * - This is complete but we STILL need to test it...
 * 
 */

public class LBDriver implements Driver {

    public static Logger log = Logger.getLogger( LBDriver.class );

    /**
     * Maximum number of connection attempts before an exception is thrown or
     * the client blocks forever.  If -1 we block for infinity.
     */
    public static int MAX_CONNECTION_ATTEMPTS = -1;

    /**
     * Enable/disable the poller thread.  For production systems this should be
     * true.
     */
    public static boolean ENABLE_POLLER_THREAD = true;

    /**
     * How fast should be poll hosts to determine their status.  Ideally in the
     * future only one driver would poll but for the most part this is SO
     * efficient on the MySQL side that it really doesn't matter.
     */
    public static long HOST_POLL_INTERVAL = 15L * 1000L;

    /**
     * The maximum amount of time a slave can be behind before being marked as
     * offline.  It will then be flagged and not used by clients any longer and
     * should (probably) catch up with the master shortly and then added back
     * into service (all automatically).
     */
    public static long MAX_SLAVE_BEHIND_INTERVAL = 30L * 1000L;

    /**
     * When the slave has fallen behind for N minutes, we should throttle the
     * entire lbpool configuration (I think) because the slave will probably
     * never catch up.
     *
     */
    public static long MAX_MASTER_THROTTLE_INTERVAL = 30L * 60L * 1000L;
    
    /**
     * If the load of a specific host is too high... we should stop using it for
     * a while to allow it to catch up.  This should improve overall cluster
     * performance, throughput, and prevent replication from falling behind.
     */
    public static long MAX_HOST_LOAD = 2L * 60L * 1000L;
    
    /**
     * True if we should debug this driver. 
     */
    public static boolean DEBUG = false;

    /**
     * Map of lbpool: URLs and their associated total connection information.
     */
    public static Hashtable PROFILE_REGISTRY = new Hashtable();

    /**
     * Internal (real) JDBC driver.
     */
    private Driver delegate = null;

    //register the driver with the DriverManager
	static {

		try {
			java.sql.DriverManager.registerDriver( new LBDriver() );
		} catch (SQLException E) {
            //probably will never happen in production
			throw new RuntimeException("Can't register driver!");
		}

	}

	/**
	 * Construct a new driver and register it with DriverManager
	 */
	public LBDriver() throws SQLException {}

    /**
     * Create a new connection. Note that this has to be synchronized because
     * the poller needs to init first before we connect to the first host.
     */
    public synchronized Connection connect( String url, java.util.Properties info )
        throws SQLException {
        
        Pattern p = Pattern.compile( "lbpool:([^:]+):([^:]+):([^?]+)(.*)" );
        Matcher m = p.matcher( url );
        
        if ( ! m.find() ) {
            return null;
        }

        LBConnectionProfile profile = null;
        
        synchronized( PROFILE_REGISTRY ) {
        
            profile = (LBConnectionProfile)PROFILE_REGISTRY.get( url );

            if ( profile == null ) {
                
                profile = new LBConnectionProfile();
                
                profile.balanced_url = url;
                
                profile.user = (String)info.get( "user" );
                profile.password = (String)info.get( "password" );
                
                //delegate_classname = m.group( 1 );
                profile.classname = m.group( 1 );
                profile.host = m.group( 2 );
                
                profile.database = m.group( 3 );
                profile.url = m.group( 4 );
                
                if ( log.isDebugEnabled() ) {
                    
                    log.debug( "Using profile.classname: " + profile.classname );
                    log.debug( "Using profile.host: " + profile.host );
                    log.debug( "Using profile.database: " + profile.database );
                    log.debug( "Using profile._url: " + profile.url );
                    
                }

                //before the first (or any) connectiosn are established we need
                //to run the profiler once and then start a thread to run it in
                //the future.
        
                doPollerInit( profile );

                PROFILE_REGISTRY.put( url, profile );

            }

        }

        // get the host we should connect to.

        // Extract this right from the JDBC URL.
        // jdbc:mysql://db3.tailrank.com/blogindex? 

        LBHostStatus real_host = null;

        int i = 0;
        while( true) {

            ++i;

            //Determine how to break out when necessary.....
            if ( MAX_CONNECTION_ATTEMPTS != -1 ) {

                if ( i > MAX_CONNECTION_ATTEMPTS )
                    break;

            }
            
            real_host = getRealHost( profile.host, profile );

            if ( real_host != null ) {

                break;

            } else {

                log.warn( "No slave host available for: " + 
                          profile.host + 
                          " with connections " + 
                          profile.hostRegistry );

                try {
                    Thread.sleep( HOST_POLL_INTERVAL );
                } catch( InterruptedException e ) {
                    break;
                }

            }

        }

        if ( real_host == null )
            throw new SQLException( "Failed during connection.  DB pool not available: " +
                                    profile.host );
        
        Connection conn = getRealConnection( real_host, profile );
        
        //increment the number of connections to this host because we can't wait
        //for the next polling cycle.  If we didn't do this the value would
        //always be zero and the same host would be used.
        ++real_host.numConnections;

        //wrap this with an LBConnection... to allow reconnect on load or failure.
        LBConnection lbconn = new LBConnection( conn, real_host, profile, this );

        synchronized( profile.openConnections ) {
            profile.openConnections.add( lbconn );
        }

        real_host.openConnections.add( lbconn );

        return lbconn;

    }
    
    public boolean acceptsURL( String url ) throws SQLException {
        return url.startsWith( "lbpool:" );

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
            System.out.println( "lbpool: " + msg );
        }

    }

    /**
     * Get a real data connection to a real database server using our best guess
     * as to the correct box to use.
     */
    protected static Connection getRealConnection( LBHostStatus real_host, 
                                                   LBConnectionProfile profile ) 
        throws SQLException {

        try {

            String real_url = 
                "jdbc:mysql://" + real_host.hostname + "/" +
                profile.database + profile.url;
            
            log.debug( "real_url: " + real_url );
            
            //update the MySQL connection URL and remove the lbpool_ properties.

            Class.forName( profile.classname );
            
            Connection conn = 
                LBDriver.getMySQLConnection( real_url, 
                                             profile.user, 
                                             profile.password );

            return conn;

        } catch ( Exception e ) {
            SQLException sqe = new SQLException();
            sqe.initCause( e );
            throw sqe;
        }

    }
    
    /**
     * Init the poller for a specific load balanced connection URL to get host
     * info.  If this is the first time run it in the current thread.
     */
    private synchronized void doPollerInit( LBConnectionProfile profile ) {

        LBHostPoller poller = new LBHostPoller( profile, this );

        try {
            poller.doPoll(); 
        } catch ( Exception e ) {

            //go ahead and print this stack trace right now because if
            //there's an exception on startup we probably want to warn FAST.
            log.error( "Exception in poller for host: " + profile.host, e );
            
        }

        if ( ENABLE_POLLER_THREAD )
            poller.start();
        
        profile.poller = poller;
        
    }

    private LBHostStatus getWinningHost( String name,
                                         LBConnectionProfile profile ) {

        Iterator it = profile.hostRegistry.keySet().iterator();

        LBHostStatus winner = null;

        while( it.hasNext() ) {
            
            String host = it.next().toString();

            LBHostStatus current = (LBHostStatus)profile.hostRegistry.get( host );

            if ( current.isOffline() )
                continue;

            if ( winner == null )
                winner = current;

            //TODO: load needs to be a factor here but we need an agent to
            //discovery the load value.

            //if this one has less connections use it.
            if ( winner.numConnections > current.numConnections )
                winner = current;

        }

        return winner;

    }

    /** 
     * Given the name of a DNS host to query determine the best MySQL slave to
     * use for this connection.
     */
    protected LBHostStatus getRealHost( String name,
                                        LBConnectionProfile profile ) {

        LBHostStatus winner = getWinningHost( name, profile );

        if ( winner != null ) {
            
            if ( log.isDebugEnabled() )
                log.debug( "Using winning db host: " + winner );
            
            return winner;
        }
        
        return null;

    }

    public static String getConnectionStatus() throws SQLException {

        return "";
        
    }

    public static String getConnectionStatus( String url ) throws SQLException {

        LBConnectionProfile profile = (LBConnectionProfile)PROFILE_REGISTRY.get( url );

        if ( profile == null ) {
            return "No profile for URL: " + url;
        }
        
        return getConnectionStatus( profile );
        
    }

    public static void dumpConnectionStatus() throws SQLException {

        System.out.printf( "------------\n" );
        System.out.printf( "Connection status:\n" );

        for( Object key : PROFILE_REGISTRY.keySet() ) {
            System.out.printf( "%s\n", key.toString() );
            System.out.printf( "---\n" );
            System.out.printf( "%s\n", getConnectionStatus( key.toString() ) );
        }
        
    }

    /**
     * Get status information for all connections.
     */
    public static String getConnectionStatus( LBConnectionProfile profile ) throws SQLException {

        StringBuffer buff = new StringBuffer();

        synchronized( profile.openConnections ) {

            buff.append( "lbpool: Available hosts in pool: " + profile.host + "\n" );

            buff.append( profile.hostRegistry );
            buff.append( "\n" );

            Iterator i = profile.openConnections.iterator(); 

            if ( ! i.hasNext() ) {
                buff.append( "lbpool: NO OPEN CONNETIONS\n" );
            } else {
                buff.append( "lbpool: Current " + profile.openConnections.size() + " open connections\n" );
            }

            //for all open connections
            while ( i.hasNext() ) {

                //call checkOffline() (which is efficient and won't run if the host is
                //online).

                LBConnection conn = (LBConnection)i.next();
                
                buff.append( "lbpool: " + conn.getMetaData().getURL() + "\n" );

            }

        }

        return buff.toString();

    }

    /**
     * If a slave is offline, and connections are established across OTHER
     * slaves, Try to rebalance the connections across them.
     */
    public void rebalanceConnections( LBConnectionProfile profile ) {

        //for all hosts...
        TreeSet hosts = new TreeSet();
        Iterator it = profile.hostRegistry.values().iterator();

        while( it.hasNext() ) {

            LBHostStatus status = (LBHostStatus)it.next();

            if ( status.isOffline() )
                continue;

            log.debug( "Adding host for potential rebalance: " + status );

            hosts.add( status );

        }

        //obviously we don't need to do anything here. Its important to perform
        //this check because if we DON'T then we'll get an NPE below.
        if ( hosts.size() == 0 )
            return;
        
        while ( true ) {

            LBHostStatus last = (LBHostStatus)hosts.last();
            LBHostStatus first = (LBHostStatus)hosts.first();

            if ( last.numConnections - first.numConnections <= 1 )
                break;

            if ( log.isDebugEnabled() ) {
                debug( "---------" );
                debug( "forcing reconnect for rebalance from: " + last + " " + 
                       "forcing reconnect for rebalance to: " + first );
            }

            // This will only happen when we're trying to rebalance with no connections.
            if ( last.openConnections.size() == 0 )
                break;
            
            LBConnection target = (LBConnection)last.openConnections.get( 0 );
            target.forceReconnectForRebalance = true;
            target.checkOnline( false );

            if ( log.isDebugEnabled() ) {
                log.debug( "Rebalance resulted in using: " + target.status );
            }

        }

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

        try {

            return driver.connect( url, info );

        } catch ( SQLException e ) {
            throw new SQLException( String.format( "Unable to connect to %s: %s",
                                                   url, e.getMessage() ) );
        }

    }

}

