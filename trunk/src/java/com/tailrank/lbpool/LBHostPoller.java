package com.tailrank.lbpool;

import java.net.*;
import java.sql.*;
import java.util.*;
import java.util.regex.*;

import org.apache.log4j.*;

import org.apache.commons.benchmark.*;

/**
 * Handles looking at all hosts periodically to determine which hosts are
 * offline, healthy, etc.
 */
public class LBHostPoller extends Thread {

    public static final int SLAVE_IS_MASTER = -1;
    public static final int SLAVE_ONLINE    =  0;
    public static final int SLAVE_OFFLINE   =  1;

    public static Logger log = Logger.getLogger( LBHostPoller.class );

    private LBConnectionProfile profile = null;
    private LBDriver driver = null;
    
    public LBHostPoller( LBConnectionProfile profile,
                         LBDriver driver ) {

        super( "lbconn-host-poller" );
        
        this.profile = profile;
        this.driver = driver;

        this.setDaemon( true );

    }

    public void run() {

        while( true ) {

            try { 
                
                Thread.sleep( LBDriver.HOST_POLL_INTERVAL );

                doPoll();
                
            } catch ( Exception t ) {
                log.debug( "Exception in LBHostPoller: ", t );
            }

        }

    }

    /**
     * If a host is removed from our DNS config we should mark it offline so
     * that it is no longer used.
     */
    private void doMarkOfflineRemovedHosts( String[] addr ) {

        HashSet livehosts = new HashSet();

        for( int i = 0; i < addr.length; ++i ) {

            String address = addr[i];
            livehosts.add( address );

        }

        Iterator it = profile.hostRegistry.keySet().iterator();

        while( it.hasNext() ) {

            Object key = it.next();

            if ( ! livehosts.contains( key ) ) {
                
                LBHostStatus status = (LBHostStatus)profile.hostRegistry.get( key );
                status.setOffline( true );

                if ( log.isDebugEnabled() )
                    log.debug( "Host was marked offline since removed from config: " + status );

            }

        }

    }

    /**
     * Go through all open LBConnection objects and ones which have just been
     * flagged as going offline and force them to reconnect to functional
     * slaves. 
     */
    private void reconnectOfflineSlaves() {

        // Tue Jun 06 2006 07:22 PM (burton1@rojo.com): had to comment out
        // because this prevents rebalance when a slave is flagged to go
        // offline.  This was originally added during a time when we had a but
        // in reconnect support to disable it. 
        //
        //if ( LBConnection.SPIN_ON_SLAVE_OVERLOAD )
        //    return;

        log.debug( "Reconnecting offline slaves..." );

        synchronized( profile.openConnections ) {

            Iterator i = profile.openConnections.iterator(); 

            //for all open connections
            while ( i.hasNext() ) {

                //call checkOffline() (which is efficient and won't run if the host is
                //online).
                
                LBConnection conn = (LBConnection)i.next();

                if ( log.isDebugEnabled() )
                    log.debug( "Reconnecting offline slaves... " + conn.status );

                synchronized( conn.MUTEX ) {
                    conn.checkOnline( false );
                }
                
            }
            
        }

        log.debug( "Reconnecting offline slaves... done" );

    }

    private String[] getSlaveHostsFromMaster() throws Exception {

        Connection conn = null;
        Statement stmt = null;
        ResultSet results = null;

        Vector v = new Vector();
        
        try {
            
            Class.forName( profile.classname );

            String url = "jdbc:mysql://" + profile.host + "/lbpool?connectTimeout=15000&socketTimeout=15000";

            if ( log.isDebugEnabled() )
                log.debug( "Connecting to master with: " + url );
                
            conn =  LBDriver.getMySQLConnection( url, 
                                                 profile.user, 
                                                 profile.password );

            String q = 
                "SELECT * FROM HOST WHERE POOLNAME = '" + 
                profile.host + 
                "' /* lbpool */";

            stmt = conn.createStatement();
            results = stmt.executeQuery( q );

            while ( results.next() ) {

                String hostname = results.getString( "HOSTNAME" );
                boolean offline = results.getBoolean( "OFFLINE" );

                if ( log.isDebugEnabled() )
                    log.debug( "Master returned host: " + hostname );

                if ( offline ) {

                    if ( log.isDebugEnabled() )
                        log.debug( "Host marked offline: " + hostname );

                    continue;
                }

                v.add( hostname );

            }

            String[] result = new String[ v.size() ];
            v.copyInto( result );
            return result;

        } finally {
            
            if ( conn != null )
                conn.close();

            if ( stmt != null )
                stmt.close();

            if ( results != null )
                results.close();

        }

    }

    /**
     * Poll an individual MySQL box and update stats.
     */
    public void doPoll( String address ) throws Exception {

        String url = "jdbc:mysql://" + address + "/?connectTimeout=15000&socketTimeout=15000";

        if ( log.isDebugEnabled() )
            log.debug( "Going to poll: " + url );
        
        LBHostStatus status = (LBHostStatus)profile.hostRegistry.get( address );
        
        //add this to the host registry if necessary.
        if ( status == null ) {
            status = new LBHostStatus();
            status.hostname = address;
            profile.hostRegistry.put( address, status );
        }
        
        Connection conn = null;
        
        try {
            
            Class.forName( profile.classname );
            
            conn =  LBDriver.getMySQLConnection( url, 
                                                 profile.user, 
                                                 profile.password );
            
            SlaveStatus slave_status = fetchSlaveStatus( conn, status );

            //update registry information for this master host.

            LBHostStatus current_master = null;

            if ( slave_status.master_host != null )
                current_master = (LBHostStatus)profile.hostRegistry.get( slave_status.master_host );

            if ( current_master != null ) {
                log.debug( "Found master in pool config: " + slave_status.master_host );
                current_master.setMaster( true );
            }
                
            status.setMasterHost( slave_status.master_host );
            
            boolean offline = slave_status.offline_state == SLAVE_OFFLINE;
            
            if ( offline && ! status.isOffline() ) {
                log.debug( "Slave is NOW marked offline: " + status );
            } 
            
            //its not offline right now if we can extablish a conn (though
            //it might be offline if replication is broken)
            status.setOffline( offline );
            status.setOfflineSlaveOverload( offline );

            // When we're ONLY running with two hosts in the pool... and ONE of
            // them is the master we should ALSO take the master offline for
            // queries too let the whole cluster catch up.
            if ( profile.hostRegistry.size() == 2 &&
                 current_master != null ) {

                //FIXME: if the slave is physically marked offline we should
                //JUST use the master I think.

                // smarter handling of automatic throttling.  For now we can
                // allow the slave to fall behind for a while before we detect
                // that it will NEVER catch up.  Usually the slave starts to
                // catch up without needing to pause the entire cluster.
                if ( slave_status.milliseconds_behind_master > LBDriver.MAX_MASTER_THROTTLE_INTERVAL ) {
                
                    current_master.setOffline( offline );
                    current_master.setOfflineSlaveOverload( offline );

                }

            }
            
            updateHostMeta( conn, status );

            if ( log.isDebugEnabled() )
                log.debug( "Poll result: " + status );
            
        } catch ( Exception e ) {
            
            if ( LBDriver.DEBUG ) {
                
                //this happens within a thread so we dont' really want to
                //throw an exception up the stack and we also have to
                //process ALL the hosts.
                log.error( "Exception trying to determine slave status: ", e );
                
            }

            //TODO Mon Jan 26 2009 05:34 PM (burton@tailrank.com): add a new
            //field for offline_exception so that we can store and log and
            //exception that happends when we communicate with this host.
            
            status.setOffline( true );
            status.offline_message = e.getMessage();
            
        } finally {
            
            if ( conn != null )
                conn.close();
            
        }

    }

    /**
     * Do the actual polling of all the hosts in our load balanced queue.
     */
    public void doPoll() throws Exception {

        log.debug( "*** Within doPoll() *** " );

        String[] addr = getSlaveHostsFromMaster();

        doMarkOfflineRemovedHosts( addr );

        for( int i = 0; i < addr.length; ++i ) {
            
            String address = addr[i];
            doPoll( address );

        }

        //If a slave has been marked offline... just reconnect it.
        reconnectOfflineSlaves();
        driver.rebalanceConnections( profile );

    }

    /**
     * Update metadata for this host including number of connections and virtual
     * load.
     */
    private void updateHostMeta( Connection conn, LBHostStatus status ) throws Exception {

        Statement stmt = null;
        ResultSet results = null;

        try {

            stmt = conn.createStatement();
            results = stmt.executeQuery( "SHOW PROCESSLIST /* lbpool */" );
            
            int numConnections = 0;
            int load = 0;
            
            while ( results.next() ) {

                String info = results.getString( "Info" );
                String db = results.getString( "db" );

                //ignore connections from other hosts not to our target
                //database.  This is probably going to be a connection without
                //any real load.
                if ( db == null || ! db.equals( profile.database ) ) {
                    continue;
                }

                ++numConnections;

                if ( info == null )
                    continue;

                load += results.getInt( "Time" );

            }

            status.numConnections = numConnections;

            // TODO: Tue Jun 06 2006 08:51 PM (burton1@rojo.com): move to a real
            // load agent powered by an external system like munin or crontab
            // for example.
            status.load = load;

            long load_mean = 0;
            
            if ( numConnections > 0 )
                load_mean = load / numConnections;

            status.load_mean = load_mean;

        } finally {

            if ( stmt != null )
                stmt.close();

            if ( results != null )
                results.close();

        }

    }

    /**
     * Determine if this slave is offline.
     * 
     * Returns:
     *
     *  -1  If the this machine is actually a master
     *   0  If this slave is NOT offline
     *   1  If the slave IS offline.
     */
    private SlaveStatus fetchSlaveStatus( Connection conn,
                                          LBHostStatus status ) throws Exception {

        Statement stmt = null;
        ResultSet results = null;

        SlaveStatus slave_status = new SlaveStatus();
        
        try {

            stmt = conn.createStatement();
            results = stmt.executeQuery( "SHOW SLAVE STATUS /* lbpool */ " );

            slave_status.offline_state = getOfflineState( results, status );

            if ( results.first() ) {
                
                if ( slave_status.offline_state != SLAVE_IS_MASTER )
                    slave_status.master_host = results.getString( "Master_Host" );
                
                int seconds_behind_master = results.getInt( "Seconds_Behind_Master" );
                slave_status.milliseconds_behind_master = seconds_behind_master * 1000;

            }

            return slave_status;
            
        } finally {

            if ( stmt != null )
                stmt.close();

            if ( results != null )
                results.close();

        }

    }

    private int getOfflineState( ResultSet results,
                                 LBHostStatus status ) throws Exception {

        // go to the first result...
        if ( ! results.first() ) {
            return SLAVE_IS_MASTER;
        }

        String slave_sql_running = results.getString( "Slave_SQL_Running" );
        
        if ( ! "Yes".equals( slave_sql_running ) ) {
            
            log.debug( "MySQL slave sql not running" );

            return SLAVE_OFFLINE;

        }

        int seconds_behind_master = results.getInt( "Seconds_Behind_Master" );

        long milliseconds_behind_master = seconds_behind_master * 1000;
        
        if ( milliseconds_behind_master > LBDriver.MAX_SLAVE_BEHIND_INTERVAL ) {

            if ( log.isDebugEnabled() )
                log.debug( "MySQL Slave server behind replication: " + milliseconds_behind_master );

            return SLAVE_OFFLINE;

        }

        //If the slave is currently offline.... but is coming back online
        //and hasn't reached the threadhold yet keep it offline until it
        //does.

        if ( status.isOffline() &&
             milliseconds_behind_master > LBDriver.HOST_POLL_INTERVAL ) {

            log.debug( "MySQL slave still behing replication for poll: " + milliseconds_behind_master ); 
            return SLAVE_OFFLINE;
        }
        
        return SLAVE_ONLINE;

    }
    
    /**
     * Wrapper for show slave status.
     */
    class SlaveStatus {

        int offline_state = -1024;
        String master_host = null;
        long milliseconds_behind_master = 0;
        
    }
}
