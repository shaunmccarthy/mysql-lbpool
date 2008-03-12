package com.tailrank.lbpool;

import java.net.*;
import java.sql.*;
import java.util.*;
import java.util.regex.*;

import org.apache.log4j.*;

import org.apache.commons.benchmark.*;

/**
 * 
 * Wait until no more pending statements are open and executing. When they are
 * all closed then we should close this connection.
 * 
 */
public class LBDisconnectThread extends Thread {

    public static Logger log = Logger.getLogger( LBDisconnectThread.class );

    public static long SPIN_INTERVAL = 1000L;

    public static long MAX_SPIN_INTERVAL = 5L * SPIN_INTERVAL;

    public Connection delegate = null;
    
    public LBPendingStatementMeta pendingStatementMeta = null;

    public LBDisconnectThread( Connection delegate, 
                               LBPendingStatementMeta pendingStatementMeta ) {

        super( "LBDisconnectThread" );

        this.delegate = delegate;
        this.pendingStatementMeta = pendingStatementMeta;

        this.setDaemon( true );

    }

    public void run() {

        long started = System.currentTimeMillis();

        while( true ) {

            try {

                long duration = System.currentTimeMillis() - started;

                if ( duration > MAX_SPIN_INTERVAL ) {

                    log.warn( "LBDisconnectThread: Closing open connection due to inactivity. " + 
                              "Resources not closed correctly: " + delegate );

                    delegate.close();
                    return;

                }
                
                if ( pendingStatementMeta.openStatements <= 0 ) {

                    log.debug( "LBDisconnectThread: Closing open connection due to no open statements: " +
                               delegate );
                    
                    delegate.close();

                    //we're done now... time to leave.
                    return;

                }

                if ( log.isDebugEnabled() ) {

                    log.debug( "LBDisconnectThread: Sleeping waiting for no open statements: " + 
                               pendingStatementMeta.openStatements );

                }

                Thread.sleep( SPIN_INTERVAL );

            } catch ( Throwable t ) {
                log.debug( "Exception in LBDisconnectThread: ", t );
                return; //I don't think we should go forward here.
            }

        }

    }

}
