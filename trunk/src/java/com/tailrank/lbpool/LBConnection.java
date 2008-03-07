package com.tailrank.lbpool;

import java.util.*;
import java.sql.*;
import java.io.PrintWriter;
import javax.sql.DataSource;
import java.util.Enumeration;

import com.mysql.jdbc.SQLError;

import org.apache.commons.benchmark.*;

import org.apache.log4j.*;

/**
 * Load balanced connection class providing reconnect on any new statements if
 * the server is offline.
 */
public class LBConnection implements Connection {

    public static Logger log = Logger.getLogger( LBConnection.class );

    /**
     * The only really acceptable error we can handle (and should rethrow) when
     * running a query on a slave.  If we encounter another error OTHER than
     * this code we mark the slave as offline and select another one to use.  If
     * we THEN get the same error the slave is no longer marked offline and we
     * throw the SQLException up the stack.
     */
    public static final int ER_PARSE_ERROR = 1064;

    /**
     * Instead of reconnecting to another slave just spin while this slave
     * catches up.
     */
    public static boolean SPIN_ON_SLAVE_OVERLOAD = false;

    public static Benchmark b1 = new CallerBenchmark();
    public static Benchmark b2 = new CallerBenchmark();
    public static Benchmark b3 = new CallerBenchmark();

    /**
     * The connection status for this host.  Each connection will end up being
     * on a different server and this status reflects this machines current
     * state.
     */
    protected LBHostStatus status = null;

    /**
     * Profile for this profile which contains username, password, etc.
     */
    protected LBConnectionProfile profile = null;

    /**
     * The real connection interface...
     */
    protected Connection delegate = null;

    /**
     * True if we've had to close this delegate due to a host going offline.
     */
    protected boolean delegateIsClosed = false;
    
    /**
     * True if we've requested this connection to reconnect (to a better host).
     */
    protected boolean forceReconnectForRebalance = false;

    /**
     * The time this connection was opened.
     */
    protected long opened = -1;

    protected String url = null;

    /**
     * If this host needs to reconnected (either for rebalance or because its
     * falling behind on replication) then this connection should be flagged for
     * reconnection and the first new query will be reconnected to a new slave
     * host.  Its important that existing connections finish executing.
     */
    protected boolean flagForReconnect = false;

    /**
     * When true we've created a statement and it is executing.  We can't
     * rebalance this connection while it has SQL executing.
     */
    protected boolean isExecutingStatement = false;
    
    /**
     * Used to prevent concurrent delegate close() from this thread while
     * handling exceptions, executing queries, and the status thread for when
     * doing reconnect.
     */
    protected Object MUTEX = new Object();

    protected LBPendingStatementMeta pendingStatementMeta = 
        new LBPendingStatementMeta();

    protected LBDriver driver = null;
    
    public LBConnection( Connection conn, 
                         LBHostStatus status, 
                         LBConnectionProfile profile,
                         LBDriver driver ) 
        throws SQLException {

        this.delegate = conn;
        this.status = status;
        this.profile = profile;
        this.driver = driver;
        
        opened = System.currentTimeMillis();

        //this is the only thing that should throw an SQLException 
        this.url = delegate.getMetaData().getURL();

    }

    public String toString() {
        synchronized( MUTEX ) {
            return delegate.toString();
        }
    }

    public void close() throws SQLException
    {

        synchronized( profile.openConnections ) {
            profile.openConnections.remove( this );
        }

        synchronized( MUTEX ) {
            delegate.close();
        }

    }

    protected void handleException( SQLException e )
        throws SQLException {

        try {

            b3.start();
            handleException1( e );

        } catch ( SQLException rethrow ) {

            log.error( "LBConnection found exception in connection: " + delegate, rethrow );
            throw rethrow;

        } finally {
            b3.complete();
        }
        
    }
    
    protected void handleException1( SQLException e )
        throws SQLException {
        
        if ( e.getErrorCode() == ER_PARSE_ERROR ) 
            throw e;

        String state = e.getSQLState();
        
        //for some reason in the source the error code can specify -1...
        if ( SQLError.SQL_STATE_COLUMN_NOT_FOUND.equals( state ) ) {
            throw e;
        }
        
        //if the error code is zero then the problem happened within the JDBC driver.
        if ( e.getErrorCode() == 0 ) {
            
            //when the lbpool doesn't have a error code from the server these are the
            //only states we should accept.  The only exceptions which would pass are
            //seriously problematic issues like connection issues.
            if ( ! SQLError.SQL_STATE_TRANSACTION_RESOLUTION_UNKNOWN.equals( state ) &&
                 ! SQLError.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE.equals( state ) &&
                 ! SQLError.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE.equals( state ) &&
                 ! SQLError.SQL_STATE_CONNECTION_NOT_OPEN.equals( state ) &&
                 ! SQLError.SQL_STATE_DRIVER_NOT_CAPABLE.equals( state ) &&
                 ! SQLError.SQL_STATE_COMMUNICATION_LINK_FAILURE.equals( state ) ) {
                     
                throw e;
                
            } 
            
        }

        log.warn( "handling exception - " + Thread.currentThread() +
                  " - error code: " + e.getErrorCode() + 
                  " - conn: " + delegate + 
                  " - url: " + url, e );
        
        //replace the internal delegate with a new (working) connection and allow
        //the caller to try again.
        checkOnline();

    }

    protected void checkOnline() {
        checkOnline( true );
    }

    /**
     * Check to make sure this connection is online.  If its not online then we
     * need to reconnect it to another slave.  Note that this NEEDS to be
     * synchronized so that existing (idle) connections can be reconnected from
     * the LBHostPoller thread.
     */
    protected void checkOnline( boolean spin ) {

        //FIXME: ALL THIS CODE NEEDS TO BE CLEANED UP.

        ///FIXME: I REALLY don't like this method.  It tries to do TOO much and
        ///has too many paths for different threads doing different things. It
        ///needs significant simplification.  It just ended up evolving to do
        ///TOO many things as the library developed.

        boolean isOfflineSlaveOverload = status.isOfflineSlaveOverload();

        while ( LBConnection.SPIN_ON_SLAVE_OVERLOAD && 
                forceReconnectForRebalance == false &&
                spin && 
                isOfflineSlaveOverload ) {

            try {

                log.debug( "Sleeping for slave catchup..." );

                Thread.sleep( 1000 );
            } catch ( InterruptedException e ) {
                return;
            }

            if ( status.isOfflineSlaveOverload() == false )
                return;

        }

        // We use a flag named isExecuting in the LBConnection class.  We set
        // this to true when I'm running SQL... then skip this connection when
        // we're rebalancing so that this query can finish executing without
        // being forced to close() which would throw an exception.
        //
        // since checkOnline is called right before we prepare a statement this
        // query will just finish up and then reconnect by itself once the SQL
        // is finished running.
                
        if ( spin == false && isExecutingStatement ) {

            ///NOTE: We shouldn't log by default in this method because it's
            ///called often from various pieces of code and it will end up
            ///generating too many log statements.

            //log.warn( "Skipping checkOnline due to executing SQL: " + this );
            return;
        }
        
        try {
            delegateIsClosed = delegate.isClosed();
        } catch ( SQLException sqe ) { 
            log.warn( sqe ); //not thrown in the MySQL JDBC driver
        }

        //if this host has been marked offline try another host... if its coming
        //back online after multiple polls the delegate will be closed so we
        //need to reopen it.
        if ( status.isOffline() || delegateIsClosed || forceReconnectForRebalance ) {

            while ( true ) {

                //NOTE: This must be individually synchronized outside of the
                //spin lock so that it doesn't block the poller thread when it
                //calls checkOnline.  If we don't do this the poller thread
                //would never be able to get a lock and would never be able to
                //mark this connection as back online.
                synchronized( MUTEX ) {
                    
                    doDisconnect();
                    
                    if ( doReconnect() )
                        break;
                    
                }

                //sleep for our host poll interval because this would be the FASTEST
                //we could really find a new server since we have to wait for more
                //metadata to be available to determine a suitable host.

                //if called from the polling code we don't want to spin.
                if ( spin == false )
                    break;

                sleepForConnection();

            }

        }

    }

    /**
     * Sleep for HOST_POLL_INTERVAL so that the a connection can come back
     * online (either to sleep for this host or connect to a new one).
     */
    private void sleepForConnection() {

        try {
            
            b2.start();

            if ( log.isDebugEnabled() )
                log.debug( "Sleeping to re-establish connection to slave. Last host: " + status );
            
            Thread.sleep( LBDriver.HOST_POLL_INTERVAL );
            
        } catch ( Exception e ) { 
            log.error( e ); //won't happen in production I think.
        } finally {
            b2.complete();
        }
        
    }

    /**
     * Force reconnect to a new host. 
     */
    private void doDisconnect() {

        //close the underlying connection to free up resources on
        //the remote end.
        try {
            
            if ( ! delegate.isClosed() ) {
                
                log.debug( "Closing connection: " + delegate );
            
                LBDisconnectThread disconnectThread =
                    new LBDisconnectThread( delegate, 
                                            pendingStatementMeta );
                
                disconnectThread.start();

                delegateIsClosed = true;
                
                status.openConnections.remove( this );
                
                //update the number of connections so it realistically reflects
                //the status of this host instead of having to wait to poll.
                --status.numConnections;
                
            }
            
        } catch ( SQLException e ) {
            log.warn( "Caught unknown SQL Exception: ", e );
        }

    }

    /**
     * Force reconnect to a new host. 
     */
    private boolean doReconnect() {

        try {

            b1.start();

            //we need to grab e new underlying connection for this bad boy and
            //swap it in before we continue
            LBHostStatus new_status = driver.getRealHost( profile.host, profile );

            if ( new_status == null ) 
                return false;

            this.delegate = LBDriver.getRealConnection( new_status, profile );
            this.delegateIsClosed = false;
            this.forceReconnectForRebalance = false;
            this.opened = System.currentTimeMillis();

            this.url = delegate.getMetaData().getURL();

            if ( log.isDebugEnabled() )
                log.debug( "Found new slave connection: " + new_status );
            
            //quit this sleep loop..
            status = new_status;
            status.openConnections.add( this );
            ++status.numConnections;

            return true;

        } catch ( SQLException e ) {

            //this can happenn if the connection to the new host fails
            //for some reason but should generally never happen in
            //production.
            
            log.warn( "Connection to new host failed: ", e );

            return false;
            
        } finally {
            b1.complete();
        }

    }

    public Connection getDelegate() {
        return delegate;
    }

    // **** Wrapped SQL execution ***********************************************

    public Properties getClientInfo() throws SQLException {
        throw new SQLException( "not implemented" );
    }
    
    public Struct createStruct(String typeName,
                               Object[] attributes) 
        throws SQLException {

        throw new SQLException( "not implemented" );
        
    }

    public Array createArrayOf(String typeName,
                        Object[] elements)
        throws SQLException {

        throw new SQLException( "not implemented" );

    }
    
    public Statement createStatement(int resultSetType,
                                     int resultSetConcurrency,
                                     int resultSetHoldability)
        throws SQLException {

        checkOnline();

        synchronized( MUTEX ) {
            return new LBStatement( this, delegate.createStatement( resultSetType,
                                                                    resultSetConcurrency,
                                                                    resultSetHoldability ) );
        }

    }

    public Statement createStatement(int resultSetType,
                                     int resultSetConcurrency)
            throws SQLException {

        checkOnline();

        synchronized( MUTEX ) {
            return new LBStatement( this, delegate.createStatement(resultSetType,resultSetConcurrency));
        }

    }

    public Statement createStatement() throws SQLException {

        checkOnline();

        synchronized( MUTEX ) {
            return new LBStatement( this, delegate.createStatement() );
        }

    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {

        checkOnline();

        synchronized( MUTEX ) {
            return new LBPreparedStatement( this, sql, delegate.prepareStatement(sql) );
        }

    }

    public PreparedStatement prepareStatement( String sql, String columnNames[] )
        throws SQLException {

        checkOnline();

        synchronized( MUTEX ) {
            return new LBPreparedStatement( this, sql, delegate.prepareStatement(sql, columnNames));
        }

    }
    public PreparedStatement prepareStatement(String sql, int columnIndexes[])
        throws SQLException {
        checkOnline();

        synchronized( MUTEX ) {
            return new LBPreparedStatement( this, sql, delegate.prepareStatement(sql, columnIndexes));
        }

    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
        throws SQLException {
        checkOnline();

        synchronized( MUTEX ) {
            return new LBPreparedStatement( this, sql, delegate.prepareStatement(sql, autoGeneratedKeys) );
        }

    }
    
    public PreparedStatement prepareStatement(String sql,
                                              int resultSetType,
                                              int resultSetConcurrency,
                                              int resultSetHoldability)
        throws SQLException {
        checkOnline();

        synchronized( MUTEX ) {

            return new LBPreparedStatement( this, sql, delegate.prepareStatement(sql,
                                                                                 resultSetType,
                                                                                 resultSetConcurrency,
                                                                                 resultSetHoldability) );
        }

    }

    public PreparedStatement prepareStatement(String sql,
                                              int resultSetType,
                                              int resultSetConcurrency)
        throws SQLException {
        checkOnline();

        synchronized( MUTEX ) {
            return new LBPreparedStatement( this, sql, delegate.prepareStatement( sql,
                                                                                  resultSetType,
                                                                                  resultSetConcurrency ));
        }

    }

    // **** java.sql.Connection *************************************************

    public CallableStatement prepareCall(String sql) throws SQLException {
        checkOnline();

        synchronized( MUTEX ) {
            return delegate.prepareCall( sql );
        }

    }

    public String nativeSQL(String sql) throws SQLException {
        checkOnline();

        synchronized( MUTEX ) {
            return delegate.nativeSQL( sql );
        }

    }

    public java.sql.Savepoint setSavepoint() throws SQLException {
        checkOnline();

        synchronized( MUTEX ) {
            return delegate.setSavepoint();
        }

    }

    public java.sql.Savepoint setSavepoint(String name) 
        throws SQLException {

        checkOnline();

        synchronized( MUTEX ) {
            return delegate.setSavepoint(name);
        }

    }

    public CallableStatement prepareCall(String sql,
                                         int resultSetType,
                                         int resultSetConcurrency)
            throws SQLException {

        checkOnline();

        synchronized( MUTEX ) {
            return delegate.prepareCall(sql, resultSetType,resultSetConcurrency);
        }

    }

    public boolean isReadOnly() throws SQLException
    { 
        checkOnline(); 

        synchronized( MUTEX ) {
            return delegate.isReadOnly();
        }

    }

    public void rollback() throws SQLException 
    { 
        checkOnline(); 

        synchronized( MUTEX ) {
            delegate.rollback();
        }

    }

    public void setAutoCommit(boolean autoCommit) throws SQLException 
    { 
        checkOnline(); 
    
        synchronized( MUTEX ) {
            delegate.setAutoCommit(autoCommit);
        }

    }

    public void setCatalog(String catalog) throws SQLException 
    { 
        checkOnline(); 
    
        synchronized( MUTEX ) {
            delegate.setCatalog(catalog);
        }

    }

    public void setReadOnly(boolean readOnly) throws SQLException 
    { 
        checkOnline(); 

        synchronized( MUTEX ) {
            delegate.setReadOnly(readOnly);
        }

    }

    public void setTransactionIsolation(int level) throws SQLException 
    { 
        checkOnline();
    
        synchronized( MUTEX ) {
            delegate.setTransactionIsolation(level);
        }
    
    }

    public void setTypeMap(Map map) throws SQLException 
    { 
        checkOnline();
    
        synchronized( MUTEX ) {
            delegate.setTypeMap(map);
        }
    
    }

    public void clearWarnings() throws SQLException 
    { 
        checkOnline();

        synchronized( MUTEX ) {
            delegate.clearWarnings();
        }

    }

    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int resultSetConcurrency,
                                         int resultSetHoldability)
        throws SQLException {

        checkOnline();

        synchronized( MUTEX ) {

            return delegate.prepareCall( sql, 
                                         resultSetType,
                                         resultSetConcurrency,
                                         resultSetHoldability );

        }

    }

    public Map getTypeMap() throws SQLException 
    {
        checkOnline();

        synchronized( MUTEX ) {
            return delegate.getTypeMap();
        }
    
    }

    public int getHoldability() throws SQLException {
        checkOnline();

        synchronized( MUTEX ) {
            return delegate.getHoldability();
        }

    }

    public void setHoldability(int holdability) throws SQLException {
        checkOnline();

        synchronized( MUTEX ) {
            delegate.setHoldability(holdability);
        }

    }

    public void rollback(java.sql.Savepoint savepoint) throws SQLException {
        checkOnline();   

        synchronized( MUTEX ) {
            delegate.rollback(savepoint);
        }

    }

    public void releaseSavepoint(java.sql.Savepoint savepoint) throws SQLException {
        checkOnline();

        synchronized( MUTEX ) {
            delegate.releaseSavepoint(savepoint);
        }

    }

    public SQLWarning getWarnings() throws SQLException 
    { 
        checkOnline(); 

        synchronized( MUTEX ) {
            return delegate.getWarnings();
        }

    }
    
    public DatabaseMetaData getMetaData() throws SQLException
    {  
        checkOnline(); 

        synchronized( MUTEX ) {
            return delegate.getMetaData();
        }

    }

    public String getCatalog() throws SQLException 
    {  
        checkOnline(); 

        synchronized( MUTEX ) {
            return delegate.getCatalog();
        }

    }

    public boolean getAutoCommit() throws SQLException
    {  
        checkOnline(); 

        synchronized( MUTEX ) {
            return delegate.getAutoCommit();
        }

    }

    public boolean isClosed() throws SQLException {

        synchronized( MUTEX ) {
            return delegate.isClosed(); 
        }

    }

    public void commit() throws SQLException
    {  
        checkOnline(); 

        synchronized( MUTEX ) {
            delegate.commit(); 
        }

    }

    public int getTransactionIsolation() 
        throws SQLException { 

        checkOnline();

        synchronized( MUTEX ) {
            return delegate.getTransactionIsolation();
        }

    }

}
