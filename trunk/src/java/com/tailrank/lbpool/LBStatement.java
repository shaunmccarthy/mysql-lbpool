package com.tailrank.lbpool;

import java.sql.*;

import java.util.List;
import java.util.Iterator;

public class LBStatement implements Statement {

    /** My delegate. */
    protected Statement _stmt = null;

    /** The connection that created me. **/
    protected LBConnection _conn = null;

    protected LBPendingStatementMeta pendingStatementMeta = null;

    /**
     *
     * @param s the {@link Statement} to delegate all calls to.
     * @param c the {@link DelegatingConnection} that created this statement.
     */
    public LBStatement( LBConnection c, Statement s) {
        _stmt = s;
        _conn = c;
        pendingStatementMeta = c.pendingStatementMeta;

        //this statement is open now... 
        ++pendingStatementMeta.openStatements;

    }

    protected void handleException( SQLException e ) 
        throws SQLException {

        _conn.handleException( e );

        //FIXME: see LBPreparedStatement.handleException for an explanation of
        //why this is a problem right now.
        _stmt = _conn.getDelegate().createStatement();

    }

    /**
     * Returns my underlying {@link Statement}.
     * @return my underlying {@link Statement}.
     */
    public Statement getDelegate() {
        return _stmt;
    }

    /** Sets my delegate. */
    public void setDelegate(Statement s) {
        _stmt = s;
    }

   // **** QUERY EXECUTION - CODE THAT NEEDS RETRY *****************************

    public ResultSet executeQuery( String sql ) throws SQLException {

        while( true ) {

            try {
                _conn.isExecutingStatement = true;
                return _stmt.executeQuery(sql);
            } catch ( SQLException e ) {
                handleException( e );
            } finally {
                _conn.isExecutingStatement = false;
            }

        }

    }

    public boolean execute(String sql) throws SQLException {  

        while( true ) {

            try {
                _conn.isExecutingStatement = true;
                return _stmt.execute(sql);
            } catch ( SQLException e ) {
                handleException( e );
            } finally {
                _conn.isExecutingStatement = false;
            }

        }

    }

    /**
     * Close this DelegatingStatement, and close
     * any ResultSets that were not explicitly closed.
     */
    public void close() throws SQLException {
        _stmt.close();

        --pendingStatementMeta.openStatements;

    }

    public Connection getConnection() throws SQLException {
        return _conn; 
    }

    public ResultSet getResultSet() throws SQLException {

        return _stmt.getResultSet();
    }

    public int executeUpdate(String sql) throws SQLException {  return _stmt.executeUpdate(sql);}
    public int getMaxFieldSize() throws SQLException {  return _stmt.getMaxFieldSize();}
    public void setMaxFieldSize(int max) throws SQLException {  _stmt.setMaxFieldSize(max);}
    public int getMaxRows() throws SQLException {  return _stmt.getMaxRows();}
    public void setMaxRows(int max) throws SQLException {  _stmt.setMaxRows(max);}
    public void setEscapeProcessing(boolean enable) throws SQLException {  _stmt.setEscapeProcessing(enable);}
    public int getQueryTimeout() throws SQLException {  return _stmt.getQueryTimeout();}
    public void setQueryTimeout(int seconds) throws SQLException {  _stmt.setQueryTimeout(seconds);}
    public void cancel() throws SQLException {  _stmt.cancel();}
    public SQLWarning getWarnings() throws SQLException {  return _stmt.getWarnings();}
    public void clearWarnings() throws SQLException {  _stmt.clearWarnings();}
    public void setCursorName(String name) throws SQLException {  _stmt.setCursorName(name);}
    public int getUpdateCount() throws SQLException {  return _stmt.getUpdateCount();}
    public boolean getMoreResults() throws SQLException {  return _stmt.getMoreResults();}
    public void setFetchDirection(int direction) throws SQLException {  _stmt.setFetchDirection(direction);}
    public int getFetchDirection() throws SQLException {  return _stmt.getFetchDirection();}
    public void setFetchSize(int rows) throws SQLException {  _stmt.setFetchSize(rows);}
    public int getFetchSize() throws SQLException {  return _stmt.getFetchSize();}
    public int getResultSetConcurrency() throws SQLException {  return _stmt.getResultSetConcurrency();}
    public int getResultSetType() throws SQLException {  return _stmt.getResultSetType();}
    public void addBatch(String sql) throws SQLException {  _stmt.addBatch(sql);}
    public void clearBatch() throws SQLException {  _stmt.clearBatch();}
    public int[] executeBatch() throws SQLException {  return _stmt.executeBatch();}

    public boolean getMoreResults(int current) throws SQLException {
        return _stmt.getMoreResults(current);
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        
        return _stmt.getGeneratedKeys();
    }

    public int executeUpdate(String sql, int autoGeneratedKeys)
        throws SQLException {
        
        return _stmt.executeUpdate(sql, autoGeneratedKeys);
    }

    public int executeUpdate(String sql, int columnIndexes[])
        throws SQLException {
        
        return _stmt.executeUpdate(sql, columnIndexes);
    }

    public int executeUpdate(String sql, String columnNames[])
        throws SQLException {
        
        return _stmt.executeUpdate(sql, columnNames);
    }

    public boolean execute(String sql, int autoGeneratedKeys)
        throws SQLException {
        
        return _stmt.execute(sql, autoGeneratedKeys);
    }

    public boolean execute(String sql, int columnIndexes[])
        throws SQLException {
        
        return _stmt.execute(sql, columnIndexes);
    }

    public boolean execute(String sql, String columnNames[])
        throws SQLException {
        
        return _stmt.execute(sql, columnNames);
    }

    public int getResultSetHoldability() throws SQLException {
        
        return _stmt.getResultSetHoldability();
    }

}

