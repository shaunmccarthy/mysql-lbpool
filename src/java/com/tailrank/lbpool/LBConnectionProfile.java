package com.tailrank.lbpool;

import java.util.*;

/**
 * Metainfo about a connection we're storing internally.  Each connection has
 * associated host, user, password, etc.  This is metainfo about the lbpool
 * virtual connection not the underlying delegate connections.
 */
public class LBConnectionProfile {

    /**
     * The original URL requested.  Example:
     * 
     * lbpool:com.mysql.jdbc.Driver:db.tailrank.com:blogindex
     * 
     */
    public String balanced_url = null;

    public String classname = null;
    public String host = null;
    public String user = null;
    public String password = null;
    public String database = null;

    /**
     * Non-load balanced URL info including query parameters.
     */
    public String url = null;

    protected LBHostPoller poller = null;
    
    /** 
     * Holds map of all host -> status information.
     */
    protected Hashtable hostRegistry = new Hashtable();

    /**
     * Keep handles on open connections so we can reconnect them if they're
     * idle.
     */
    protected Collection openConnections = new ArrayList();

}