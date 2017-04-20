package com.simility.cassandra.purgeddata;

import com.datastax.driver.core.SocketOptions;

import java.io.Serializable;

/**
 * Created by ayaz on 15/4/17.
 */
public class CassandraConfig implements Serializable {
    private String databaseURL = "localhost";
    private int port = 9042;
    private String clusterName = "SimilityDb";
    private int localCoreConnections = 8;
    private int localMaxConnections = 8;
    private int remoteCoreConnections = 2;
    private int remoteMaxConnections = 2;
    private int connectTimeoutMillis = SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS;
    private int readTimeoutMillis = SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS;
    private String replicationClass;
    private int replicationFactor;
    private String consistency = "QUORUM";
    private String writeConsistency = "LOCAL_ONE";
    private String alterTableConsistency = "LOCAL_ONE";
    private String username;
    private String password;
    private String dc;

    public String getDatabaseURL() {
        return databaseURL;
    }

    public void setDatabaseURL(String databaseURL) {
        this.databaseURL = databaseURL;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public int getLocalCoreConnections() {
        return localCoreConnections;
    }

    public void setLocalCoreConnections(int localCoreConnections) {
        this.localCoreConnections = localCoreConnections;
    }

    public int getLocalMaxConnections() {
        return localMaxConnections;
    }

    public void setLocalMaxConnections(int localMaxConnections) {
        this.localMaxConnections = localMaxConnections;
    }

    public int getRemoteCoreConnections() {
        return remoteCoreConnections;
    }

    public void setRemoteCoreConnections(int remoteCoreConnections) {
        this.remoteCoreConnections = remoteCoreConnections;
    }

    public int getRemoteMaxConnections() {
        return remoteMaxConnections;
    }

    public void setRemoteMaxConnections(int remoteMaxConnections) {
        this.remoteMaxConnections = remoteMaxConnections;
    }

    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public void setConnectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    public int getReadTimeoutMillis() {
        return readTimeoutMillis;
    }

    public void setReadTimeoutMillis(int readTimeoutMillis) {
        this.readTimeoutMillis = readTimeoutMillis;
    }

    public String getReplicationClass() {
        return replicationClass;
    }

    public void setReplicationClass(String replicationClass) {
        this.replicationClass = replicationClass;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public String getConsistency() {
        return consistency;
    }

    public void setConsistency(String consistency) {
        this.consistency = consistency;
    }

    public String getWriteConsistency() {
        return writeConsistency;
    }

    public void setWriteConsistency(String writeConsistency) {
        this.writeConsistency = writeConsistency;
    }

    public String getAlterTableConsistency() {
        return alterTableConsistency;
    }

    public void setAlterTableConsistency(String alterTableConsistency) {
        this.alterTableConsistency = alterTableConsistency;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDc() {
        return dc;
    }

    public void setDc(String dc) {
        this.dc = dc;
    }
}
