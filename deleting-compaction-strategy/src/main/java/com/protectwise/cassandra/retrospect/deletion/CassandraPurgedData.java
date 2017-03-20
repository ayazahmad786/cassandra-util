package com.protectwise.cassandra.retrospect.deletion;

import com.protectwise.cassandra.util.SerializerMetaData;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by ayaz on 9/3/17.
 */
public class CassandraPurgedData implements Serializable {

    private String ksName;

    private String cfName;

    private Set<String> partitonKeys = new HashSet<>();

    private Set<String> clusterKeys = new HashSet<>();

    private Map<String, SerializerMetaData> columnSerializerMetaDatas = new HashMap<>();

    private Map<String, SerializableColumnData> columnSerializedValues = new HashMap<>();

    public Set<String> getPartitonKeys() {
        return partitonKeys;
    }

    public void setPartitonKeys(Set<String> partitonKeys) {
        this.partitonKeys = partitonKeys;
    }

    public Set<String> getClusterKeys() {
        return clusterKeys;
    }

    public void setClusterKeys(Set<String> clusterKeys) {
        this.clusterKeys = clusterKeys;
    }

    public Map<String, SerializerMetaData> getColumnSerializerMetaDatas() {
        return columnSerializerMetaDatas;
    }

    public void setColumnSerializerMetaDatas(Map<String, SerializerMetaData> columnSerializerMetaDatas) {
        this.columnSerializerMetaDatas = columnSerializerMetaDatas;
    }

    public Map<String, SerializableColumnData> getColumnSerializedValues() {
        return columnSerializedValues;
    }

    public void setColumnSerializedValues(Map<String, SerializableColumnData> columnSerializedValues) {
        this.columnSerializedValues = columnSerializedValues;
    }


    public String getKsName() {
        return ksName;
    }

    public void setKsName(String ksName) {
        this.ksName = ksName;
    }


    public String getCfName() {
        return cfName;
    }

    public void setCfName(String cfName) {
        this.cfName = cfName;
    }

    public CassandraPurgedData addPartitonKey(String key, SerializerMetaData serializerMetaData, ByteBuffer value, Long timestamp) {
        partitonKeys.add(key);
        columnSerializerMetaDatas.put(key, serializerMetaData);
        SerializableColumnData columnData = getSerializableColumnData(value, timestamp);
        columnSerializedValues.put(key, columnData);
        return this;
    }

    public CassandraPurgedData addClusteringKey(String key, SerializerMetaData serializerMetaData, ByteBuffer value, Long timestamp) {
        clusterKeys.add(key);
        columnSerializerMetaDatas.put(key, serializerMetaData);

        SerializableColumnData columnData = getSerializableColumnData(value, timestamp);

        columnSerializedValues.put(key, columnData);
        return this;
    }

    private SerializableColumnData getSerializableColumnData(ByteBuffer value, Long timestamp) {
        SerializableColumnData columnData = new SerializableColumnData();
        columnData.setValue(ByteBufferUtil.getArray(value));
        columnData.setTimestamp(timestamp);
        return columnData;
    }

    public CassandraPurgedData addNonKeyColumn(String name, SerializerMetaData serializerMetaData, ByteBuffer value, Long timestamp) {
        columnSerializerMetaDatas.put(name, serializerMetaData);
        SerializableColumnData columnData = getSerializableColumnData(value, timestamp);
        columnSerializedValues.put(name, columnData);
        return this;
    }
}
