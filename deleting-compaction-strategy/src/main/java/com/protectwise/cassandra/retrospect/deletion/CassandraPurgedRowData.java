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
public class CassandraPurgedRowData implements Serializable {
    private String ksName;

    private String cfName;

    private Set<String> partitionKeys = new HashSet<>();

    private Set<String> clusterKeys = new HashSet<>();

    private Map<String, SerializerMetaData> columnSerializerMetaDatas = new HashMap<>();

    private Map<String, SerializableCellData> cellSerializedValues = new HashMap<>();

    public Set<String> getPartitionKeys() {
        return partitionKeys;
    }

    public void setPartitionKeys(Set<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
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

    public Map<String, SerializableCellData> getCellSerializedValues() {
        return cellSerializedValues;
    }

    public void setCellSerializedValues(Map<String, SerializableCellData> cellSerializedValues) {
        this.cellSerializedValues = cellSerializedValues;
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

    public  CassandraPurgedRowData addNonKeyCell(SerializableCellData cellData, String columnName,  SerializerMetaData serializerMetaData) {
        cellSerializedValues.put(cellData.getCellId(), cellData);
        columnSerializerMetaDatas.put(columnName, serializerMetaData);
        return this;
    }

    /**
     * assumption that primary key are only scalar(i.e a collection type can't be part of primary key)
     * @param key
     * @param serializerMetaData
     * @param value
     * @param timestamp
     * @return
     */
    public CassandraPurgedRowData addPartitonKey(String key, SerializerMetaData serializerMetaData, ByteBuffer value, Long timestamp) {
        partitionKeys.add(key);
        columnSerializerMetaDatas.put(key, serializerMetaData);

        setPrimaryKeyColumnCellData(key, value);

        return this;
    }

    public CassandraPurgedRowData addClusteringKey(String key, SerializerMetaData serializerMetaData, ByteBuffer value, Long timestamp) {
        clusterKeys.add(key);
        columnSerializerMetaDatas.put(key, serializerMetaData);

        setPrimaryKeyColumnCellData(key, value);
        return this;
    }

    private void setPrimaryKeyColumnCellData(String key, ByteBuffer value) {
        SerializableCellData cellData = new SerializableCellData();
        cellData.setCellId(key);
        cellData.setValue(ByteBufferUtil.getArray(value));

        cellSerializedValues.put(cellData.getCellId(), cellData);
    }
}
