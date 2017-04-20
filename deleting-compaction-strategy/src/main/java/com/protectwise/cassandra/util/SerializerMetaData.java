package com.protectwise.cassandra.util;

import com.protectwise.cassandra.retrospect.deletion.SerializableCellData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import java.io.Serializable;

/**
 * Created by ayaz on 15/3/17.
 */

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = AsciiSerializerMetaData.class, name = "AsciiSerializerMetaData"),
        @JsonSubTypes.Type(value = BooleanSerializerMetaData.class, name = "BooleanSerializerMetaData"),
        @JsonSubTypes.Type(value = BytesSerializerMetaData.class, name = "BytesSerializerMetaData"),
        @JsonSubTypes.Type(value = DecimalSerializerMetaData.class, name = "DecimalSerializerMetaData"),
        @JsonSubTypes.Type(value = DoubleSerializerMetaData.class, name = "DoubleSerializerMetaData"),
        @JsonSubTypes.Type(value = EmptySerializerMetaData.class, name = "EmptySerializerMetaData"),
        @JsonSubTypes.Type(value = FloatSerializerMetaData.class, name = "FloatSerializerMetaData"),
        @JsonSubTypes.Type(value = InetAddressSerializerMetaData.class, name = "InetAddressSerializerMetaData"),
        @JsonSubTypes.Type(value = Int32SerializerMetaData.class, name = "Int32SerializerMetaData"),
        @JsonSubTypes.Type(value = IntegerSerializerMetaData.class, name = "IntegerSerializerMetaData"),
        @JsonSubTypes.Type(value = ListSerializerMetaData.class, name = "ListSerializerMetaData"),
        @JsonSubTypes.Type(value = LongSerializerMetaData.class, name = "LongSerializerMetaData"),
        @JsonSubTypes.Type(value = MapSerializerMetaData.class, name = "MapSerializerMetaData"),
        @JsonSubTypes.Type(value = SetSerializerMetaData.class, name = "SetSerializerMetaData"),
        @JsonSubTypes.Type(value = TimestampSerializerMetaData.class, name = "TimestampSerializerMetaData"),
        @JsonSubTypes.Type(value = TimeUUIDSerializerMetaData.class, name = "TimeUUIDSerializerMetaData"),
        @JsonSubTypes.Type(value = UTF8SerializerMetaData.class, name = "UTF8SerializerMetaData"),
        @JsonSubTypes.Type(value = UUIDSerializerMetaData.class, name = "UUIDSerializerMetaData"),
})
public abstract class SerializerMetaData implements Serializable {
    @JsonIgnore
    public  static final String CELL_ID_JOINER = ":";

    private String qualifiedClassName;

    public String getQualifiedClassName() {
        return qualifiedClassName;
    }

    public void setQualifiedClassName(String qualifiedClassName) {
        this.qualifiedClassName = qualifiedClassName;
    }

    @JsonIgnore
    public abstract TypeSerializer getSerializer();

    @JsonIgnore
    public abstract SerializerMetaData getSerializerMetaData(TypeSerializer typeSerializer);

    /**
     * This should be used only when cell is primitive type
     * @param cell
     * @param columnFamily
     * @return
     */
    @JsonIgnore
    public SerializableCellData getSerializableCellData(Cell cell, ColumnFamily columnFamily) {
        SerializableCellData serializerCellData = new SerializableCellData();
        //cell id is columnname
        String cellId = "";
        ColumnDefinition columnDefinition = columnFamily.metadata().getColumnDefinition(cell.name());
        cellId = String.join(CELL_ID_JOINER, columnDefinition.name.toString());
        serializerCellData.setCellId(cellId);
        serializerCellData.setValue(ByteBufferUtil.getArray(cell.value()));
        serializerCellData.setTimestamp(Long.valueOf(cell.timestamp()));
        return serializerCellData;
    }

    @JsonIgnore
    public abstract Object getCellValue(byte[] bytes);
}
