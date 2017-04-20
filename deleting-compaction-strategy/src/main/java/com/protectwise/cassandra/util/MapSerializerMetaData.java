package com.protectwise.cassandra.util;

import com.protectwise.cassandra.retrospect.deletion.SerializableCellData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.serializers.MapSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.codehaus.jackson.annotate.JsonTypeName;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ayaz on 16/3/17.
 */
@JsonTypeName("MapSerializerMetaData")
public class MapSerializerMetaData extends SerializerMetaData {
    private SerializerMetaData keySerializerMetaData;
    private SerializerMetaData valueSerializerMetaData;

    public SerializerMetaData getValueSerializerMetaData() {
        return valueSerializerMetaData;
    }

    public void setValueSerializerMetaData(SerializerMetaData valueSerializerMetaData) {
        this.valueSerializerMetaData = valueSerializerMetaData;
    }

    public SerializerMetaData getKeySerializerMetaData() {
        return keySerializerMetaData;
    }

    public void setKeySerializerMetaData(SerializerMetaData keySerializerMetaData) {
        this.keySerializerMetaData = keySerializerMetaData;
    }

    @Override
    public TypeSerializer getSerializer() {
        return MapSerializer.getInstance(keySerializerMetaData.getSerializer(), valueSerializerMetaData.getSerializer());
    }

    @Override
    public SerializerMetaData getSerializerMetaData(TypeSerializer typeSerializer) {
        if(typeSerializer instanceof MapSerializer) {
            MapSerializerMetaData serializerMetaData = new MapSerializerMetaData();
            serializerMetaData.setKeySerializerMetaData(SerializerMetaDataFactory.getSerializerMetaData(((MapSerializer) typeSerializer).keys));
            serializerMetaData.setValueSerializerMetaData(SerializerMetaDataFactory.getSerializerMetaData(((MapSerializer) typeSerializer).values));
            return serializerMetaData;
        } else {
            throw new RuntimeException("type serializer: " + typeSerializer.getClass().getName() + " is not compatible for class " + this.getClass().getName());
        }
    }

    @Override
    public SerializableCellData getSerializableCellData(Cell cell, ColumnFamily columnFamily) {
        AbstractType<?> cellNameType = columnFamily.metadata().getValueValidator(cell.name());
        try {
            if (cellNameType instanceof MapType) {
                SerializableCellData serializerCellData = new SerializableCellData();
                //cell id is columnname:key
                String cellId = "";
                AbstractType<?> collectionElementType = ((MapType) cellNameType).nameComparator();

                List<Long> splitPositions = new ArrayList<>();

                String key = collectionElementType.getString(cell.name().collectionElement());
                ColumnDefinition columnDefinition = columnFamily.metadata().getColumnDefinition(cell.name());
                String columnName = columnDefinition.name.toString();
                splitPositions.add(Long.valueOf(columnName.length()));

                cellId = String.join(CELL_ID_JOINER, columnName, key);
                serializerCellData.setCellId(cellId);
                serializerCellData.setValue(ByteBufferUtil.getArray(cell.value()));
                serializerCellData.setTimestamp(Long.valueOf(cell.timestamp()));
                serializerCellData.setCellIdComponentPositions(splitPositions);

                return serializerCellData;
            } else {
                throw new RuntimeException(this.getClass().getName() + " doesn't support cellNameType: " + cellNameType.toString());
            }
        }catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object getCellValue(byte[] bytes) {
        return this.getValueSerializerMetaData().getCellValue(bytes);
    }
}
