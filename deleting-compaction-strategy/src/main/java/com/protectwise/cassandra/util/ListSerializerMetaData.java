package com.protectwise.cassandra.util;

import com.protectwise.cassandra.retrospect.deletion.SerializableCellData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.serializers.ListSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.codehaus.jackson.annotate.JsonTypeName;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ayaz on 16/3/17.
 */
@JsonTypeName("ListSerializerMetaData")
public class ListSerializerMetaData extends SerializerMetaData {
    private SerializerMetaData elementSerializerMetaData;


    public SerializerMetaData getElementSerializerMetaData() {
        return elementSerializerMetaData;
    }

    public void setElementSerializerMetaData(SerializerMetaData elementSerializerMetaData) {
        this.elementSerializerMetaData = elementSerializerMetaData;
    }

    @Override
    public TypeSerializer getSerializer() {
        return ListSerializer.getInstance(elementSerializerMetaData.getSerializer());
    }

    @Override
    public SerializerMetaData getSerializerMetaData(TypeSerializer typeSerializer) {
        if(typeSerializer instanceof ListSerializer) {
            ListSerializerMetaData serializerMetaData = new ListSerializerMetaData();
            serializerMetaData.setElementSerializerMetaData(SerializerMetaDataFactory.getSerializerMetaData(((ListSerializer) typeSerializer).elements));
            return serializerMetaData;
        } else {
            throw new RuntimeException("type serializer: " + typeSerializer.getClass().getName() + " is not compatible for class " + this.getClass().getName());
        }
    }

    @Override
    public SerializableCellData getSerializableCellData(Cell cell, ColumnFamily columnFamily) {
        AbstractType<?> cellNameType = columnFamily.metadata().getValueValidator(cell.name());
        try {
            if (cellNameType instanceof ListType) {
                SerializableCellData serializerCellData = new SerializableCellData();
                //cell id is columnname:uuid
                String cellId = "";
                AbstractType<?> collectionElementType = ((ListType) cellNameType).nameComparator();
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
        return this.getElementSerializerMetaData().getCellValue(bytes);
    }
}
