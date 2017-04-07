package com.protectwise.cassandra.util;

import com.protectwise.cassandra.retrospect.deletion.SerializableCellData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.serializers.AsciiSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.codehaus.jackson.annotate.JsonTypeName;

import java.nio.ByteBuffer;

/**
 * Created by ayaz on 15/3/17.
 */
@JsonTypeName("AsciiSerializerMetaData")
public class AsciiSerializerMetaData extends SerializerMetaData {
    @Override
    public TypeSerializer getSerializer() {
        return AsciiSerializer.instance;
    }

    @Override
    public SerializerMetaData getSerializerMetaData(TypeSerializer typeSerializer) {
        if(typeSerializer instanceof AsciiSerializer) {
            return this;
        } else {
            throw new RuntimeException("type serializer: " + typeSerializer.getClass().getName() + " is not compatible for class " + this.getClass().getName());
        }
    }

    @Override
    public SerializableCellData getSerializableCellData(Cell cell, ColumnFamily columnFamily) {
        AbstractType<?> cellNameType = columnFamily.metadata().getValueValidator(cell.name());
        try {
            if(cellNameType instanceof AsciiType) {
                SerializableCellData serializerCellData = new SerializableCellData();
                //cell id is columnname
                String cellId = "";
                ColumnDefinition columnDefinition = columnFamily.metadata().getColumnDefinition(cell.name());
                cellId = String.join(CELL_ID_JOINER, columnDefinition.name.toString());
                serializerCellData.setCellId(cellId);
                serializerCellData.setValue(ByteBufferUtil.getArray(cell.value()));
                serializerCellData.setTimestamp(Long.valueOf(cell.timestamp()));
                return serializerCellData;
            }else {
                throw new RuntimeException(this.getClass().getName() + " doesn't support cellNameType: " + cellNameType.toString());
            }
        }catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object getCellValue(byte[] bytes) {
        return getSerializer().deserialize(ByteBuffer.wrap(bytes));
    }
}
