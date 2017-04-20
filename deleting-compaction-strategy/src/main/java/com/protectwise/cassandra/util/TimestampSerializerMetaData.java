package com.protectwise.cassandra.util;

import com.protectwise.cassandra.retrospect.deletion.SerializableCellData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.serializers.TimestampSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.codehaus.jackson.annotate.JsonTypeName;

import java.nio.ByteBuffer;

/**
 * Created by ayaz on 15/3/17.
 */
@JsonTypeName("TimestampSerializerMetaData")
public class TimestampSerializerMetaData extends SerializerMetaData {
    @Override
    public TypeSerializer getSerializer() {
        return TimestampSerializer.instance;
    }

    @Override
    public SerializerMetaData getSerializerMetaData(TypeSerializer typeSerializer) {
        if(typeSerializer instanceof  TimestampSerializer) {
            return this;
        } else {
            throw new RuntimeException("type serializer: " + typeSerializer.getClass().getName() + " is not compatible for class " + this.getClass().getName());
        }
    }

    @Override
    public SerializableCellData getSerializableCellData(Cell cell, ColumnFamily columnFamily) {
        AbstractType<?> cellNameType = columnFamily.metadata().getValueValidator(cell.name());
        try {
            if(cellNameType instanceof TimestampType) {
                return super.getSerializableCellData(cell, columnFamily);
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
