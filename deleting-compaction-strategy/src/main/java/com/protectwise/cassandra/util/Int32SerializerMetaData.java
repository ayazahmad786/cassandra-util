package com.protectwise.cassandra.util;

import com.protectwise.cassandra.db.compaction.BackupSinkForDeletingCompaction;
import com.protectwise.cassandra.retrospect.deletion.SerializableCellData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.serializers.Int32Serializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.codehaus.jackson.annotate.JsonTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Created by ayaz on 15/3/17.
 */
@JsonTypeName("Int32SerializerMetaData")
public class Int32SerializerMetaData extends SerializerMetaData {
    private static final Logger logger = LoggerFactory.getLogger(Int32SerializerMetaData.class);

    public Int32SerializerMetaData() {
        setQualifiedClassName(this.getClass().getName());
    }

    @Override
    public TypeSerializer getSerializer() {
        return Int32Serializer.instance;
    }

    @Override
    public SerializerMetaData getSerializerMetaData(TypeSerializer typeSerializer) {
        if(typeSerializer instanceof Int32Serializer) {
            logger.debug("Inside serializermeta data: {}", this.getClass().getName());
            return this;
        } else {
            throw new RuntimeException("type serializer: " + typeSerializer.getClass().getName() + " is not compatible for int serializermetadata class");
        }
    }

    @Override
    public SerializableCellData getSerializableCellData(Cell cell, ColumnFamily columnFamily) {
        AbstractType<?> cellNameType = columnFamily.metadata().getValueValidator(cell.name());
        try {
            if(cellNameType instanceof Int32Type) {
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
