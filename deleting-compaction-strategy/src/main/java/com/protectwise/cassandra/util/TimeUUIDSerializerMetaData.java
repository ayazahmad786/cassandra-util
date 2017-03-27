package com.protectwise.cassandra.util;

import org.apache.cassandra.serializers.TimeUUIDSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.codehaus.jackson.annotate.JsonTypeName;

/**
 * Created by ayaz on 15/3/17.
 */
@JsonTypeName("TimeUUIDSerializerMetaData")
public class TimeUUIDSerializerMetaData extends SerializerMetaData {
    @Override
    public TypeSerializer getSerializer() {
        return TimeUUIDSerializer.instance;
    }

    @Override
    public SerializerMetaData getSerializerMetaData(TypeSerializer typeSerializer) {
        if(typeSerializer instanceof TimeUUIDSerializer) {
            return this;
        } else {
            throw new RuntimeException("type serializer: " + typeSerializer.getClass().getName() + " is not compatible for class " + this.getClass().getName());
        }
    }
}