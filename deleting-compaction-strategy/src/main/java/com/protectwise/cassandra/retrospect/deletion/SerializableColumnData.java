package com.protectwise.cassandra.retrospect.deletion;

import java.io.Serializable;

/**
 * Created by ayaz on 20/3/17.
 */
public class SerializableColumnData implements Serializable {
    private byte [] value;

    private Long timestamp;

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
