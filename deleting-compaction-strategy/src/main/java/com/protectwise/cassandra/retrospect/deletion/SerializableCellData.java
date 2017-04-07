package com.protectwise.cassandra.retrospect.deletion;

import java.io.Serializable;
import java.util.List;

/**
 * Created by ayaz on 20/3/17.
 */
public class SerializableCellData implements Serializable {
    private String cellId;

    private byte [] value;

    private Long timestamp;

    //must be in increasing order
    private List<Long> cellIdComponenetPositions;

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

    public String getCellId() {
        return cellId;
    }

    public void setCellId(String cellId) {
        this.cellId = cellId;
    }

    public List<Long> getCellIdComponenetPositions() {
        return cellIdComponenetPositions;
    }

    public void setCellIdComponenetPositions(List<Long> cellIdComponenetPositions) {
        this.cellIdComponenetPositions = cellIdComponenetPositions;
    }
}
