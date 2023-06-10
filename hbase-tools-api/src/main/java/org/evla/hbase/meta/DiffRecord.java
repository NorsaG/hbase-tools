package org.evla.hbase.meta;

import java.util.Objects;

public class DiffRecord {
    private final DiffType type;
    private final String record;

    public DiffRecord(DiffType type, String record) {
        this.type = type;
        this.record = record;
    }

    public DiffType getType() {
        return type;
    }

    public String getRecord() {
        return record;
    }

    @Override
    public String toString() {
        return "DiffRecord{" +
                "type=" + type +
                ", record=" + record +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DiffRecord that = (DiffRecord) o;
        return type == that.type && Objects.equals(record, that.record);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, record);
    }
}
