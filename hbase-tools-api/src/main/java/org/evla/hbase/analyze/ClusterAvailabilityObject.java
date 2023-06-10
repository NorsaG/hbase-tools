package org.evla.hbase.analyze;

import org.apache.hadoop.hbase.TableName;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ClusterAvailabilityObject {
    private final List<TableState> readTable;
    private final List<TableState> writeTables;

    public ClusterAvailabilityObject(int countRead, int countWrite) {
        this.readTable = new ArrayList<>(countRead);
        this.writeTables = new ArrayList<>(countWrite);
    }

    public boolean canRead() {
        return checkOperation(readTable);
    }

    public boolean canWrite() {
        return checkOperation(writeTables);
    }

    private boolean checkOperation(List<TableState> tableStates) {
        boolean result = true;
        for (TableState ts : tableStates) {
            result &= ts.getAvailability() == Availability.AVAILABLE;
        }
        return result;
    }

    public void addTableResult(Type type, TableName tableName, boolean isAvailable) {
        TableState state = new TableState(tableName, isAvailable);
        if (type == Type.READ) {
            this.readTable.add(state);
        } else if (type == Type.WRITE) {
            this.writeTables.add(state);
        }
    }

    public void cleanup() {
        this.readTable.clear();
        this.writeTables.clear();
    }

    @Override
    public String toString() {
        return "Availability of Cluster: \n" +
                "\tREAD:\n" +
                "\t\t" + readTable.stream().map(TableState::toString).collect(Collectors.joining("\n\t\t")) + "\n" +
                "\tWRITE:\n" +
                "\t\t" + writeTables.stream().map(TableState::toString).collect(Collectors.joining("\n\t\t"));
    }

    public static class TableState {
        private final TableName tableName;
        private final Availability availability;

        public TableState(TableName tableName, boolean isAvailable) {
            this(tableName, isAvailable ? Availability.AVAILABLE : Availability.NOT_AVAILABLE);
        }

        public TableState(TableName tableName, Availability availability) {
            this.tableName = tableName;
            this.availability = availability;
        }

        public TableName getTableName() {
            return tableName;
        }

        public Availability getAvailability() {
            return availability;
        }

        @Override
        public String toString() {
            return tableName + ": " + availability;
        }
    }

    public enum Availability {
        AVAILABLE, NOT_AVAILABLE;
    }

    public enum Type {
        READ, WRITE;
    }
}
