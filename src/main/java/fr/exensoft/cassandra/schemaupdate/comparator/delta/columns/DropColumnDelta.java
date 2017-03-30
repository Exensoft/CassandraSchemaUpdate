package fr.exensoft.cassandra.schemaupdate.comparator.delta.columns;

import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaPriorities;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaType;
import fr.exensoft.cassandra.schemaupdate.model.Column;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;

public class DropColumnDelta extends ColumnAbstractDelta {

    public DropColumnDelta(Keyspace keyspace, Table table, Column column) {
        super(keyspace, table, column, null);
    }

    @Override
    public DeltaType getDeltaType() {
        return DeltaType.DROP;
    }

    @Override
    public int getPriority() {
        return DeltaPriorities.DROP_COLUMN;
    }

    @Override
    public String generateCQL() {
        return String.format("ALTER TABLE \"%s\".\"%s\" DROP \"%s\"", keyspace.getName(), table.getName(), source.getName());
    }

    @Override
    public String toString() {
        return String.format("Drop column \"%s\" on table \"%s\"", source.getName(), table.getName());
    }
}
