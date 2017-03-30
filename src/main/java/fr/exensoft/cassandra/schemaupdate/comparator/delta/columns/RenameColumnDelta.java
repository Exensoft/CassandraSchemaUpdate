package fr.exensoft.cassandra.schemaupdate.comparator.delta.columns;

import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaPriorities;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaType;
import fr.exensoft.cassandra.schemaupdate.model.Column;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;

public class RenameColumnDelta extends ColumnAbstractDelta {

    public RenameColumnDelta(Keyspace keyspace, Table table, Column source, Column target) {
        super(keyspace, table, source, target);
    }

    @Override
    public DeltaType getDeltaType() {
        return DeltaType.RENAME;
    }

    @Override
    public int getPriority() {
        return DeltaPriorities.RENAME_COLUMN;
    }

    @Override
    public String generateCQL() {
        return String.format("ALTER TABLE \"%s\".\"%s\" RENAME \"%s\" TO \"%s\"", keyspace.getName(), table.getName(), source.getName(), target.getName());
    }

    @Override
    public String toString() {
        return String.format("Rename column \"%s\" to \"%s\" on table \"%s\"", source.getName(), target.getName(), table.getName());
    }
}
