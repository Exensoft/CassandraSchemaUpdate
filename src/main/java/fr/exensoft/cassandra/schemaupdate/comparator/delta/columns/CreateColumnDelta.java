package fr.exensoft.cassandra.schemaupdate.comparator.delta.columns;

import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaPriorities;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaType;
import fr.exensoft.cassandra.schemaupdate.model.Column;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;

public class CreateColumnDelta extends ColumnAbstractDelta {


    public CreateColumnDelta(Keyspace keyspace, Table table, Column column) {
        super(keyspace, table, null, column);
    }

    @Override
    public DeltaType getDeltaType() {
        return DeltaType.CREATE;
    }

    @Override
    public int getPriority() {
        return DeltaPriorities.CREATE_COLUMN;
    }

    @Override
    public String generateCQL() {
        return String.format("ALTER TABLE \"%s\".\"%s\" ADD \"%s\" %s", keyspace.getName(), table.getName(), target.getName(), target.getType().getType());
    }

    @Override
    public String toString() {
        return String.format("Create column \"%s\" on table \"%s\"", target.getName(), table.getName());
    }
}
