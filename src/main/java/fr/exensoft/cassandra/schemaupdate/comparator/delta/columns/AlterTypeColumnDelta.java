package fr.exensoft.cassandra.schemaupdate.comparator.delta.columns;

import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaPriorities;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaType;
import fr.exensoft.cassandra.schemaupdate.model.Column;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;

public class AlterTypeColumnDelta extends ColumnAbstractDelta {

    public AlterTypeColumnDelta(Keyspace keyspace, Table table, Column source, Column target) {
        super(keyspace, table, source, target);
    }

    @Override
    public DeltaType getDeltaType() {
        return DeltaType.ALTER_TYPE;
    }

    @Override
    public int getPriority() {
        return DeltaPriorities.ALTER_TYPE;
    }

    @Override
    public String generateCQL() {
        return String.format("ALTER TABLE \"%s\".\"%s\" ALTER \"%s\" TYPE %s", keyspace.getName(), table.getName(), target.getName(), target.getType().getType());
    }

    @Override
    public String toString() {
        return String.format("Alter column \"%s\" type from \"%s\" to \"%s\" on table \"%s\"", target.getName(), source.getType().getType(), target.getType().getType(), table.getName());
    }
}
