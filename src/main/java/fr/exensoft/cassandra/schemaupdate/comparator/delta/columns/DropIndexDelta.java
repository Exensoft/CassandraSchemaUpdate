package fr.exensoft.cassandra.schemaupdate.comparator.delta.columns;

import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaPriorities;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaType;
import fr.exensoft.cassandra.schemaupdate.model.Column;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;

public class DropIndexDelta extends ColumnAbstractDelta {

    public DropIndexDelta(Keyspace keyspace, Table table, Column source, Column target) {
        super(keyspace, table, source, target);
    }

    @Override
    public DeltaType getDeltaType() {
        return DeltaType.DROP_INDEX;
    }

    @Override
    public int getPriority() {
        return DeltaPriorities.DROP_INDEX;
    }

    @Override
    public String generateCQL() {
        return String.format("DROP INDEX \"%s\".\"%s\"", keyspace.getName(), table.getIndex(source).getName());
    }

    @Override
    public String toString() {
        return String.format("Drop index on column \"%s\" on table \"%s\"", source.getName(), table.getName());
    }
}
