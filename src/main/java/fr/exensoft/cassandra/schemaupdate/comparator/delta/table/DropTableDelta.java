package fr.exensoft.cassandra.schemaupdate.comparator.delta.table;

import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaPriorities;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaType;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;

public class DropTableDelta extends TableAbstractDelta {

    public DropTableDelta(Keyspace keyspace, Table table) {
        super(keyspace, table, null);
    }

    @Override
    public DeltaType getDeltaType() {
        return DeltaType.DROP;
    }

    @Override
    public int getPriority() {
        return DeltaPriorities.DROP_TABLE;
    }

    @Override
    public String generateCQL() {
        return String.format("DROP TABLE \"%s\".\"%s\"", keyspace.getName(), source.getName());
    }
    @Override
    public String toString() {
        return String.format("Drop table \"%s\"", source.getName());
    }
}
