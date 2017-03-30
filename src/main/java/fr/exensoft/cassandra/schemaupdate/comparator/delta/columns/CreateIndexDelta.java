package fr.exensoft.cassandra.schemaupdate.comparator.delta.columns;

import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaPriorities;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaType;
import fr.exensoft.cassandra.schemaupdate.model.Column;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;

public class CreateIndexDelta extends ColumnAbstractDelta {


    public CreateIndexDelta(Keyspace keyspace, Table table, Column source, Column target) {
        super(keyspace, table, source, target);
    }

    @Override
    public DeltaType getDeltaType() {
        return DeltaType.CREATE_INDEX;
    }

    @Override
    public int getPriority() {
        return DeltaPriorities.CREATE_INDEX;
    }

    @Override
    public String generateCQL() {
        return String.format("CREATE INDEX \"%s\" ON \"%s\".\"%s\" (\"%s\")", table.getIndex(target).getName(), keyspace.getName(), table.getName(), target.getName());
    }

    @Override
    public String toString() {
        return String.format("Create index \"%s\" on column \"%s\" on table \"%s\"", table.getIndex(target).getName(), target.getName(), table.getName());
    }
}
