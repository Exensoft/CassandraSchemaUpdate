package fr.exensoft.cassandra.schemaupdate.comparator.delta.keyspace;

import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaPriorities;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaType;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;

public class DropKeyspaceDelta extends KeyspaceAbstractDelta{

    public DropKeyspaceDelta(Keyspace keyspace) {
        super(keyspace);
    }

    @Override
    public DeltaType getDeltaType() {
        return DeltaType.CREATE;
    }

    @Override
    public int getPriority() {
        return DeltaPriorities.CREATE_KEYSPACE;
    }

    @Override
    public String generateCQL() {
        return String.format("DROP KEYSPACE \"%s\"", keyspace.getName());
    }

    @Override
    public String toString() {
        return String.format("Drop keyspace \"%s\"", keyspace.getName());
    }

}
