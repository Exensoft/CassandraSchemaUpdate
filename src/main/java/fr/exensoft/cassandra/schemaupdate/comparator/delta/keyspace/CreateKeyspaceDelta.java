package fr.exensoft.cassandra.schemaupdate.comparator.delta.keyspace;

import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaPriorities;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaType;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;

public class CreateKeyspaceDelta extends KeyspaceAbstractDelta{

    public CreateKeyspaceDelta(Keyspace keyspace) {
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
        return String.format("CREATE KEYSPACE \"%s\" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;", keyspace.getName());
    }

    @Override
    public String toString() {
        return String.format("Create keyspace \"%s\"", keyspace.getName());
    }

}
