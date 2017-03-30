package fr.exensoft.cassandra.schemaupdate.comparator.delta.keyspace;


import fr.exensoft.cassandra.schemaupdate.comparator.delta.AbstractDelta;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.ElementType;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;

public abstract class KeyspaceAbstractDelta extends AbstractDelta {

    public KeyspaceAbstractDelta(Keyspace keyspace) {
        super(keyspace);
    }

    public ElementType getElementType() {
        return ElementType.KEYSPACE;
    }
}
