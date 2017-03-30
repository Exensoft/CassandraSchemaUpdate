package fr.exensoft.cassandra.schemaupdate.comparator.delta;

import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaType;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.ElementType;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;

public abstract class AbstractDelta implements Comparable<AbstractDelta>{

    protected Keyspace keyspace;

    public AbstractDelta(Keyspace keyspace) {
        this.keyspace = keyspace;
    }

    public Keyspace getKeyspace() {
        return keyspace;
    }

    public abstract ElementType getElementType();

    public abstract DeltaType getDeltaType();

    public abstract int getPriority();

    public abstract String generateCQL();

    @Override
    public int compareTo(AbstractDelta o) {
        return Integer.compare(getPriority(), o.getPriority());
    }
}
