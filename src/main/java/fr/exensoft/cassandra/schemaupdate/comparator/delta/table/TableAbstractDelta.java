package fr.exensoft.cassandra.schemaupdate.comparator.delta.table;

import fr.exensoft.cassandra.schemaupdate.comparator.delta.AbstractDelta;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaType;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.ElementType;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;

public abstract class TableAbstractDelta extends AbstractDelta{

    protected Table source;
    protected Table target;

    public TableAbstractDelta(Keyspace keyspace, Table source, Table target) {
        super(keyspace);
        this.source = source;
        this.target = target;
    }

    @Override
    public ElementType getElementType() {
        return ElementType.TABLE;
    }

    public Table getSource() {
        return source;
    }

    public Table getTarget() {
        return target;
    }
}
