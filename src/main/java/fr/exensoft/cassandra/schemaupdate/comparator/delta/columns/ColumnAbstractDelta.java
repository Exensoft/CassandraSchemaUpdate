package fr.exensoft.cassandra.schemaupdate.comparator.delta.columns;

import fr.exensoft.cassandra.schemaupdate.comparator.delta.AbstractDelta;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaType;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.ElementType;
import fr.exensoft.cassandra.schemaupdate.model.Column;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;

public abstract class ColumnAbstractDelta extends AbstractDelta{

    protected Table table;
    protected Column source;
    protected Column target;

    public ColumnAbstractDelta(Keyspace keyspace, Table table, Column source, Column target) {
        super(keyspace);
        this.table = table;
        this.target = target;
        this.source = source;
    }

    @Override
    public ElementType getElementType() {
        return ElementType.COLUMN;
    }

    public Table getTable() {
        return table;
    }

    public Column getSource() {
        return source;
    }

    public Column getTarget() {
        return target;
    }
}
