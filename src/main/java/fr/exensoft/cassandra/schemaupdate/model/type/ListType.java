package fr.exensoft.cassandra.schemaupdate.model.type;

public class ListType implements ColumnType, OneParameterType{

    public final static String VALIDATOR = "org.apache.cassandra.db.marshal.ListType";

    private ColumnType innerType;

    public ListType(ColumnType innerType) {
        this.innerType = innerType;
    }

    public String getType() {
        return String.format("list<%s>", innerType.getType());
    }

    @Override
    public String toString() {
        return String.format("List<%s>", innerType);
    }

    @Override
    public ColumnType getInnerType() {
        return innerType;
    }

    @Override
    public boolean equals(Object type) {
        if(!(type instanceof ListType)) {
            return false;
        }
        ListType other = (ListType) type;
        return innerType.equals(other.innerType);
    }
}
