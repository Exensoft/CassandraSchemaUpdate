package fr.exensoft.cassandra.schemaupdate.model.type;

public class FrozenType implements ColumnType, OneParameterType{

    public final static String VALIDATOR = "org.apache.cassandra.db.marshal.FrozenType";

    private ColumnType innerType;

    public FrozenType(ColumnType innerType) {
        this.innerType = innerType;
    }

    public String getType() {
        return String.format("frozen<%s>", innerType.getType());
    }

    @Override
    public String toString() {
        return String.format("Frozen<%s>", innerType);
    }

    @Override
    public boolean equals(Object type) {
        if(!(type instanceof FrozenType)) {
            return false;
        }
        FrozenType other = (FrozenType) type;
        return innerType.equals(other.innerType);
    }

    @Override
    public ColumnType getInnerType() {
        return innerType;
    }
}
