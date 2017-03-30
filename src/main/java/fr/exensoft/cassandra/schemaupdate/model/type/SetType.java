package fr.exensoft.cassandra.schemaupdate.model.type;

import java.util.Set;

public class SetType implements ColumnType, OneParameterType{

    public final static String VALIDATOR = "org.apache.cassandra.db.marshal.SetType";

    private ColumnType innerType;

    public SetType(ColumnType innerType) {
        this.innerType = innerType;
    }

    public String getType() {
        return String.format("set<%s>", innerType.getType());
    }

    @Override
    public String toString() {
        return String.format("Set<%s>", innerType);
    }

    @Override
    public ColumnType getInnerType() {
        return innerType;
    }

    @Override
    public boolean equals(Object type) {
        if(!(type instanceof SetType)) {
            return false;
        }
        SetType other = (SetType) type;
        return innerType.equals(other.innerType);
    }
}
