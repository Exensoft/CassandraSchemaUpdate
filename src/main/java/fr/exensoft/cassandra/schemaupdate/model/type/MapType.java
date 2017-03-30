package fr.exensoft.cassandra.schemaupdate.model.type;

import fr.exensoft.cassandra.schemaupdate.model.Column;

public class MapType implements ColumnType{

    public final static String VALIDATOR = "org.apache.cassandra.db.marshal.MapType";

    private ColumnType keyType;

    private ColumnType valueType;

    public MapType(ColumnType keyType, ColumnType valueType) {
        this.keyType = keyType;
        this.valueType = valueType;
    }

    public ColumnType getKeyType() {
        return keyType;
    }

    public ColumnType getValueType() {
        return valueType;
    }

    public String getType() {
        return String.format("map<%s,%s>", keyType.getType(), valueType.getType());
    }

    @Override
    public String toString() {
        return String.format("Map<%s, %s>", keyType, valueType);
    }

    @Override
    public boolean equals(Object type) {
        if(!(type instanceof MapType)) {
            return false;
        }
        MapType other = (MapType) type;
        return keyType.equals(other.keyType) && valueType.equals(other.valueType);
    }
}
