package fr.exensoft.cassandra.schemaupdate.model.type;

import java.util.Arrays;

public enum BasicType implements ColumnType{
    ASCII("ascii", "org.apache.cassandra.db.marshal.AsciiType"),
    BIGINT("bigint", "org.apache.cassandra.db.marshal.LongType"),
    BLOB("blob", "org.apache.cassandra.db.marshal.BytesType"),
    BOOLEAN("boolean", "org.apache.cassandra.db.marshal.BooleanType"),
    COUNTER("counter", "org.apache.cassandra.db.marshal.CounterColumnType"),
    DECIMAL("decimal", "org.apache.cassandra.db.marshal.DecimalType"),
    DOUBLE("double", "org.apache.cassandra.db.marshal.DoubleType"),
    FLOAT("float", "org.apache.cassandra.db.marshal.FloatType"),
    INET("inet", "org.apache.cassandra.db.marshal.InetAddressType"),
    INT("int", "org.apache.cassandra.db.marshal.Int32Type"),
    TEXT("text", "org.apache.cassandra.db.marshal.UTF8Type"),
    TIMESTAMP("timestamp", "org.apache.cassandra.db.marshal.TimestampType"),
    TIMEUUID("timeuuid", "org.apache.cassandra.db.marshal.TimeUUIDType"),
    UUID("uuid", "org.apache.cassandra.db.marshal.UUIDType"),
    VARCHAR("varchar", "org.apache.cassandra.db.marshal.UTF8Type"),
    VARINT("varint", "org.apache.cassandra.db.marshal.IntegerType");

    private String type;

    private String validator;

    private BasicType(String type, String validator) {
        this.type = type;
        this.validator = validator;
    }

    public String getType() {
        return type;
    }

    public static BasicType fromValidator(String validator) {
        return Arrays.stream(values())
                .filter(type->type.validator.equals(validator))
                .findFirst()
                .orElse(null);
    }
}
