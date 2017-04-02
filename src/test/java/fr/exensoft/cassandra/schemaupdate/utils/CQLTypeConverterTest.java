package fr.exensoft.cassandra.schemaupdate.utils;

import fr.exensoft.cassandra.schemaupdate.SchemaUpdateException;
import fr.exensoft.cassandra.schemaupdate.model.type.*;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

public class CQLTypeConverterTest {

    @Test
    public void basicTypeTest() {
        ColumnType type = CQLTypeConverter.validatorToType("org.apache.cassandra.db.marshal.IntegerType");
        assertThat(type).isEqualTo(BasicType.VARINT);

        type = CQLTypeConverter.validatorToType("org.apache.cassandra.db.marshal.UTF8Type");
        assertThat(type).isEqualTo(BasicType.TEXT);
    }

    @Test
    public void composedTest() {
        ColumnType type = CQLTypeConverter.validatorToType("org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type)");
        assertThat(type).isEqualTo(new SetType(BasicType.TEXT));

        type = CQLTypeConverter.validatorToType("org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.Int32Type)))");
        assertThat(type).isEqualTo(new MapType(BasicType.TEXT, new FrozenType(new SetType(BasicType.INT))));

        type = CQLTypeConverter.validatorToType("org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.BytesType)");
        assertThat(type).isEqualTo(new ListType(BasicType.BLOB));
    }

    @Test
    public void invalidExpression() {
        assertThatThrownBy(()->
                CQLTypeConverter.validatorToType(""))
                .isInstanceOf(SchemaUpdateException.class)
                .hasMessageContaining("Invalid validator");
        assertThatThrownBy(()->
                CQLTypeConverter.validatorToType("(())())"))
                .isInstanceOf(SchemaUpdateException.class)
                .hasMessageContaining("Invalid validator");

        assertThatThrownBy(()->
                CQLTypeConverter.validatorToType("org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.BytesType,org.apache.cassandra.db.marshal.BytesType"))
                .isInstanceOf(SchemaUpdateException.class)
                .hasMessageContaining("Invalid validator");

        assertThatThrownBy(()->
                CQLTypeConverter.validatorToType("org.apache.cassandra.db.marshal.SetType)org.apache.cassandra.db.marshal.BytesType,org.apache.cassandra.db.marshal.BytesType)"))
                .isInstanceOf(SchemaUpdateException.class)
                .hasMessageContaining("Invalid validator");

        assertThatThrownBy(()->
                CQLTypeConverter.validatorToType("org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.BytesType"))
                .isInstanceOf(SchemaUpdateException.class)
                .hasMessageContaining("Invalid validator");

        assertThatThrownBy(()->
                CQLTypeConverter.validatorToType("org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.BytesType("))
                .isInstanceOf(SchemaUpdateException.class)
                .hasMessageContaining("Invalid validator");
    }

    @Test
    public void invalidDefinitions() {
        assertThatThrownBy(()->
                CQLTypeConverter.validatorToType("unknown_type"))
                .isInstanceOf(SchemaUpdateException.class)
                .hasMessageContaining("Unknown validator unknown_type");

        assertThatThrownBy(()->
                CQLTypeConverter.validatorToType("org.apache.cassandra.db.marshal.SetType"))
                .isInstanceOf(SchemaUpdateException.class)
                .hasMessageContaining("Set requires a type definition");

        assertThatThrownBy(()->
                CQLTypeConverter.validatorToType("org.apache.cassandra.db.marshal.ListType"))
                .isInstanceOf(SchemaUpdateException.class)
                .hasMessageContaining("List requires a type definition");

        assertThatThrownBy(()->
                CQLTypeConverter.validatorToType("org.apache.cassandra.db.marshal.FrozenType"))
                .isInstanceOf(SchemaUpdateException.class)
                .hasMessageContaining("Frozen requires a type definition");

        assertThatThrownBy(()->
                CQLTypeConverter.validatorToType("org.apache.cassandra.db.marshal.ReversedType"))
                .isInstanceOf(SchemaUpdateException.class)
                .hasMessageContaining("Can not use reversed validator without type");

        assertThatThrownBy(()->
                CQLTypeConverter.validatorToType("org.apache.cassandra.db.marshal.MapType"))
                .isInstanceOf(SchemaUpdateException.class)
                .hasMessageContaining("Map requires two type definition");
    }
}
