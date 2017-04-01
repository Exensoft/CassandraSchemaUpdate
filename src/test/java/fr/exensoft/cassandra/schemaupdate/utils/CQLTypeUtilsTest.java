package fr.exensoft.cassandra.schemaupdate.utils;

import fr.exensoft.cassandra.schemaupdate.model.type.*;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CQLTypeUtilsTest {

    @Test
    public void isCompatibleTest_SimpleCases() {
        assertThat(CQLTypeUtils.isCompatible(BasicType.INT, BasicType.INT)).isTrue();
        assertThat(CQLTypeUtils.isCompatible(BasicType.TEXT, BasicType.VARCHAR)).isTrue();
        assertThat(CQLTypeUtils.isCompatible(BasicType.VARCHAR, BasicType.TEXT)).isTrue();
        assertThat(CQLTypeUtils.isCompatible(BasicType.INT, BasicType.VARINT)).isTrue();
        assertThat(CQLTypeUtils.isCompatible(BasicType.INT, BasicType.BLOB)).isTrue();


        assertThat(CQLTypeUtils.isCompatible(BasicType.VARINT, BasicType.INT)).isFalse();
        assertThat(CQLTypeUtils.isCompatible(BasicType.BLOB, BasicType.INT)).isFalse();
        assertThat(CQLTypeUtils.isCompatible(BasicType.UUID, BasicType.INT)).isFalse();
    }

    @Test
    public void isCompatibleTest_HybridCases() {
        assertThat(CQLTypeUtils.isCompatible(BasicType.BLOB, new MapType(BasicType.INT, BasicType.VARCHAR))).isFalse();
        assertThat(CQLTypeUtils.isCompatible(new MapType(BasicType.INT, BasicType.VARCHAR), BasicType.BLOB)).isFalse();

        assertThat(CQLTypeUtils.isCompatible(BasicType.BLOB, new SetType(BasicType.VARCHAR))).isFalse();
        assertThat(CQLTypeUtils.isCompatible(new SetType(BasicType.VARCHAR), BasicType.BLOB)).isFalse();

        assertThat(CQLTypeUtils.isCompatible(BasicType.BLOB, new ListType(BasicType.VARCHAR))).isFalse();
        assertThat(CQLTypeUtils.isCompatible(new ListType(BasicType.VARCHAR), BasicType.BLOB)).isFalse();


        assertThat(CQLTypeUtils.isCompatible(new SetType(BasicType.VARCHAR), new ListType(BasicType.VARCHAR))).isFalse();
        assertThat(CQLTypeUtils.isCompatible(new ListType(BasicType.VARCHAR), new SetType(BasicType.VARCHAR))).isFalse();
    }

    @Test
    public void isCompatibleTest_ComplexCases() {
        assertThat(CQLTypeUtils.isCompatible( new MapType(BasicType.INT, BasicType.VARCHAR), new MapType(BasicType.INT, BasicType.VARCHAR))).isTrue();
        assertThat(CQLTypeUtils.isCompatible( new MapType(BasicType.INT, BasicType.VARCHAR), new MapType(BasicType.INT, BasicType.BLOB))).isTrue();
        assertThat(CQLTypeUtils.isCompatible( new MapType(BasicType.INT, BasicType.BLOB), new MapType(BasicType.INT, BasicType.VARCHAR))).isFalse();

        assertThat(CQLTypeUtils.isCompatible( new MapType(BasicType.INT, BasicType.VARCHAR), new MapType(BasicType.VARINT, BasicType.VARCHAR))).isFalse();
        assertThat(CQLTypeUtils.isCompatible( new MapType(BasicType.TEXT, BasicType.VARCHAR), new MapType(BasicType.VARCHAR, BasicType.VARCHAR))).isTrue();


        assertThat(CQLTypeUtils.isCompatible( new ListType(BasicType.VARCHAR), new ListType(BasicType.VARCHAR))).isTrue();
        assertThat(CQLTypeUtils.isCompatible( new ListType(BasicType.TEXT), new ListType(BasicType.VARCHAR))).isTrue();
        assertThat(CQLTypeUtils.isCompatible( new ListType(BasicType.VARCHAR), new ListType(BasicType.TEXT))).isTrue();
        assertThat(CQLTypeUtils.isCompatible( new ListType(BasicType.VARCHAR), new ListType(BasicType.BLOB))).isTrue();
        assertThat(CQLTypeUtils.isCompatible( new ListType(BasicType.VARCHAR), new ListType(BasicType.INT))).isFalse();


        assertThat(CQLTypeUtils.isCompatible( new SetType(BasicType.VARCHAR), new SetType(BasicType.VARCHAR))).isTrue();
        assertThat(CQLTypeUtils.isCompatible( new SetType(BasicType.TEXT), new SetType(BasicType.VARCHAR))).isTrue();
        assertThat(CQLTypeUtils.isCompatible( new SetType(BasicType.VARCHAR), new SetType(BasicType.TEXT))).isTrue();
        assertThat(CQLTypeUtils.isCompatible( new SetType(BasicType.VARCHAR), new SetType(BasicType.BLOB))).isTrue();
        assertThat(CQLTypeUtils.isCompatible( new SetType(BasicType.VARCHAR), new SetType(BasicType.INT))).isFalse();
    }


    @Test
    public void isOrderCompatibleTest_SimpleCases() {
        assertThat(CQLTypeUtils.isOrderCompatible(BasicType.INT, BasicType.INT)).isTrue();
        assertThat(CQLTypeUtils.isOrderCompatible(BasicType.TEXT, BasicType.VARCHAR)).isTrue();
        assertThat(CQLTypeUtils.isOrderCompatible(BasicType.VARCHAR, BasicType.TEXT)).isTrue();
        assertThat(CQLTypeUtils.isOrderCompatible(BasicType.INT, BasicType.VARINT)).isTrue();

        assertThat(CQLTypeUtils.isOrderCompatible(BasicType.VARINT, BasicType.INT)).isFalse();
        assertThat(CQLTypeUtils.isOrderCompatible(BasicType.INT, BasicType.BLOB)).isFalse();
        assertThat(CQLTypeUtils.isOrderCompatible(BasicType.VARINT, BasicType.INT)).isFalse();
        assertThat(CQLTypeUtils.isOrderCompatible(BasicType.BLOB, BasicType.INT)).isFalse();
        assertThat(CQLTypeUtils.isOrderCompatible(BasicType.TIMEUUID, BasicType.UUID)).isFalse();
    }

    @Test
    public void isOrderCompatibleTest_HybridCases() {
        assertThat(CQLTypeUtils.isOrderCompatible(BasicType.BLOB, new MapType(BasicType.INT, BasicType.VARCHAR))).isFalse();
        assertThat(CQLTypeUtils.isOrderCompatible(new MapType(BasicType.INT, BasicType.VARCHAR), BasicType.BLOB)).isFalse();

        assertThat(CQLTypeUtils.isOrderCompatible(BasicType.BLOB, new SetType(BasicType.VARCHAR))).isFalse();
        assertThat(CQLTypeUtils.isOrderCompatible(new SetType(BasicType.VARCHAR), BasicType.BLOB)).isFalse();

        assertThat(CQLTypeUtils.isOrderCompatible(BasicType.BLOB, new ListType(BasicType.VARCHAR))).isFalse();
        assertThat(CQLTypeUtils.isOrderCompatible(new ListType(BasicType.VARCHAR), BasicType.BLOB)).isFalse();


        assertThat(CQLTypeUtils.isOrderCompatible(new SetType(BasicType.VARCHAR), new ListType(BasicType.VARCHAR))).isFalse();
        assertThat(CQLTypeUtils.isOrderCompatible(new ListType(BasicType.VARCHAR), new SetType(BasicType.VARCHAR))).isFalse();
    }

    @Test
    public void isOrderCompatibleTest_ComplexCases() {
        assertThat(CQLTypeUtils.isOrderCompatible( new MapType(BasicType.INT, BasicType.VARCHAR), new MapType(BasicType.INT, BasicType.VARCHAR))).isTrue();
        assertThat(CQLTypeUtils.isOrderCompatible( new MapType(BasicType.INT, BasicType.VARCHAR), new MapType(BasicType.INT, BasicType.BLOB))).isFalse();
        assertThat(CQLTypeUtils.isOrderCompatible( new MapType(BasicType.INT, BasicType.BLOB), new MapType(BasicType.INT, BasicType.VARCHAR))).isFalse();

        assertThat(CQLTypeUtils.isOrderCompatible( new MapType(BasicType.INT, BasicType.VARCHAR), new MapType(BasicType.VARINT, BasicType.VARCHAR))).isFalse();
        assertThat(CQLTypeUtils.isOrderCompatible( new MapType(BasicType.TEXT, BasicType.VARCHAR), new MapType(BasicType.VARCHAR, BasicType.VARCHAR))).isTrue();


        assertThat(CQLTypeUtils.isOrderCompatible( new ListType(BasicType.VARCHAR), new ListType(BasicType.VARCHAR))).isTrue();
        assertThat(CQLTypeUtils.isOrderCompatible( new ListType(BasicType.TEXT), new ListType(BasicType.VARCHAR))).isTrue();
        assertThat(CQLTypeUtils.isOrderCompatible( new ListType(BasicType.VARCHAR), new ListType(BasicType.TEXT))).isTrue();
        assertThat(CQLTypeUtils.isOrderCompatible( new ListType(BasicType.VARCHAR), new ListType(BasicType.BLOB))).isFalse();
        assertThat(CQLTypeUtils.isOrderCompatible( new ListType(BasicType.VARCHAR), new ListType(BasicType.INT))).isFalse();


        assertThat(CQLTypeUtils.isOrderCompatible( new SetType(BasicType.VARCHAR), new SetType(BasicType.VARCHAR))).isTrue();
        assertThat(CQLTypeUtils.isOrderCompatible( new SetType(BasicType.TEXT), new SetType(BasicType.VARCHAR))).isTrue();
        assertThat(CQLTypeUtils.isOrderCompatible( new SetType(BasicType.VARCHAR), new SetType(BasicType.TEXT))).isTrue();
        assertThat(CQLTypeUtils.isOrderCompatible( new SetType(BasicType.VARCHAR), new SetType(BasicType.BLOB))).isFalse();
        assertThat(CQLTypeUtils.isOrderCompatible( new SetType(BasicType.VARCHAR), new SetType(BasicType.INT))).isFalse();
    }


    @Test
    public void equalsTest_SimpleCases() {
        assertThat(CQLTypeUtils.equals(BasicType.INT, BasicType.INT)).isTrue();
        assertThat(CQLTypeUtils.equals(BasicType.VARCHAR, BasicType.VARCHAR)).isTrue();
        assertThat(CQLTypeUtils.equals(BasicType.VARCHAR, BasicType.TEXT)).isTrue();
        assertThat(CQLTypeUtils.equals(BasicType.TEXT, BasicType.VARCHAR)).isTrue();
        assertThat(CQLTypeUtils.equals(BasicType.BLOB, BasicType.BLOB)).isTrue();


        assertThat(CQLTypeUtils.equals(BasicType.VARINT, BasicType.INT)).isFalse();
        assertThat(CQLTypeUtils.equals(BasicType.BLOB, BasicType.INT)).isFalse();
        assertThat(CQLTypeUtils.equals(BasicType.UUID, BasicType.INT)).isFalse();
        assertThat(CQLTypeUtils.equals(BasicType.VARCHAR, BasicType.BLOB)).isFalse();
    }

    @Test
    public void equalsTest_HybridCases() {
        assertThat(CQLTypeUtils.equals(BasicType.INT, new MapType(BasicType.INT, BasicType.VARCHAR))).isFalse();
        assertThat(CQLTypeUtils.equals(BasicType.INT, new ListType(BasicType.INT))).isFalse();
        assertThat(CQLTypeUtils.equals(BasicType.INT, new SetType(BasicType.INT))).isFalse();
        assertThat(CQLTypeUtils.equals(new SetType(BasicType.INT), new FrozenType(new SetType(BasicType.INT)))).isFalse();
    }


    private boolean equals(ColumnType type) {
        return CQLTypeUtils.equals(type, type);
    }
    @Test
    public void equalsTest_ComplexCases() {
        assertThat(equals(new MapType(BasicType.INT, BasicType.VARCHAR))).isTrue();
        assertThat(equals(new MapType(BasicType.VARINT, BasicType.BLOB))).isTrue();
        assertThat(equals(new MapType(BasicType.INT, new FrozenType(new SetType(BasicType.INT))))).isTrue();
        assertThat(equals(new SetType(new MapType(BasicType.INT, BasicType.VARCHAR)))).isTrue();
        assertThat(CQLTypeUtils.equals(new MapType(BasicType.INT, BasicType.TEXT), new MapType(BasicType.INT, BasicType.VARCHAR))).isTrue();
        assertThat(CQLTypeUtils.equals(new MapType(BasicType.VARCHAR, BasicType.TEXT), new MapType(BasicType.TEXT, BasicType.VARCHAR))).isTrue();


        assertThat(CQLTypeUtils.equals(new MapType(BasicType.INT, BasicType.BLOB), new MapType(BasicType.INT, BasicType.VARCHAR))).isFalse();
        assertThat(CQLTypeUtils.equals(new MapType(BasicType.INT, BasicType.BLOB), new MapType(BasicType.UUID, BasicType.BLOB))).isFalse();
    }
}
