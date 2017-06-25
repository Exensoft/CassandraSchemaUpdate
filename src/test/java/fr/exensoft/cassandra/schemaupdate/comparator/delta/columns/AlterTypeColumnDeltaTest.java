package fr.exensoft.cassandra.schemaupdate.comparator.delta.columns;

import fr.exensoft.cassandra.schemaupdate.model.Column;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;
import fr.exensoft.cassandra.schemaupdate.model.type.BasicType;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AlterTypeColumnDeltaTest {

    @Test
    public void testCQL() {
        Keyspace keyspace = new Keyspace("keyspace1");
        Table table = new Table("table1");
        Column source_column = new Column("column1", BasicType.INT);
        Column target_column = new Column("column1", BasicType.BLOB);
        AlterTypeColumnDelta alterTypeColumnDelta = new AlterTypeColumnDelta(keyspace, table, source_column, target_column);

        assertThat(alterTypeColumnDelta.generateCQL())
                .isEqualTo("ALTER TABLE \"keyspace1\".\"table1\" ALTER \"column1\" TYPE blob");
    }
}
