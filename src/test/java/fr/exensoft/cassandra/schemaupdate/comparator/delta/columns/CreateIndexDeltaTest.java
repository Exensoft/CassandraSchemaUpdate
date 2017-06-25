package fr.exensoft.cassandra.schemaupdate.comparator.delta.columns;

import fr.exensoft.cassandra.schemaupdate.model.Column;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;
import fr.exensoft.cassandra.schemaupdate.model.type.BasicType;
import fr.exensoft.cassandra.schemaupdate.model.type.SetType;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CreateIndexDeltaTest {

    @Test
    public void testCQL() {
        Keyspace keyspace = new Keyspace("keyspace1");
        Table table = new Table("table1");
        Column column = new Column("column1", new SetType(BasicType.INT));

        table.addColumn(column);
        table.addIndex("index1", "column1");

        CreateIndexDelta createIndexDelta = new CreateIndexDelta(keyspace, table, null, column);

        assertThat(createIndexDelta.generateCQL())
                .isEqualTo("CREATE INDEX \"index1\" ON \"keyspace1\".\"table1\" (\"column1\")");
    }
}
