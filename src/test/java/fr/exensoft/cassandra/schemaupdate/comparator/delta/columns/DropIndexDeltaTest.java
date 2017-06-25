package fr.exensoft.cassandra.schemaupdate.comparator.delta.columns;

import fr.exensoft.cassandra.schemaupdate.model.Column;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;
import fr.exensoft.cassandra.schemaupdate.model.type.BasicType;
import fr.exensoft.cassandra.schemaupdate.model.type.SetType;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DropIndexDeltaTest {

    @Test
    public void testCQL() {
        Keyspace keyspace = new Keyspace("keyspace1");
        Table table = new Table("table1");
        Column column = new Column("column1", new SetType(BasicType.INT));

        table.addColumn(column);
        table.addIndex("index1", "column1");

        DropIndexDelta dropIndexDelta = new DropIndexDelta(keyspace, table, column, null);

        assertThat(dropIndexDelta.generateCQL())
                .isEqualTo("DROP INDEX \"keyspace1\".\"index1\"");
    }
}
