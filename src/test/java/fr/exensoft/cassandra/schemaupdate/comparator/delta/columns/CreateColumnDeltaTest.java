package fr.exensoft.cassandra.schemaupdate.comparator.delta.columns;

import fr.exensoft.cassandra.schemaupdate.model.Column;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;
import fr.exensoft.cassandra.schemaupdate.model.type.BasicType;
import fr.exensoft.cassandra.schemaupdate.model.type.SetType;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CreateColumnDeltaTest {

    @Test
    public void testCQL() {
        Keyspace keyspace = new Keyspace("keyspace1");
        Table table = new Table("table1");
        Column column = new Column("column1", new SetType(BasicType.INT));

        CreateColumnDelta createColumnDelta = new CreateColumnDelta(keyspace, table, column);

        assertThat(createColumnDelta.generateCQL())
                .isEqualTo("ALTER TABLE \"keyspace1\".\"table1\" ADD \"column1\" set<int>");
    }
}
