package fr.exensoft.cassandra.schemaupdate.comparator.delta.table;

import fr.exensoft.cassandra.schemaupdate.model.Column;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;
import fr.exensoft.cassandra.schemaupdate.model.type.BasicType;
import fr.exensoft.cassandra.schemaupdate.model.values.SortOrder;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DropTableDeltaTest {

    @Test
    public void simpleTableCQL() {
        Keyspace keyspace = new Keyspace("keyspace1");

        Table table = new Table("table1");

        DropTableDelta dropTableDelta = new DropTableDelta(keyspace, table);

        assertThat(dropTableDelta.generateCQL())
                .isEqualTo("DROP TABLE \"keyspace1\".\"table1\"");
    }

}
