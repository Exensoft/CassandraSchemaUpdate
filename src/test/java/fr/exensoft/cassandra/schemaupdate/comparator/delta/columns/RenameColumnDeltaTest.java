package fr.exensoft.cassandra.schemaupdate.comparator.delta.columns;

import fr.exensoft.cassandra.schemaupdate.model.Column;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;
import fr.exensoft.cassandra.schemaupdate.model.type.BasicType;
import fr.exensoft.cassandra.schemaupdate.model.type.SetType;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RenameColumnDeltaTest {

    @Test
    public void testCQL() {
        Keyspace keyspace = new Keyspace("keyspace1");
        Table table = new Table("table1");
        Column source_column = new Column("column1", new SetType(BasicType.INT));
        Column target_column = new Column("column1_new_name", new SetType(BasicType.INT));

        RenameColumnDelta renameColumnDelta = new RenameColumnDelta(keyspace, table, source_column, target_column);

        assertThat(renameColumnDelta.generateCQL())
                .isEqualTo("ALTER TABLE \"keyspace1\".\"table1\" RENAME \"column1\" TO \"column1_new_name\"");
    }
}
