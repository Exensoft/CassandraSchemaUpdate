package fr.exensoft.cassandra.schemaupdate.comparator.delta.table;

import fr.exensoft.cassandra.schemaupdate.model.Column;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;
import fr.exensoft.cassandra.schemaupdate.model.type.BasicType;
import fr.exensoft.cassandra.schemaupdate.model.values.SortOrder;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CreateTableDeltaTest {

    @Test
    public void simpleTableCQL() {
        Keyspace keyspace = new Keyspace("keyspace1");

        Table table = new Table("table1")
                .addColumn(new Column("column1", BasicType.TEXT))
                .addColumn(new Column("column2", BasicType.INT))
                .addColumn(new Column("column3", BasicType.BLOB))
                .addPartitioningKey("column1");

        CreateTableDelta createTableDelta = new CreateTableDelta(keyspace, table);

        assertThat(createTableDelta.generateCQL())
                .isEqualTo("CREATE TABLE \"keyspace1\".\"table1\" (\"column1\" text, \"column2\" int, \"column3\" blob, PRIMARY KEY (\"column1\"));");
    }

    @Test
    public void withClusteringColumnTableCQL() {
        Keyspace keyspace = new Keyspace("keyspace1");

        Table table = new Table("table1")
                .addColumn(new Column("column1", BasicType.TEXT))
                .addColumn(new Column("column2", BasicType.INT))
                .addColumn(new Column("column3", BasicType.INT))
                .addPartitioningKey("column1")
                .addClusteringColumn("column2")
                .addClusteringColumn("column3", SortOrder.DESC);

        CreateTableDelta createTableDelta = new CreateTableDelta(keyspace, table);

        assertThat(createTableDelta.generateCQL())
                .isEqualTo("CREATE TABLE \"keyspace1\".\"table1\" (\"column1\" text, \"column2\" int, \"column3\" int, PRIMARY KEY (\"column1\", \"column2\", \"column3\")) WITH CLUSTERING ORDER BY (\"column3\" DESC);");
    }

}
