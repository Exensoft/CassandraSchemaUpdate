package fr.exensoft.cassandra.schemaupdate.comparator;


import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaList;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaResult;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.columns.*;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaFlag;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.keyspace.CreateKeyspaceDelta;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.keyspace.DropKeyspaceDelta;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.table.CreateTableDelta;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.table.DropTableDelta;
import fr.exensoft.cassandra.schemaupdate.model.Column;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;
import fr.exensoft.cassandra.schemaupdate.model.type.BasicType;
import fr.exensoft.cassandra.schemaupdate.model.values.SortOrder;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class KeyspaceComparatorTest {

    @Test
    public void createKeyspaceTest() {

        Keyspace targetKeyspace = new Keyspace("test")
                .addTable(new Table("table1")
                        .addColumn(new Column("column1", BasicType.UUID))
                        .addColumn(new Column("column2", BasicType.VARCHAR))
                        .addColumn(new Column("column3", BasicType.VARINT))
                        .addPartitioningKey("column1")
                )
                .addTable(new Table("table2")
                        .addColumn(new Column("column1", BasicType.TEXT))
                        .addPartitioningKey("column1")
                )
                ;


        KeyspaceComparator keyspaceComparator = new KeyspaceComparator(null, targetKeyspace);

        DeltaResult result = keyspaceComparator.compare();

        assertThat(result.hasUpdate()).isTrue();

        assertThat(result.getKeyspaceDelta()).isNotNull();
        assertThat(result.getKeyspaceDelta().getDeltas().get(0)).isInstanceOf(CreateKeyspaceDelta.class);
        assertThat(result.getTablesDelta()).containsOnlyKeys("table1", "table2");
        assertThat(result.getTablesDelta().get("table1").getDeltas().get(0)).isInstanceOf(CreateTableDelta.class);
        assertThat(result.getTablesDelta().get("table2").getDeltas().get(0)).isInstanceOf(CreateTableDelta.class);
    }

    @Test
    public void dropKeyspaceTest() {

        Keyspace sourceKeyspace = new Keyspace("test")
                .addTable(new Table("table1")
                        .addColumn(new Column("column1", BasicType.UUID))
                        .addColumn(new Column("column2", BasicType.VARCHAR))
                        .addColumn(new Column("column3", BasicType.VARINT))
                        .addPartitioningKey("column1")
                )
                .addTable(new Table("table2")
                        .addColumn(new Column("column1", BasicType.TEXT))
                        .addPartitioningKey("column1")
                )
                ;


        KeyspaceComparator keyspaceComparator = new KeyspaceComparator(sourceKeyspace, null);

        DeltaResult result = keyspaceComparator.compare();

        assertThat(result.hasUpdate()).isTrue();

        assertThat(result.getKeyspaceDelta()).isNotNull();
        assertThat(result.getKeyspaceDelta().getDeltas().get(0)).isInstanceOf(DropKeyspaceDelta.class);
        assertThat(result.getTablesDelta()).isEmpty();

    }

    @Test
    public void renameKeyspaceTest() {

        Keyspace sourceKeyspace = new Keyspace("test")
                .addTable(new Table("table1")
                        .addColumn(new Column("column1", BasicType.UUID))
                        .addColumn(new Column("column2", BasicType.VARCHAR))
                        .addColumn(new Column("column3", BasicType.VARINT))
                        .addPartitioningKey("column1")
                )
                .addTable(new Table("table2")
                        .addColumn(new Column("column1", BasicType.TEXT))
                        .addPartitioningKey("column1")
                )
                ;

        Keyspace targetKeyspace = new Keyspace("target")
                .addTable(new Table("table1")
                        .addColumn(new Column("column1", BasicType.UUID))
                        .addColumn(new Column("column2", BasicType.VARCHAR))
                        .addColumn(new Column("column3", BasicType.VARINT))
                        .addPartitioningKey("column1")
                )
                .addTable(new Table("table2")
                        .addColumn(new Column("column1", BasicType.TEXT))
                        .addPartitioningKey("column1")
                )
                ;


        KeyspaceComparator keyspaceComparator = new KeyspaceComparator(sourceKeyspace, targetKeyspace);

        DeltaResult result = keyspaceComparator.compare();

        assertThat(result.hasUpdate()).isTrue();

        assertThat(result.getKeyspaceDelta()).isNotNull();
        assertThat(result.getKeyspaceDelta().hasFlag(DeltaFlag.DATA_LOSS)).isTrue();
        assertThat(result.getKeyspaceDelta().getDeltas().get(0)).isInstanceOf(DropKeyspaceDelta.class);
        assertThat(result.getKeyspaceDelta().getDeltas().get(1)).isInstanceOf(CreateKeyspaceDelta.class);

        assertThat(result.getTablesDelta()).containsOnlyKeys("table1", "table2");
        assertThat(result.getTablesDelta().get("table1").getDeltas().get(0)).isInstanceOf(CreateTableDelta.class);
        assertThat(result.getTablesDelta().get("table2").getDeltas().get(0)).isInstanceOf(CreateTableDelta.class);

    }

    @Test
    public void dropTableTest() {

        Keyspace sourceKeyspace = new Keyspace("test")
                .addTable(new Table("table1")
                        .addColumn(new Column("column1", BasicType.UUID))
                        .addColumn(new Column("column2", BasicType.VARCHAR))
                        .addColumn(new Column("column3", BasicType.VARINT))
                        .addPartitioningKey("column1")
                )
                .addTable(new Table("table2")
                        .addColumn(new Column("column1", BasicType.TEXT))
                        .addPartitioningKey("column1")
                )
                ;

        Keyspace targetKeyspace = new Keyspace("test")
                .addTable(new Table("table2")
                        .addColumn(new Column("column1", BasicType.TEXT))
                        .addPartitioningKey("column1")
                )
                ;


        KeyspaceComparator keyspaceComparator = new KeyspaceComparator(sourceKeyspace, targetKeyspace);

        DeltaResult result = keyspaceComparator.compare();

        assertThat(result.hasUpdate()).isTrue();

        assertThat(result.getKeyspaceDelta().hasUpdate()).isFalse();

        assertThat(result.getTablesDelta()).containsOnlyKeys("table1", "table2");

        assertThat(result.getTablesDelta().get("table1").hasUpdate()).isTrue();
        assertThat(result.getTablesDelta().get("table1").getDeltas().get(0)).isInstanceOf(DropTableDelta.class);

        assertThat(result.getTablesDelta().get("table2").hasUpdate()).isFalse();

    }

    @Test
    public void createTableTest() {

        Keyspace sourceKeyspace = new Keyspace("test")
                .addTable(new Table("table2")
                        .addColumn(new Column("column1", BasicType.TEXT))
                        .addPartitioningKey("column1")
                )
                ;

        Keyspace targetKeyspace = new Keyspace("test")
                .addTable(new Table("table1")
                        .addColumn(new Column("column1", BasicType.UUID))
                        .addColumn(new Column("column2", BasicType.VARCHAR))
                        .addColumn(new Column("column3", BasicType.VARINT))
                        .addPartitioningKey("column1")
                )
                .addTable(new Table("table2")
                        .addColumn(new Column("column1", BasicType.TEXT))
                        .addPartitioningKey("column1")
                )
                ;


        KeyspaceComparator keyspaceComparator = new KeyspaceComparator(sourceKeyspace, targetKeyspace);

        DeltaResult result = keyspaceComparator.compare();

        assertThat(result.hasUpdate()).isTrue();

        assertThat(result.getKeyspaceDelta().hasUpdate()).isFalse();

        assertThat(result.getTablesDelta()).containsOnlyKeys("table1", "table2");

        assertThat(result.getTablesDelta().get("table1").hasUpdate()).isTrue();
        assertThat(result.getTablesDelta().get("table1").getDeltas().get(0)).isInstanceOf(CreateTableDelta.class);

        assertThat(result.getTablesDelta().get("table2").hasUpdate()).isFalse();

    }

}
