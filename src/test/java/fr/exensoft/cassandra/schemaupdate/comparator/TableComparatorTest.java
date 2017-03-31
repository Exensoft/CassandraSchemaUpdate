package fr.exensoft.cassandra.schemaupdate.comparator;


import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaList;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.columns.*;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaFlag;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.table.CreateTableDelta;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.table.DropTableDelta;
import fr.exensoft.cassandra.schemaupdate.model.Column;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;
import fr.exensoft.cassandra.schemaupdate.model.type.BasicType;
import fr.exensoft.cassandra.schemaupdate.model.values.SortOrder;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class TableComparatorTest {

    private Keyspace keyspace = new Keyspace("test");

    @Test
    public void createTable() {

        Table targetTable = new Table("test_table")
                .addColumn(new Column("name", BasicType.TEXT))
                .addPartitioningKey("name")
                .setKeyspace(keyspace);
        TableComparator tableComparator = new TableComparator(null, targetTable);

        DeltaList list = tableComparator.compare();

        assertThat(list.hasUpdate()).isTrue();
        assertThat(list.getDeltas()).hasSize(1);
        assertThat(list.getFlags()).isEmpty();

        assertThat(list.getDeltas().get(0)).isInstanceOf(CreateTableDelta.class);
        assertThat(((CreateTableDelta) list.getDeltas().get(0)).getSource()).isNull();
        assertThat(((CreateTableDelta) list.getDeltas().get(0)).getTarget()).isEqualTo(targetTable);
    }

    @Test
    public void dropTable() {

        Table sourceTable = new Table("test_table")
                .addColumn(new Column("name", BasicType.TEXT))
                .addPartitioningKey("name")
                .setKeyspace(keyspace);

        TableComparator tableComparator = new TableComparator(sourceTable, null);

        DeltaList list = tableComparator.compare();

        assertThat(list.hasUpdate()).isTrue();
        assertThat(list.getDeltas()).hasSize(1);
        assertThat(list.getFlags()).containsExactly(DeltaFlag.DATA_LOSS);

        assertThat(list.getDeltas().get(0)).isInstanceOf(DropTableDelta.class);
        assertThat(((DropTableDelta) list.getDeltas().get(0)).getSource()).isEqualTo(sourceTable);
        assertThat(((DropTableDelta) list.getDeltas().get(0)).getTarget()).isNull();
    }

    @Test
    public void renamePartitioningKeyColumn() {
        Table sourceTable = new Table("test_table")
                .addColumn(new Column("name", BasicType.TEXT))
                .addPartitioningKey("name")
                .setKeyspace(keyspace);

        Table targetTable = new Table("test_table2")
                .addColumn(new Column("name2", BasicType.TEXT).addOldName("name"))
                .addPartitioningKey("name2")
                .setKeyspace(keyspace);

        TableComparator tableComparator = new TableComparator(sourceTable, targetTable);

        DeltaList list = tableComparator.compare();

        assertThat(list.hasUpdate()).isTrue();
        assertThat(list.getDeltas()).hasSize(1);
        assertThat(list.getFlags()).isEmpty();

        assertThat(list.getDeltas().get(0)).isInstanceOf(RenameColumnDelta.class);
        assertThat(((ColumnAbstractDelta) list.getDeltas().get(0)).getSource()).isEqualTo(sourceTable.getColumn("name"));
        assertThat(((ColumnAbstractDelta) list.getDeltas().get(0)).getTarget()).isEqualTo(targetTable.getColumn("name2"));
    }

    @Test
    public void renameClusteringKeyColumn() {
        Table sourceTable = new Table("test_table")
                .addColumn(new Column("name", BasicType.TEXT))
                .addColumn(new Column("column2", BasicType.TEXT))
                .addPartitioningKey("name")
                .addClusteringColumn("column2")
                .setKeyspace(keyspace);

        Table targetTable = new Table("test_table2")
                .addColumn(new Column("name", BasicType.TEXT))
                .addColumn(new Column("column2_bis", BasicType.TEXT).addOldName("column2"))
                .addPartitioningKey("name")
                .addClusteringColumn("column2_bis")
                .setKeyspace(keyspace);

        TableComparator tableComparator = new TableComparator(sourceTable, targetTable);

        DeltaList list = tableComparator.compare();

        assertThat(list.hasUpdate()).isTrue();
        assertThat(list.getDeltas()).hasSize(1);
        assertThat(list.getFlags()).isEmpty();

        assertThat(list.getDeltas().get(0)).isInstanceOf(RenameColumnDelta.class);
        assertThat(((ColumnAbstractDelta) list.getDeltas().get(0)).getSource()).isEqualTo(sourceTable.getColumn("column2"));
        assertThat(((ColumnAbstractDelta) list.getDeltas().get(0)).getTarget()).isEqualTo(targetTable.getColumn("column2_bis"));
    }

    @Test
    public void renameNonKeyColumn() {
        Table sourceTable = new Table("test_table")
                .addColumn(new Column("name", BasicType.TEXT))
                .addColumn(new Column("column2", BasicType.TEXT))
                .addColumn(new Column("column3", BasicType.TEXT))
                .addPartitioningKey("name")
                .addClusteringColumn("column2")
                .setKeyspace(keyspace);

        Table targetTable = new Table("test_table2")
                .addColumn(new Column("name", BasicType.TEXT))
                .addColumn(new Column("column2", BasicType.TEXT))
                .addColumn(new Column("column3_bis", BasicType.TEXT).addOldName("column3"))
                .addPartitioningKey("name")
                .addClusteringColumn("column2")
                .setKeyspace(keyspace);

        TableComparator tableComparator = new TableComparator(sourceTable, targetTable);

        DeltaList list = tableComparator.compare();

        assertThat(list.hasUpdate()).isTrue();
        assertThat(list.getDeltas()).hasSize(2);
        assertThat(list.getFlags()).contains(DeltaFlag.DATA_LOSS);

        assertThat(list.getDeltas().get(0)).isInstanceOf(DropColumnDelta.class);
        assertThat(((ColumnAbstractDelta) list.getDeltas().get(0)).getSource()).isEqualTo(sourceTable.getColumn("column3"));
        assertThat(((ColumnAbstractDelta) list.getDeltas().get(0)).getTarget()).isNull();

        assertThat(list.getDeltas().get(1)).isInstanceOf(CreateColumnDelta.class);
        assertThat(((CreateColumnDelta) list.getDeltas().get(1)).getSource()).isNull();
        assertThat(((CreateColumnDelta) list.getDeltas().get(1)).getTarget()).isEqualTo(targetTable.getColumn("column3_bis"));
    }


    @Test
    public void alterNonKeyColumnType_CompatibleTypes() {
        Table sourceTable = new Table("test_table")
                .addColumn(new Column("name", BasicType.TEXT))
                .addColumn(new Column("column2", BasicType.TEXT))
                .addColumn(new Column("column3", BasicType.INT))
                .addPartitioningKey("name")
                .addClusteringColumn("column2")
                .setKeyspace(keyspace);

        Table targetTable = new Table("test_table2")
                .addColumn(new Column("name", BasicType.TEXT))
                .addColumn(new Column("column2", BasicType.TEXT))
                .addColumn(new Column("column3", BasicType.BLOB))
                .addPartitioningKey("name")
                .addClusteringColumn("column2")
                .setKeyspace(keyspace);

        TableComparator tableComparator = new TableComparator(sourceTable, targetTable);

        DeltaList list = tableComparator.compare();

        assertThat(list.hasUpdate()).isTrue();
        assertThat(list.getDeltas()).hasSize(1);
        assertThat(list.getFlags()).isEmpty();

        assertThat(list.getDeltas().get(0)).isInstanceOf(AlterTypeColumnDelta.class);
        assertThat(((AlterTypeColumnDelta) list.getDeltas().get(0)).getSource()).isEqualTo(sourceTable.getColumn("column3"));
        assertThat(((AlterTypeColumnDelta) list.getDeltas().get(0)).getTarget()).isEqualTo(targetTable.getColumn("column3"));
    }

    @Test
    public void alterNonKeyColumnType_NotCompatibleTypes() {
        Table sourceTable = new Table("test_table")
                .addColumn(new Column("name", BasicType.TEXT))
                .addColumn(new Column("column2", BasicType.TEXT))
                .addColumn(new Column("column3", BasicType.INT))
                .addPartitioningKey("name")
                .addClusteringColumn("column2")
                .setKeyspace(keyspace);

        Table targetTable = new Table("test_table2")
                .addColumn(new Column("name", BasicType.TEXT))
                .addColumn(new Column("column2", BasicType.TEXT))
                .addColumn(new Column("column3", BasicType.VARCHAR))
                .addPartitioningKey("name")
                .addClusteringColumn("column2")
                .setKeyspace(keyspace);

        TableComparator tableComparator = new TableComparator(sourceTable, targetTable);

        DeltaList list = tableComparator.compare();

        assertThat(list.hasUpdate()).isTrue();
        assertThat(list.getDeltas()).hasSize(2);
        assertThat(list.getFlags()).contains(DeltaFlag.DATA_LOSS);

        assertThat(list.getDeltas().get(0)).isInstanceOf(DropColumnDelta.class);
        assertThat(((ColumnAbstractDelta) list.getDeltas().get(0)).getSource()).isEqualTo(sourceTable.getColumn("column3"));
        assertThat(((ColumnAbstractDelta) list.getDeltas().get(0)).getTarget()).isNull();

        assertThat(list.getDeltas().get(1)).isInstanceOf(CreateColumnDelta.class);
        assertThat(((CreateColumnDelta) list.getDeltas().get(1)).getSource()).isNull();
        assertThat(((CreateColumnDelta) list.getDeltas().get(1)).getTarget()).isEqualTo(targetTable.getColumn("column3"));
    }


    @Test
    public void alterPartitioningKeyColumnType_CompatibleTypes() {
        Table sourceTable = new Table("test_table")
                .addColumn(new Column("name", BasicType.TEXT))
                .addColumn(new Column("column2", BasicType.TEXT))
                .addColumn(new Column("column3", BasicType.TEXT))
                .addPartitioningKey("name")
                .addClusteringColumn("column2")
                .setKeyspace(keyspace);

        Table targetTable = new Table("test_table2")
                .addColumn(new Column("name", BasicType.BLOB))
                .addColumn(new Column("column2", BasicType.TEXT))
                .addColumn(new Column("column3", BasicType.TEXT))
                .addPartitioningKey("name")
                .addClusteringColumn("column2")
                .setKeyspace(keyspace);

        TableComparator tableComparator = new TableComparator(sourceTable, targetTable);

        DeltaList list = tableComparator.compare();

        assertThat(list.hasUpdate()).isTrue();
        assertThat(list.getDeltas()).hasSize(1);
        assertThat(list.getFlags()).isEmpty();

        assertThat(list.getDeltas().get(0)).isInstanceOf(AlterTypeColumnDelta.class);
        assertThat(((AlterTypeColumnDelta) list.getDeltas().get(0)).getSource()).isEqualTo(sourceTable.getColumn("name"));
        assertThat(((AlterTypeColumnDelta) list.getDeltas().get(0)).getTarget()).isEqualTo(targetTable.getColumn("name"));
    }


    @Test
    public void alterPartitioningKeyColumnType_NotCompatibleTypes() {
        Table sourceTable = new Table("test_table")
                .addColumn(new Column("name", BasicType.TEXT))
                .addColumn(new Column("column2", BasicType.TEXT))
                .addColumn(new Column("column3", BasicType.TEXT))
                .addPartitioningKey("name")
                .addClusteringColumn("column2")
                .setKeyspace(keyspace);

        Table targetTable = new Table("test_table2")
                .addColumn(new Column("name", BasicType.UUID))
                .addColumn(new Column("column2", BasicType.TEXT))
                .addColumn(new Column("column3", BasicType.TEXT))
                .addPartitioningKey("name")
                .addClusteringColumn("column2")
                .setKeyspace(keyspace);

        TableComparator tableComparator = new TableComparator(sourceTable, targetTable);

        DeltaList list = tableComparator.compare();

        assertThat(list.hasUpdate()).isTrue();
        assertThat(list.getDeltas()).hasSize(2);
        assertThat(list.getFlags()).contains(DeltaFlag.DATA_LOSS);

        assertThat(list.getDeltas().get(0)).isInstanceOf(DropTableDelta.class);
        assertThat(((DropTableDelta) list.getDeltas().get(0)).getSource()).isEqualTo(sourceTable);
        assertThat(((DropTableDelta) list.getDeltas().get(0)).getTarget()).isNull();

        assertThat(list.getDeltas().get(1)).isInstanceOf(CreateTableDelta.class);
        assertThat(((CreateTableDelta) list.getDeltas().get(1)).getSource()).isNull();
        assertThat(((CreateTableDelta) list.getDeltas().get(1)).getTarget()).isEqualTo(targetTable);
    }


    @Test
    public void alterClusteringKeyColumnType_OrderCompatiblesType() {
        Table sourceTable = new Table("test_table")
                .addColumn(new Column("name", BasicType.TEXT))
                .addColumn(new Column("column2", BasicType.INT))
                .addColumn(new Column("column3", BasicType.TEXT))
                .addPartitioningKey("name")
                .addClusteringColumn("column2")
                .setKeyspace(keyspace);

        Table targetTable = new Table("test_table2")
                .addColumn(new Column("name", BasicType.TEXT))
                .addColumn(new Column("column2", BasicType.VARINT))
                .addColumn(new Column("column3", BasicType.TEXT))
                .addPartitioningKey("name")
                .addClusteringColumn("column2")
                .setKeyspace(keyspace);

        TableComparator tableComparator = new TableComparator(sourceTable, targetTable);

        DeltaList list = tableComparator.compare();

        assertThat(list.hasUpdate()).isTrue();
        assertThat(list.getDeltas()).hasSize(1);
        assertThat(list.getFlags()).isEmpty();

        assertThat(list.getDeltas().get(0)).isInstanceOf(AlterTypeColumnDelta.class);
        assertThat(((AlterTypeColumnDelta) list.getDeltas().get(0)).getSource()).isEqualTo(sourceTable.getColumn("column2"));
        assertThat(((AlterTypeColumnDelta) list.getDeltas().get(0)).getTarget()).isEqualTo(targetTable.getColumn("column2"));
    }

    @Test
    public void alterClusteringKeyColumnType_NotOrderCompatiblesType() {
        Table sourceTable = new Table("test_table")
                .addColumn(new Column("name", BasicType.TEXT))
                .addColumn(new Column("column2", BasicType.TEXT))
                .addColumn(new Column("column3", BasicType.TEXT))
                .addPartitioningKey("name")
                .addClusteringColumn("column2")
                .setKeyspace(keyspace);

        Table targetTable = new Table("test_table2")
                .addColumn(new Column("name", BasicType.TEXT))
                .addColumn(new Column("column2", BasicType.BLOB))
                .addColumn(new Column("column3", BasicType.TEXT))
                .addPartitioningKey("name")
                .addClusteringColumn("column2")
                .setKeyspace(keyspace);

        TableComparator tableComparator = new TableComparator(sourceTable, targetTable);

        DeltaList list = tableComparator.compare();

        assertThat(list.hasUpdate()).isTrue();
        assertThat(list.getDeltas()).hasSize(2);
        assertThat(list.getFlags()).contains(DeltaFlag.DATA_LOSS);

        assertThat(list.getDeltas().get(0)).isInstanceOf(DropTableDelta.class);
        assertThat(((DropTableDelta) list.getDeltas().get(0)).getSource()).isEqualTo(sourceTable);
        assertThat(((DropTableDelta) list.getDeltas().get(0)).getTarget()).isNull();

        assertThat(list.getDeltas().get(1)).isInstanceOf(CreateTableDelta.class);
        assertThat(((CreateTableDelta) list.getDeltas().get(1)).getSource()).isNull();
        assertThat(((CreateTableDelta) list.getDeltas().get(1)).getTarget()).isEqualTo(targetTable);
    }

    @Test
    public void alterClusteringKeyColumnType_NotCompatiblesType() {
        Table sourceTable = new Table("test_table")
                .addColumn(new Column("name", BasicType.TEXT))
                .addColumn(new Column("column2", BasicType.TEXT))
                .addColumn(new Column("column3", BasicType.TEXT))
                .addPartitioningKey("name")
                .addClusteringColumn("column2")
                .setKeyspace(keyspace);

        Table targetTable = new Table("test_table2")
                .addColumn(new Column("name", BasicType.TEXT))
                .addColumn(new Column("column2", BasicType.UUID))
                .addColumn(new Column("column3", BasicType.TEXT))
                .addPartitioningKey("name")
                .addClusteringColumn("column2")
                .setKeyspace(keyspace);

        TableComparator tableComparator = new TableComparator(sourceTable, targetTable);

        DeltaList list = tableComparator.compare();

        assertThat(list.hasUpdate()).isTrue();
        assertThat(list.getDeltas()).hasSize(2);
        assertThat(list.getFlags()).contains(DeltaFlag.DATA_LOSS);

        assertThat(list.getDeltas().get(0)).isInstanceOf(DropTableDelta.class);
        assertThat(((DropTableDelta) list.getDeltas().get(0)).getSource()).isEqualTo(sourceTable);
        assertThat(((DropTableDelta) list.getDeltas().get(0)).getTarget()).isNull();

        assertThat(list.getDeltas().get(1)).isInstanceOf(CreateTableDelta.class);
        assertThat(((CreateTableDelta) list.getDeltas().get(1)).getSource()).isNull();
        assertThat(((CreateTableDelta) list.getDeltas().get(1)).getTarget()).isEqualTo(targetTable);
    }

    @Test
    public void alterClusteringKeyColumnSortedOrder() {
        Table sourceTable = new Table("test_table")
                .addColumn(new Column("name", BasicType.TEXT))
                .addColumn(new Column("column2", BasicType.TEXT))
                .addColumn(new Column("column3", BasicType.TEXT))
                .addPartitioningKey("name")
                .addClusteringColumn("column2", SortOrder.ASC)
                .setKeyspace(keyspace);

        Table targetTable = new Table("test_table2")
                .addColumn(new Column("name", BasicType.TEXT))
                .addColumn(new Column("column2", BasicType.TEXT))
                .addColumn(new Column("column3", BasicType.TEXT))
                .addPartitioningKey("name")
                .addClusteringColumn("column2", SortOrder.DESC)
                .setKeyspace(keyspace);

        TableComparator tableComparator = new TableComparator(sourceTable, targetTable);

        DeltaList list = tableComparator.compare();

        assertThat(list.hasUpdate()).isTrue();
        assertThat(list.getDeltas()).hasSize(2);
        assertThat(list.getFlags()).contains(DeltaFlag.DATA_LOSS);

        assertThat(list.getDeltas().get(0)).isInstanceOf(DropTableDelta.class);
        assertThat(((DropTableDelta) list.getDeltas().get(0)).getSource()).isEqualTo(sourceTable);
        assertThat(((DropTableDelta) list.getDeltas().get(0)).getTarget()).isNull();

        assertThat(list.getDeltas().get(1)).isInstanceOf(CreateTableDelta.class);
        assertThat(((CreateTableDelta) list.getDeltas().get(1)).getSource()).isNull();
        assertThat(((CreateTableDelta) list.getDeltas().get(1)).getTarget()).isEqualTo(targetTable);
    }

    @Test
    public void alterClusteringKeyColumnOrder() {
        Table sourceTable = new Table("test_table")
                .addColumn(new Column("name", BasicType.TEXT))
                .addColumn(new Column("column2", BasicType.TEXT))
                .addColumn(new Column("column3", BasicType.TEXT))
                .addPartitioningKey("name")
                .addClusteringColumn("column2", SortOrder.ASC)
                .addClusteringColumn("column3", SortOrder.DESC)
                .setKeyspace(keyspace);

        Table targetTable = new Table("test_table2")
                .addColumn(new Column("name", BasicType.TEXT))
                .addColumn(new Column("column2", BasicType.TEXT))
                .addColumn(new Column("column3", BasicType.TEXT))
                .addPartitioningKey("name")
                .addClusteringColumn("column3", SortOrder.DESC)
                .addClusteringColumn("column2", SortOrder.ASC)
                .setKeyspace(keyspace);

        TableComparator tableComparator = new TableComparator(sourceTable, targetTable);

        DeltaList list = tableComparator.compare();

        assertThat(list.hasUpdate()).isTrue();
        assertThat(list.getDeltas()).hasSize(2);
        assertThat(list.getFlags()).contains(DeltaFlag.DATA_LOSS);

        assertThat(list.getDeltas().get(0)).isInstanceOf(DropTableDelta.class);
        assertThat(((DropTableDelta) list.getDeltas().get(0)).getSource()).isEqualTo(sourceTable);
        assertThat(((DropTableDelta) list.getDeltas().get(0)).getTarget()).isNull();

        assertThat(list.getDeltas().get(1)).isInstanceOf(CreateTableDelta.class);
        assertThat(((CreateTableDelta) list.getDeltas().get(1)).getSource()).isNull();
        assertThat(((CreateTableDelta) list.getDeltas().get(1)).getTarget()).isEqualTo(targetTable);
    }

    @Test
    public void alterCreateColumn() {
        Table sourceTable = new Table("test_table")
                .addColumn(new Column("name", BasicType.TEXT))
                .addColumn(new Column("column2", BasicType.TEXT))
                .addColumn(new Column("column3", BasicType.TEXT))
                .addPartitioningKey("name")
                .addClusteringColumn("column2")
                .setKeyspace(keyspace);

        Table targetTable = new Table("test_table2")
                .addColumn(new Column("name", BasicType.TEXT))
                .addColumn(new Column("column2", BasicType.TEXT))
                .addColumn(new Column("column3", BasicType.TEXT))
                .addColumn(new Column("column4", BasicType.UUID))
                .addPartitioningKey("name")
                .addClusteringColumn("column2")
                .setKeyspace(keyspace);

        TableComparator tableComparator = new TableComparator(sourceTable, targetTable);

        DeltaList list = tableComparator.compare();

        assertThat(list.hasUpdate()).isTrue();
        assertThat(list.getDeltas()).hasSize(1);
        assertThat(list.getFlags()).doesNotContain(DeltaFlag.DATA_LOSS);

        assertThat(list.getDeltas().get(0)).isInstanceOf(CreateColumnDelta.class);
        assertThat(((ColumnAbstractDelta) list.getDeltas().get(0)).getSource()).isNull();
        assertThat(((ColumnAbstractDelta) list.getDeltas().get(0)).getTarget()).isEqualTo(targetTable.getColumn("column4"));
    }

    @Test
    public void alterDropColumn() {
        Table sourceTable = new Table("test_table")
                .addColumn(new Column("name", BasicType.TEXT))
                .addColumn(new Column("column2", BasicType.TEXT))
                .addColumn(new Column("column3", BasicType.TEXT))
                .addColumn(new Column("column4", BasicType.UUID))
                .addPartitioningKey("name")
                .addClusteringColumn("column2")
                .setKeyspace(keyspace);

        Table targetTable = new Table("test_table2")
                .addColumn(new Column("name", BasicType.TEXT))
                .addColumn(new Column("column2", BasicType.TEXT))
                .addColumn(new Column("column3", BasicType.TEXT))
                .addPartitioningKey("name")
                .addClusteringColumn("column2")
                .setKeyspace(keyspace);

        TableComparator tableComparator = new TableComparator(sourceTable, targetTable);

        DeltaList list = tableComparator.compare();

        assertThat(list.hasUpdate()).isTrue();
        assertThat(list.getDeltas()).hasSize(1);
        assertThat(list.getFlags()).contains(DeltaFlag.DATA_LOSS);

        assertThat(list.getDeltas().get(0)).isInstanceOf(DropColumnDelta.class);
        assertThat(((ColumnAbstractDelta) list.getDeltas().get(0)).getSource()).isEqualTo(sourceTable.getColumn("column4"));
        assertThat(((ColumnAbstractDelta) list.getDeltas().get(0)).getTarget()).isNull();
    }
}
