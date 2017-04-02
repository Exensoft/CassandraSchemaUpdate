package fr.exensoft.cassandra.schemaupdate;


import fr.exensoft.cassandra.schemaupdate.comparator.delta.AbstractDelta;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaResult;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.keyspace.CreateKeyspaceDelta;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.table.CreateTableDelta;
import fr.exensoft.cassandra.schemaupdate.model.Column;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;
import fr.exensoft.cassandra.schemaupdate.model.type.BasicType;
import fr.exensoft.cassandra.schemaupdate.model.type.SetType;
import fr.exensoft.cassandra.schemaupdate.model.values.SortOrder;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;


import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class SchemaUpdateTest {

    @Test
    public void builderTest_BuilderError() {
        SchemaUpdate.Builder schemaUpdateBuilder = new SchemaUpdate.Builder();

        assertThatThrownBy(()->schemaUpdateBuilder.build())
                .isInstanceOf(SchemaUpdateException.class);
    }



    @Test
    public void createPatchTest_BuilderWithCluster() {
        SchemaUpdate schemaUpdate = new SchemaUpdate.Builder()
                .withCluster(CassandraTestUtils.createCluster())
                .build();

        // Keyspace description like the "CassandraTestUtils" sample keyspace
        Keyspace targetKeyspace = new Keyspace("keyspace1")
                .addTable(
                        new Table("table1")
                                .addColumn(new Column("column1", BasicType.UUID))
                                .addColumn(new Column("column2", new SetType(BasicType.TEXT)))
                                .addPartitioningKey("column1")
                )
                .addTable(
                        new Table("table2")
                                .addColumn(new Column("column1", BasicType.INT))
                                .addColumn(new Column("column2", BasicType.TEXT))
                                .addColumn(new Column("column3", BasicType.INT))
                                .addColumn(new Column("column4", BasicType.TEXT))
                                .addColumn(new Column("column5", BasicType.TEXT))
                                .addPartitioningKey("column1")
                                .addClusteringColumn("column2")
                                .addClusteringColumn("column3", SortOrder.DESC)
                                .addIndex("test_index", "column5")
                );

        DeltaResult patch = schemaUpdate.createPatch(targetKeyspace);

        assertThat(patch.getKeyspaceDelta().hasUpdate()).isFalse();
        assertThat(patch.getTablesDelta()).hasSize(2);
        assertThat(patch.getTablesDelta().get("table1").hasUpdate()).isFalse();
        assertThat(patch.getTablesDelta().get("table2").hasUpdate()).isFalse();
    }


    @Test
    public void createPatchTest_BuilderWithCassandraConnection() {
        CassandraConnection cassandraConnection = Mockito.mock(CassandraConnection.class);
        Mockito.doReturn(null).when(cassandraConnection).loadKeyspace(Mockito.anyString());

        SchemaUpdate schemaUpdate = new SchemaUpdate.Builder()
                .withCassandraConnection(cassandraConnection)
                .build();

        Keyspace targetKeyspace = new Keyspace("keyspace1")
                .addTable(
                        new Table("table1")
                                .addColumn(new Column("column1", BasicType.UUID))
                                .addColumn(new Column("column2", new SetType(BasicType.TEXT)))
                                .addPartitioningKey("column1")
                );

        DeltaResult patch = schemaUpdate.createPatch(targetKeyspace);

        assertThat(patch.getKeyspaceDelta().hasUpdate()).isTrue();
        assertThat(patch.getKeyspaceDelta().getDeltas()).hasSize(1);
        assertThat(patch.getKeyspaceDelta().getDeltas().get(0)).isInstanceOf(CreateKeyspaceDelta.class);

        assertThat(patch.getTablesDelta()).hasSize(1);
        assertThat(patch.getTablesDelta().get("table1").hasUpdate()).isTrue();
        assertThat(patch.getTablesDelta().get("table1").getDeltas()).hasSize(1);
        assertThat(patch.getTablesDelta().get("table1").getDeltas().get(0)).isInstanceOf(CreateTableDelta.class);
    }

    @Test
    public void applyPatchTest_BuilderWithCassandraConnection() {
        Keyspace targetKeyspace = new Keyspace("keyspace1")
                .addTable(
                        new Table("table1")
                                .addColumn(new Column("column1", BasicType.UUID))
                                .addColumn(new Column("column2", new SetType(BasicType.TEXT)))
                                .addPartitioningKey("column1")
                );


        CassandraConnection cassandraConnection = Mockito.mock(CassandraConnection.class);
        Mockito.doReturn(null).when(cassandraConnection).loadKeyspace(Mockito.anyString());

        SchemaUpdate schemaUpdate = new SchemaUpdate.Builder()
                .withCassandraConnection(cassandraConnection)
                .build();

        DeltaResult patch = schemaUpdate.createPatch(targetKeyspace);

        schemaUpdate.applyPatch(patch);

        ArgumentCaptor<AbstractDelta> argumentCaptor = ArgumentCaptor.forClass(AbstractDelta.class);
        Mockito.verify(cassandraConnection, Mockito.times(2)).applyDelta(argumentCaptor.capture());

        assertThat(argumentCaptor.getAllValues()).hasSize(2);
        assertThat(argumentCaptor.getAllValues().get(0)).isInstanceOf(CreateKeyspaceDelta.class);
        assertThat(argumentCaptor.getAllValues().get(1)).isInstanceOf(CreateTableDelta.class);
    }

    @Test
    public void applyPatchTest_BuilderWithCassandraConnection_NoUpdates() {
        Keyspace sourceKeyspace = new Keyspace("keyspace1")
                .addTable(
                        new Table("table1")
                                .addColumn(new Column("column1", BasicType.UUID))
                                .addColumn(new Column("column2", new SetType(BasicType.TEXT)))
                                .addPartitioningKey("column1")
                );

        Keyspace targetKeyspace = new Keyspace("keyspace1")
                .addTable(
                        new Table("table1")
                                .addColumn(new Column("column1", BasicType.UUID))
                                .addColumn(new Column("column2", new SetType(BasicType.TEXT)))
                                .addPartitioningKey("column1")
                );


        CassandraConnection cassandraConnection = Mockito.mock(CassandraConnection.class);
        Mockito.doReturn(sourceKeyspace).when(cassandraConnection).loadKeyspace(Mockito.anyString());

        SchemaUpdate schemaUpdate = new SchemaUpdate.Builder()
                .withCassandraConnection(cassandraConnection)
                .build();

        DeltaResult patch = schemaUpdate.createPatch(targetKeyspace);

        schemaUpdate.applyPatch(patch);

        Mockito.verify(cassandraConnection, Mockito.times(0)).applyDelta(Mockito.any());
    }
    @Test
    public void closeTest_BuilderWithCassandraConnection() {
        CassandraConnection cassandraConnection = Mockito.mock(CassandraConnection.class);

        SchemaUpdate schemaUpdate = new SchemaUpdate.Builder()
                .withCassandraConnection(cassandraConnection)
                .build();

        schemaUpdate.close();

        Mockito.verify(cassandraConnection, Mockito.times(1)).close();
    }
}
