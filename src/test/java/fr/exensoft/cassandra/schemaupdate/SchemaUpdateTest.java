package fr.exensoft.cassandra.schemaupdate;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaList;
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

import java.util.List;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class SchemaUpdateTest {

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
}
