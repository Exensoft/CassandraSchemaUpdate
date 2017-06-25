package fr.exensoft.cassandra.schemaupdate;


import com.datastax.driver.core.*;
import fr.exensoft.cassandra.schemaupdate.cluster.CassandraClusterMock;
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

public class CassandraConnectionTest {

    @Test
    public void applyPatchTest() {
        Keyspace targetKeyspace = new Keyspace("test")
                .addTable(new Table("table1")
                        .addColumn(new Column("column1", BasicType.UUID))
                        .addColumn(new Column("column2", BasicType.VARCHAR))
                        .addColumn(new Column("column3", BasicType.VARINT))
                        .addPartitioningKey("column1")
                );

        CreateTableDelta delta = new CreateTableDelta(targetKeyspace, targetKeyspace.getTable("table1"));

        Cluster cluster = Mockito.mock(Cluster.class);
        Session session = Mockito.mock(Session.class);
        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.doReturn(session).when(cluster).connect();
        CassandraConnection cassandraConnection = new CassandraConnection(cluster);

        cassandraConnection.connect();

        cassandraConnection.applyDelta(delta);

        Mockito.verify(session, Mockito.times(1)).execute(argumentCaptor.capture());
        assertThat(argumentCaptor.getValue()).isEqualTo(delta.generateCQL());

    }

    @Test
    public void connectCloseTest() {
        Cluster cluster = Mockito.mock(Cluster.class);
        Session session = Mockito.mock(Session.class);
        Mockito.doReturn(session).when(cluster).connect();
        CassandraConnection cassandraConnection = new CassandraConnection(cluster);

        // Double call to verify number of calls
        cassandraConnection.connect();
        cassandraConnection.connect();

        Mockito.verify(cluster, Mockito.times(1)).connect();

        cassandraConnection.close();
        cassandraConnection.close();

        Mockito.verify(cluster, Mockito.times(1)).close();
        Mockito.verify(session, Mockito.times(1)).close();
    }

    @Test
    public void listKeyspacesTest() {

        CassandraConnection connection = new CassandraConnection(new CassandraClusterMock().createCluster());

        connection.connect();

        List<String> keyspaces = connection.listKeyspaces();

        assertThat(keyspaces).containsOnly("keyspace1","keyspace2");
    }

    @Test
    public void loadKeyspaceTest() {

        CassandraConnection connection = new CassandraConnection(new CassandraClusterMock().createCluster());

        connection.connect();

        Keyspace keyspace = connection.loadKeyspace("keyspace1");

        assertThat(keyspace.getTables()).hasSize(2);

        Table table1 = keyspace.getTable("table1");
        assertThat(table1.getColumns()).hasSize(2);
        assertThat(table1.getColumn("column1")).isNotNull();
        assertThat(table1.getColumn("column1").getType()).isEqualTo(BasicType.UUID);
        assertThat(table1.getColumn("column2")).isNotNull();
        assertThat(table1.getColumn("column2").getType()).isEqualTo(new SetType(BasicType.TEXT));
        assertThat(table1.getClusteringColumns()).isEmpty();
        assertThat(table1.getPartitioningKeys()).containsOnly(table1.getColumn("column1"));
        assertThat(table1.getIndexes()).isEmpty();

        Table table2 = keyspace.getTable("table2");
        assertThat(table2.getColumns()).hasSize(5);
        assertThat(table2.getColumn("column1")).isNotNull();
        assertThat(table2.getColumn("column1").getType()).isEqualTo(BasicType.INT);
        assertThat(table2.getColumn("column2")).isNotNull();
        assertThat(table2.getColumn("column2").getType()).isEqualTo(BasicType.TEXT);
        assertThat(table2.getColumn("column3")).isNotNull();
        assertThat(table2.getColumn("column3").getType()).isEqualTo(BasicType.INT);
        assertThat(table2.getColumn("column4")).isNotNull();
        assertThat(table2.getColumn("column4").getType()).isEqualTo(BasicType.TEXT);
        assertThat(table2.getColumn("column5")).isNotNull();
        assertThat(table2.getColumn("column5").getType()).isEqualTo(BasicType.TEXT);
        assertThat(table2.getPartitioningKeys()).containsOnly(table2.getColumn("column1"));
        assertThat(table2.getClusteringColumns()).containsOnly(table2.getColumn("column2"), table2.getColumn("column3"));
        assertThat(table2.getSortOrders().get(table2.getColumn("column2"))).isEqualTo(SortOrder.ASC);
        assertThat(table2.getSortOrders().get(table2.getColumn("column3"))).isEqualTo(SortOrder.DESC);
        assertThat(table2.getIndexes()).hasSize(1);
        assertThat(table2.getIndex(table2.getColumn("column5")).getName()).isEqualTo("test_index");
    }

}
