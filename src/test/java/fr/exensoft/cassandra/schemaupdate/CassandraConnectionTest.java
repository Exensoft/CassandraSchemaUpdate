package fr.exensoft.cassandra.schemaupdate;


import com.datastax.driver.core.*;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;
import fr.exensoft.cassandra.schemaupdate.model.type.BasicType;
import fr.exensoft.cassandra.schemaupdate.model.type.SetType;
import fr.exensoft.cassandra.schemaupdate.model.values.SortOrder;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class CassandraConnectionTest {

    protected ResultSet createResultSet(List<Row> rows) {
        ResultSet rs = Mockito.mock(ResultSet.class);
        if(rows.size() > 0) {
            Mockito.doReturn(rows.get(0)).when(rs).one();
        }
        else {
            Mockito.doReturn(null).when(rs).one();
        }

        Mockito.doReturn(rows.iterator()).when(rs).iterator();
        return rs;
    }

    private Row createKeyspaceRow(String keyspace_name) {
        Row row = Mockito.mock(Row.class);
        Mockito.doReturn(keyspace_name).when(row).getString(Mockito.eq("keyspace_name"));
        return row;
    }

    private Row createKeyspaceTableRow(String columnfamily_name) {
        Row row = Mockito.mock(Row.class);
        Mockito.doReturn(columnfamily_name).when(row).getString(Mockito.eq("columnfamily_name"));
        return row;
    }

    private Row createTableColumnRow(String column_name, int component_index, String index_name, String index_options, String index_type, String type, String validator) {
        Row row = Mockito.mock(Row.class);
        Mockito.doReturn(column_name).when(row).getString(Mockito.eq("column_name"));
        Mockito.doReturn(component_index).when(row).getInt(Mockito.eq("component_index"));
        Mockito.doReturn(index_name).when(row).getString(Mockito.eq("index_name"));
        Mockito.doReturn(index_options).when(row).getString(Mockito.eq("index_options"));
        Mockito.doReturn(index_type).when(row).getString(Mockito.eq("index_type"));
        Mockito.doReturn(type).when(row).getString(Mockito.eq("type"));
        Mockito.doReturn(validator).when(row).getString(Mockito.eq("validator"));
        return row;
    }

    private PreparedStatement createTableColumnsStatement(Session session) {
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        BoundStatement boundStatement = Mockito.mock(BoundStatement.class);
        AtomicReference<String> targetTable = new AtomicReference<>("table1");

        Mockito.doReturn(boundStatement).when(preparedStatement).bind();

        Mockito.doAnswer((params)->{
            targetTable.set((String) params.getArguments()[1]);
            return null;
        }).when(boundStatement).setString(Mockito.eq(1), Mockito.anyString());

        ResultSet table1RS = createResultSet(Arrays.asList(
                createTableColumnRow("column1", 1, "", "", "", "partition_key", "org.apache.cassandra.db.marshal.UUIDType"),
                createTableColumnRow("column2", 0, "", "", "", "", "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type)")
        ));

        ResultSet table2RS = createResultSet(Arrays.asList(
                createTableColumnRow("column1", 1, "", "", "", "partition_key", "org.apache.cassandra.db.marshal.Int32Type"),
                createTableColumnRow("column2", 1, "", "", "", "clustering_key", "org.apache.cassandra.db.marshal.UTF8Type"),
                createTableColumnRow("column3", 2, "", "", "", "clustering_key", "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.Int32Type)"),
                createTableColumnRow("column4", 0, "", "", "", "", "org.apache.cassandra.db.marshal.UTF8Type"),
                createTableColumnRow("column5", 0, "test_index", "{}", "COMPOSITES", "", "org.apache.cassandra.db.marshal.UTF8Type")
        ));

        Mockito.doAnswer((params)->{
            if(targetTable.get().equals("table1")) {
                return table1RS;
            }
            else if(targetTable.get().equals("table2")) {
                return table2RS;
            }
            return null;
        }).when(session).execute(Mockito.eq(boundStatement));

        return preparedStatement;
    }

    private PreparedStatement createSchemaKeyspaceTablesStatement(Session session) {
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);

        BoundStatement boundStatement = Mockito.mock(BoundStatement.class);

        Mockito.doReturn(boundStatement).when(preparedStatement).bind();

        Mockito.doReturn(createResultSet(Arrays.asList(createKeyspaceTableRow("table1"), createKeyspaceTableRow("table2")))).when(session).execute(Mockito.eq(boundStatement));

        return preparedStatement;
    }

    private PreparedStatement createSchemaKeyspacesStatement(Session session) {
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        BoundStatement boundStatement = Mockito.mock(BoundStatement.class);

        Mockito.doReturn(boundStatement).when(preparedStatement).bind();

        Mockito.doReturn(createResultSet(Arrays.asList(createKeyspaceRow("keyspace1")))).when(session).execute(Mockito.eq(boundStatement));

        return preparedStatement;
    }

    private Session createSession() {
        Session session = Mockito.mock(Session.class);

        ResultSet keyspacesRS = createResultSet(Arrays.asList(createKeyspaceRow("keyspace1"), createKeyspaceRow("keyspace2")));

        Mockito.doReturn(keyspacesRS).when(session).execute(Mockito.eq("SELECT * FROM system.schema_keyspaces"));
        Mockito.doReturn(createSchemaKeyspacesStatement(session)).when(session).prepare(Mockito.eq("SELECT * FROM system.schema_keyspaces WHERE keyspace_name=?"));
        Mockito.doReturn(createSchemaKeyspaceTablesStatement(session)).when(session).prepare(Mockito.eq("SELECT * FROM system.schema_columnfamilies WHERE keyspace_name=?"));
        Mockito.doReturn(createTableColumnsStatement(session)).when(session).prepare(Mockito.eq("SELECT * FROM system.schema_columns WHERE keyspace_name=? AND columnfamily_name=?"));

        return session;
    }

    private Cluster createCluster() {
        Cluster cluster = Mockito.mock(Cluster.class);
        Session session = createSession();
        Mockito.doReturn(session).when(cluster).connect();
        return cluster;
    }

    @Test
    public void listKeyspacesTest() {
        CassandraConnection connection = new CassandraConnection(createCluster());

        connection.connect();

        List<String> keyspaces = connection.listKeyspaces();

        assertThat(keyspaces).containsOnly("keyspace1","keyspace2");
    }

    @Test
    public void loadKeyspaceTest() {
        CassandraConnection connection = new CassandraConnection(createCluster());

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
