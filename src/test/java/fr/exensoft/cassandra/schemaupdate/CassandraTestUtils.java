package fr.exensoft.cassandra.schemaupdate;


import com.datastax.driver.core.*;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class CassandraTestUtils {

    private static ResultSet createResultSet(List<Row> rows) {
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

    private static Row createVersionRow(String version) {
        Row row = Mockito.mock(Row.class);
        Mockito.doReturn(version).when(row).getString(Mockito.eq("release_version"));
        return row;
    }

    private static Row createKeyspaceRow(String keyspace_name) {
        Row row = Mockito.mock(Row.class);
        Mockito.doReturn(keyspace_name).when(row).getString(Mockito.eq("keyspace_name"));
        return row;
    }

    private static Row createKeyspaceTableRow(String columnfamily_name) {
        Row row = Mockito.mock(Row.class);
        Mockito.doReturn(columnfamily_name).when(row).getString(Mockito.eq("columnfamily_name"));
        return row;
    }

    private static Row createTableColumnRow(String column_name, int component_index, String index_name, String index_options, String index_type, String type, String validator) {
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

    private static PreparedStatement createTableColumnsStatement(Session session) {
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
                createTableColumnRow("column3", 2, "", "", "", "clustering_key", "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.Int32Type)"),
                createTableColumnRow("column2", 1, "", "", "", "clustering_key", "org.apache.cassandra.db.marshal.UTF8Type"),
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

    private static PreparedStatement createSchemaKeyspaceTablesStatement(Session session) {
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);

        BoundStatement boundStatement = Mockito.mock(BoundStatement.class);

        Mockito.doReturn(boundStatement).when(preparedStatement).bind();

        Mockito.doReturn(createResultSet(Arrays.asList(createKeyspaceTableRow("table1"), createKeyspaceTableRow("table2")))).when(session).execute(Mockito.eq(boundStatement));

        return preparedStatement;
    }

    private static PreparedStatement createSchemaKeyspacesStatement(Session session) {
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        BoundStatement boundStatement = Mockito.mock(BoundStatement.class);

        Mockito.doReturn(boundStatement).when(preparedStatement).bind();

        Mockito.doReturn(createResultSet(Arrays.asList(createKeyspaceRow("keyspace1")))).when(session).execute(Mockito.eq(boundStatement));

        return preparedStatement;
    }

    private static Session createSession() {
        Session session = Mockito.mock(Session.class);

        ResultSet versionRS = createResultSet(Arrays.asList(createVersionRow("2.2.9")));
        ResultSet keyspacesRS = createResultSet(Arrays.asList(createKeyspaceRow("keyspace1"), createKeyspaceRow("keyspace2")));

        Mockito.doReturn(versionRS).when(session).execute(Mockito.eq("select release_version from system.local"));
        Mockito.doReturn(keyspacesRS).when(session).execute(Mockito.eq("SELECT * FROM system.schema_keyspaces"));
        Mockito.doReturn(createSchemaKeyspacesStatement(session)).when(session).prepare(Mockito.eq("SELECT * FROM system.schema_keyspaces WHERE keyspace_name=?"));
        Mockito.doReturn(createSchemaKeyspaceTablesStatement(session)).when(session).prepare(Mockito.eq("SELECT * FROM system.schema_columnfamilies WHERE keyspace_name=?"));
        Mockito.doReturn(createTableColumnsStatement(session)).when(session).prepare(Mockito.eq("SELECT * FROM system.schema_columns WHERE keyspace_name=? AND columnfamily_name=?"));

        return session;
    }

    public static Cluster createCluster() {
        Cluster cluster = Mockito.mock(Cluster.class);
        Session session = createSession();
        Mockito.doReturn(session).when(cluster).connect();
        return cluster;
    }
}
