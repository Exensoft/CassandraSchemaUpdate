package fr.exensoft.cassandra.schemaupdate.cluster;

import com.datastax.driver.core.*;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.values.IndexOption;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class CassandraClusterMock extends CassandraTestUtils{

    private ColumnMetadata createColumnMetadata(String name, DataType type) {
        ColumnMetadata columnMetadata = Mockito.mock(ColumnMetadata.class);

        Mockito.doReturn(name).when(columnMetadata).getName();
        Mockito.doReturn(type).when(columnMetadata).getType();

        return columnMetadata;
    }

    private IndexMetadata createIndex(String name, String target, IndexMetadata.Kind kind) {
        IndexMetadata indexMetadata = Mockito.mock(IndexMetadata.class);

        Mockito.doReturn(name).when(indexMetadata).getName();
        Mockito.doReturn(target).when(indexMetadata).getTarget();
        Mockito.doReturn(kind).when(indexMetadata).getKind();

        return indexMetadata;
    }

    private TableMetadata createTableMetadata(String name, List<ColumnMetadata> columns, List<ColumnMetadata> partitioningKeys, List<ColumnMetadata> clusteringColumns, List<ClusteringOrder> clusteringOrders, List<IndexMetadata> indexes) {
        TableMetadata tableMetadata = Mockito.mock(TableMetadata.class);

        Mockito.doReturn(name).when(tableMetadata).getName();
        Mockito.doReturn(columns).when(tableMetadata).getColumns();
        Mockito.doReturn(partitioningKeys).when(tableMetadata).getPartitionKey();
        Mockito.doReturn(clusteringColumns).when(tableMetadata).getClusteringColumns();
        Mockito.doReturn(clusteringOrders).when(tableMetadata).getClusteringOrder();
        Mockito.doReturn(indexes).when(tableMetadata).getIndexes();

        return tableMetadata;
    }

    private KeyspaceMetadata createKeyspaceMetadata(String keyspace, List<TableMetadata> tables) {
        KeyspaceMetadata keyspaceMetadata = Mockito.mock(KeyspaceMetadata.class);

        Mockito.doReturn(keyspace).when(keyspaceMetadata).getName();

        Mockito.doReturn(tables).when(keyspaceMetadata).getTables();

        return keyspaceMetadata;
    }

    private TableMetadata createTable1() {

        ColumnMetadata column1 = createColumnMetadata("column1", DataType.uuid());

        ColumnMetadata column2 = createColumnMetadata("column2", DataType.set(DataType.text()));

        return createTableMetadata("table1", Arrays.asList(
                column1,
                column2
            ), Arrays.asList(
                column1
            ), Collections.emptyList(), Arrays.asList(
                ClusteringOrder.ASC
            ),
            Collections.emptyList()
        );
    }

    private TableMetadata createTable2() {

        ColumnMetadata column1 = createColumnMetadata("column1", DataType.cint());
        ColumnMetadata column2 = createColumnMetadata("column2", DataType.text());
        ColumnMetadata column3 = createColumnMetadata("column3", DataType.cint());
        ColumnMetadata column4 = createColumnMetadata("column4", DataType.text());
        ColumnMetadata column5 = createColumnMetadata("column5", DataType.text());

        return createTableMetadata("table2", Arrays.asList(
                column1,
                column2,
                column3,
                column4,
                column5
            ), Arrays.asList(
                column1
            ), Arrays.asList(
                column2,
                column3
            ), Arrays.asList(
                ClusteringOrder.ASC,
                ClusteringOrder.DESC
            ), Arrays.asList(
                createIndex("test_index","column5", IndexMetadata.Kind.KEYS)
            )
        );
    }

    private Metadata createMetadata() {
        Metadata metadata = Mockito.mock(Metadata.class);

        Mockito.doReturn(createKeyspaceMetadata("keyspace1", Arrays.asList(
                createTable1(),
                createTable2()
        ))).when(metadata).getKeyspace(Mockito.eq("keyspace1"));

        Mockito.doReturn(Arrays.asList(createKeyspaceMetadata("keyspace1", null), createKeyspaceMetadata("keyspace2", null))).when(metadata).getKeyspaces();

        return metadata;
    }

    public Cluster createCluster() {
        Cluster cluster = Mockito.mock(Cluster.class);

        Metadata metadata = createMetadata();
        Mockito.doReturn(metadata).when(cluster).getMetadata();

        Session session = Mockito.mock(Session.class);
        Mockito.doReturn(session).when(cluster).connect();

        return cluster;
    }

}
