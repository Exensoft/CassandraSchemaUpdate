package fr.exensoft.cassandra.schemaupdate.loader;

import com.datastax.driver.core.*;
import fr.exensoft.cassandra.schemaupdate.model.Column;
import fr.exensoft.cassandra.schemaupdate.model.Index;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;
import fr.exensoft.cassandra.schemaupdate.model.type.*;
import fr.exensoft.cassandra.schemaupdate.model.values.IndexOption;
import fr.exensoft.cassandra.schemaupdate.model.values.SortOrder;

import java.util.List;
import java.util.stream.Collectors;

public class ClusterSchemaLoader implements SchemaLoader {

    private Cluster cluster;

    public ClusterSchemaLoader(Cluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public List<String> listKeyspaces() {
        List<KeyspaceMetadata> keyspaceMetadata = cluster.getMetadata().getKeyspaces();

        return keyspaceMetadata.stream()
                .map(KeyspaceMetadata::getName)
                .collect(Collectors.toList());
    }

    @Override
    public Keyspace loadKeyspace(String name) {
        KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(name);
        if(keyspaceMetadata == null) {
            return null;
        }

        Keyspace keyspace = new Keyspace(keyspaceMetadata.getName());

        // Load tables
        loadTables(keyspaceMetadata).forEach(keyspace::addTable);

        return keyspace;
    }

    @Override
    public List<Table> loadTables(String keyspace_name) {
        return loadTables(cluster.getMetadata().getKeyspace(keyspace_name));
    }

    @Override
    public Table loadTable(String keyspace_name, String table_name) {
        return null;
    }

    private List<Table> loadTables(KeyspaceMetadata keyspaceMetadata) {
        if(keyspaceMetadata == null) {
            return null;
        }

        return keyspaceMetadata.getTables().stream()
                .map(this::loadTable)
                .collect(Collectors.toList());
    }


    private Table loadTable(TableMetadata tableMetadata) {
        Table table = new Table(tableMetadata.getName());

        for(ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            Column column = new Column(columnMetadata.getName(), convertType(columnMetadata.getType()));
            table.addColumn(column);
        }

        for(ColumnMetadata columnMetadata : tableMetadata.getPartitionKey()) {
            table.addPartitioningKey(columnMetadata.getName());
        }

        for(int i=0;i<tableMetadata.getClusteringColumns().size();i++) {
            ColumnMetadata columnMetadata = tableMetadata.getClusteringColumns().get(i);
            ClusteringOrder clusteringOrder = tableMetadata.getClusteringOrder().get(i);

            SortOrder sortOrder = SortOrder.ASC;
            if(clusteringOrder == ClusteringOrder.DESC) {
                sortOrder = SortOrder.DESC;
            }

            table.addClusteringColumn(columnMetadata.getName(), sortOrder);
        }

        for(IndexMetadata indexMetadata : tableMetadata.getIndexes()) {
            IndexOption kind = IndexOption.VALUES;

            if(IndexMetadata.Kind.KEYS.equals(indexMetadata.getKind())) {
                kind = IndexOption.KEYS;
            }

            table.addIndex(indexMetadata.getName(), indexMetadata.getTarget(), kind);
        }

        return table;
    }

    private ColumnType convertType(DataType dataType) {
        ColumnType result = null;
        if(dataType.getName().name().equalsIgnoreCase("set")) {
            result = new SetType(convertType(dataType.getTypeArguments().get(0)));
        }
        else if(dataType.getName().name().equalsIgnoreCase("list")) {
            result = new ListType(convertType(dataType.getTypeArguments().get(0)));
        }
        else if(dataType.getName().name().equalsIgnoreCase("map")) {
            result = new MapType(convertType(dataType.getTypeArguments().get(0)), convertType(dataType.getTypeArguments().get(1)));
        }
        else {
            // Basic types
            for (BasicType type : BasicType.values()) {
                if (type.getType().equalsIgnoreCase(dataType.getName().name())) {
                    result = type;
                    break;
                }
            }
        }

        if(dataType.isFrozen()) {
            result = new FrozenType(result);
        }


        return result;
    }
}
