package fr.exensoft.cassandra.schemaupdate.loader;

import com.datastax.driver.core.*;
import fr.exensoft.cassandra.schemaupdate.model.Column;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;
import fr.exensoft.cassandra.schemaupdate.model.type.ColumnType;
import fr.exensoft.cassandra.schemaupdate.model.values.IndexOption;
import fr.exensoft.cassandra.schemaupdate.model.values.SortOrder;
import fr.exensoft.cassandra.schemaupdate.utils.CQLTypeConverter;

import java.util.LinkedList;
import java.util.List;

public class CassandraV3SchemaLoader implements SchemaLoader{

    private final static String PARTITION_KEY = "partition_key";
    private final static String CLUSTERING_KEY = "clustering_key";
    private final static String REGULAR = "regular";

    private final static String INDEX_COMPOSITES = "COMPOSITES";

    private Session session;

    private PreparedStatement selectKeyspace;
    private PreparedStatement selectKeyspaceTables;
    private PreparedStatement selectTableColumns;

    public CassandraV3SchemaLoader(Session session) {
        this.session = session;

        selectKeyspace = session.prepare("SELECT * FROM system_schema.keyspaces WHERE keyspace_name=?");
        selectKeyspaceTables = session.prepare("SELECT * FROM system.schema_columnfamilies WHERE keyspace_name=?");
        selectTableColumns = session.prepare("SELECT * FROM system.schema_columns WHERE keyspace_name=? AND columnfamily_name=?");
    }

    @Override
    public List<String> listKeyspaces() {
        ResultSet resultSet = session.execute("SELECT * FROM system.schema_keyspaces");
        List<String> result = new LinkedList<>();
        for(Row row : resultSet) {
            result.add(row.getString("keyspace_name"));
        }
        return result;
    }

    @Override
    public Keyspace loadKeyspace(String name) {
        BoundStatement statement = selectKeyspace.bind();
        statement.setString(0, name);

        ResultSet resultSet = session.execute(statement);
        Row row = resultSet.one();
        if(row == null) {
            return null;
        }

        Keyspace keyspace = new Keyspace(row.getString("keyspace_name"));

        // Load tables
        loadTables(name).forEach(keyspace::addTable);

        return keyspace;
    }

    @Override
    public List<Table> loadTables(String keyspace_name) {
        BoundStatement statement = selectKeyspaceTables.bind();
        statement.setString(0, keyspace_name);

        List<Table> tables = new LinkedList<>();

        ResultSet resultSet = session.execute(statement);

        for(Row row : resultSet) {
            tables.add(loadTable(keyspace_name, row.getString("columnfamily_name")));
        }

        return tables;
    }

    @Override
    public Table loadTable(String keyspace_name, String table_name) {
        BoundStatement statement = selectTableColumns.bind();
        statement.setString(0, keyspace_name);
        statement.setString(1, table_name);

        Table table = new Table(table_name);

        ResultSet resultSet = session.execute(statement);
        for(Row row : resultSet) {
            String columnName = row.getString("column_name");
            int componentIndex = row.getInt("component_index");
            String indexName = row.getString("index_name");
            String indexOptions = row.getString("index_options");
            String indexType = row.getString("index_type");
            String type = row.getString("type");
            String validator = row.getString("validator");

            //Extract information from validator
            ColumnType columnType = CQLTypeConverter.validatorToType(validator);
            boolean isReversed = CQLTypeConverter.isReversed(validator);

            Column column = new Column(columnName, columnType);
            table.addColumn(column);

            if(PARTITION_KEY.equals(type)) {
                table.addPartitioningKey(columnName);
            }
            else if(CLUSTERING_KEY.equals(type)) {
                table.addClusteringColumn(columnName);
                if(isReversed) {
                    table.setOrder(columnName, SortOrder.DESC);
                }
            }
            else if(INDEX_COMPOSITES.equals(indexType)){
                //TODO : add a real option parsing
                IndexOption option = IndexOption.VALUES;
                if(indexOptions != null && indexOptions.contains(IndexOption.KEYS.getValue())) {
                    option = IndexOption.KEYS;
                }
                table.addIndex(indexName, columnName, option);
            }
            column.setIndex(componentIndex);
        }

        table.validate();
        return table;
    }
}
