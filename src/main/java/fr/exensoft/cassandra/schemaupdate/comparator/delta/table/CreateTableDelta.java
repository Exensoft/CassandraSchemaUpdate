package fr.exensoft.cassandra.schemaupdate.comparator.delta.table;

import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaPriorities;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaType;
import fr.exensoft.cassandra.schemaupdate.model.Column;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;
import fr.exensoft.cassandra.schemaupdate.model.values.SortOrder;

import java.util.stream.Collectors;

public class CreateTableDelta extends TableAbstractDelta {

    public CreateTableDelta(Keyspace keyspace, Table table) {
        super(keyspace, null, table);
    }

    @Override
    public DeltaType getDeltaType() {
        return DeltaType.CREATE;
    }

    @Override
    public int getPriority() {
        return DeltaPriorities.CREATE_TABLE;
    }

    @Override
    public String generateCQL() {
        StringBuilder tableQuery = new StringBuilder();
        tableQuery.append("CREATE TABLE \"")
                .append(keyspace.getName())
                .append("\".\"")
                .append(target.getName())
                .append("\" (");
        //Columns description
        tableQuery.append(target.getColumns().stream().map(this::generateColumnCQL).collect(Collectors.joining(", ")));
        //Primary Key
        tableQuery.append(", PRIMARY KEY (");
        //Partitioning Key(s)
        if (target.getPartitioningKeys().size() > 1) {
            tableQuery.append("(")
                    .append(target.getPartitioningKeys().stream().map(c -> String.format("\"%s\"", c.getName())).collect(Collectors.joining(", ")))
                    .append(")");
        } else {
            tableQuery.append("\"")
                    .append(target.getPartitioningKeys().get(0).getName())
                    .append("\"");
        }
        //Clustering Key(s)
        if(!target.getClusteringColumns().isEmpty()) {
            tableQuery.append(", ")
                    .append(target.getClusteringColumns().stream().map(c -> String.format("\"%s\"", c.getName())).collect(Collectors.joining(", ")));
        }
        tableQuery.append(")");
        //End of table query
        tableQuery.append(")");

        // Clustering Keys Orders ? (when not ASC)
        if(target.getSortOrders().values().stream().anyMatch(SortOrder.DESC::equals)) {
            tableQuery.append(" WITH CLUSTERING ORDER BY (")
                    .append(target.getClusteringColumns().stream()
                            .filter(column->target.getSortOrders().get(column).equals(SortOrder.DESC))
                            .map(column->String.format("\"%s\" DESC", column.getName()))
                            .collect(Collectors.joining(", "))
                    )
                    .append(")");
        }

        tableQuery.append(";");
        return tableQuery.toString();
    }
    private String generateColumnCQL(Column column) {
        return String.format("\"%s\" %s", column.getName(), column.getType().getType());
    }
    @Override
    public String toString() {
        return String.format("Create table \"%s\"", target.getName());
    }
}
