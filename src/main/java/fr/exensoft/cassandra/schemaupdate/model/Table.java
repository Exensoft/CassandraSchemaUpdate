package fr.exensoft.cassandra.schemaupdate.model;

import fr.exensoft.cassandra.schemaupdate.SchemaUpdateException;
import fr.exensoft.cassandra.schemaupdate.model.values.IndexOption;
import fr.exensoft.cassandra.schemaupdate.model.values.SortOrder;
import sun.reflect.generics.tree.BaseType;

import java.util.*;

/**
 * Table object represents a Cassandra table with its columns and its indexes.
 */
public class Table {

    private String name;

    private List<Column> columns;

    private List<Column> partitioningKeys;

    private List<Column> clusteringColumns;

    private Map<Column, SortOrder> sortOrders;

    private List<Index> indexes;

    private int innerIndex = 0;

    private Keyspace keyspace;

    /**
     * Creates a new table with the name given in parameters
     * @param name Table name
     */
    public Table(String name) {
        this.name = name;
        this.columns = new ArrayList<>();
        this.partitioningKeys = new ArrayList<>();
        this.clusteringColumns = new ArrayList<>();
        this.sortOrders = new HashMap<>();
        this.indexes = new ArrayList<>();
    }

    /**
     * Returns the name of the table
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the columns of the table
     * @return
     */
    public List<Column> getColumns() {
        return columns;
    }

    /**
     * Returns the partitionning key columns
     * @return
     */
    public List<Column> getPartitioningKeys() {
        return partitioningKeys;
    }

    /**
     * Returns the clustering key columns
     * @return
     */
    public List<Column> getClusteringColumns() {
        return clusteringColumns;
    }

    /**
     * Returns the indexes (on non key columns)
     * @return
     */
    public List<Index> getIndexes() {
        return indexes;
    }

    /**
     * Returns the keyspace of the table
     * @return
     */
    public Keyspace getKeyspace() {
        return keyspace;
    }

    /**
     * Set the keyspace of the table, the keyspace is automatically set when you add the
     * table in a Keyspace object (by using addTable method)
     * @param keyspace
     * @return
     */
    public Table setKeyspace(Keyspace keyspace) {
        this.keyspace = keyspace;
        return this;
    }

    /**
     * Add a new column in the table.
     * If a column with the same name exists already, a SchemaUpdateException will be thrown.
     * @param column The new column to add
     * @return The table object itself (allow you to chain the addColumn calls)
     */
    public Table addColumn(Column column) {
        if(hasColumn(column.getName())) {
            throw new SchemaUpdateException(String.format("Table \"%s\" already has a column with name \"%s\"", name, column.getName()));
        }
        column.setInnerIndex(innerIndex);
        innerIndex++;
        columns.add(column);
        return this;
    }

    /**
     * Add a column in the partitioning key.
     * You must add the column in the table before.
     * @param columnName
     * @return
     */
    public Table addPartitioningKey(String columnName) {
        Column column = getColumn(columnName);
        if(column == null) {
            throw new SchemaUpdateException(String.format("Table \"%s\" has no column named \"%s\"", name, columnName));
        }
        column.setIndex(partitioningKeys.size());
        partitioningKeys.add(column);
        return this;
    }

    /**
     * Add a column in the clustering columns, the default sort order (ASC) will be used
     * You must add the column in the table before.
     * @param columnName
     * @return
     */
    public Table addClusteringColumn(String columnName) {
        Column column = getColumn(columnName);
        if(column == null) {
            throw new SchemaUpdateException(String.format("Table \"%s\" has no column named \"%s\"", name, columnName));
        }
        column.setIndex(clusteringColumns.size());
        if(!sortOrders.containsKey(column)) {
            sortOrders.put(column, SortOrder.ASC);
        }
        clusteringColumns.add(column);
        return this;
    }

    /**
     * Add a column in the clustering columns with the specified sort order
     * You must add the column in the table before.
     * @param columnName
     * @param sortOrder
     * @return
     */
    public Table addClusteringColumn(String columnName, SortOrder sortOrder) {
        Column column = getColumn(columnName);
        if(column == null) {
            throw new SchemaUpdateException(String.format("Table \"%s\" has no column named \"%s\"", name, columnName));
        }
        column.setIndex(clusteringColumns.size());
        sortOrders.put(column, sortOrder);
        clusteringColumns.add(column);
        return this;
    }

    /**
     * Add index to the specified column.
     * You can not add index on clustering columns or partitioning key
     * You must add the column in the table before.
     * @param name Name of the index
     * @param columnName Name of the column
     * @return
     */
    public Table addIndex(String name, String columnName) {
        return addIndex(name, columnName, IndexOption.VALUES);
    }

    /**
     * Add index to the specified column.
     * You can not add index on clustering columns or partitioning key
     * You must add the column in the table before.
     * @param name Name of the index
     * @param columnName Name of the column
     * @param indexOption Index option to use, when column type is a map you can specify if you want a key based index or a value based index
     * @return
     */
    public Table addIndex(String name, String columnName, IndexOption indexOption) {
        Column column = getColumn(columnName);
        if(column == null) {
            throw new SchemaUpdateException(String.format("Table \"%s\" has no column named \"%s\"", name, columnName));
        }
        Index index = new Index(name, column);

        if(!(column.getType() instanceof BaseType)) {
            index.addOption(indexOption);
        }

        indexes.add(index);
        return this;
    }

    /**
     * Set sort order of a clustering column
     * @param columnName Name of the clustering column
     * @param sortOrder Sort order to set for this column
     * @return
     */
    public Table setOrder(String columnName, SortOrder sortOrder) {
        Column column = getColumn(columnName);
        if(column == null) {
            throw new SchemaUpdateException(String.format("Table \"%s\" has no column named \"%s\"", name, columnName));
        }

        sortOrders.put(column, sortOrder);

        return this;
    }

    /**
     * Returns the map of clustering columns sort orders
     * @return
     */
    public Map<Column, SortOrder> getSortOrders() {
        return sortOrders;
    }

    /**
     * Validate the table.
     * Order columns (partitioning columns first then clustering columns and finally "normal" columns)
     *
     * In the future, this method will check the table consistency :
     *  - Check if partitioning key columns are not part of clustering columns
     *  - Check if clustering columns are not part of the partitioning key
     *  - Check if clustering columns and partitioning key have no indexes
     *  - Check column type consistency
     *  ...
     */
    public void validate() {
        //Reorganize columns order
        Collections.sort(columns, (a,b)->{
            int priorityA = partitioningKeys.contains(a)?3:(clusteringColumns.contains(a)?2:1);
            int priorityB = partitioningKeys.contains(b)?3:(clusteringColumns.contains(b)?2:1);
            int comparation = -Integer.compare(priorityA, priorityB);
            if(comparation == 0) {
                if(priorityA > 1) {
                    return Integer.compare(a.getIndex(), b.getIndex());
                }
                return Integer.compare(a.getInnerIndex(), b.getInnerIndex());
            }
            return comparation;
        });
    }

    /**
     * Returns a column of the table by its name
     * @param name
     * @return
     */
    public Column getColumn(String name) {
        return columns.stream()
                .filter(c->c.getName().equals(name))
                .findFirst()
                .orElse(null);
    }

    /**
     * Returns an index of the table by its column
     * @param column
     * @return
     */
    public Index getIndex(Column column) {
        return indexes.stream()
                .filter(i->i.getColumn() == column)
                .findFirst()
                .orElse(null);
    }

    /**
     * Check if the table has a column with the given name
     * @param name
     * @return
     */
    private boolean hasColumn(String name) {
        return columns.stream()
                .anyMatch(c->c.getName().equals(name));
    }



    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\tColumns :\n");
        for(Column column : columns) {
            sb.append("\t\t - ").append(column.getName()).append(" ").append(column.getType()).append("\n");
        }
        sb.append("\tPartitioning keys : \n");
        for(Column column : partitioningKeys) {
            sb.append("\t\t - ").append(column.getName()).append("\n");
        }
        sb.append("\tClustering keys : \n");
        for(Column column : clusteringColumns) {
            sb.append("\t\t - ").append(column.getName()).append(" (").append(sortOrders.get(column)).append(")\n");
        }
        if(!indexes.isEmpty()) {
            sb.append("\tIndexes : \n");
            for(Index index : indexes) {
                sb.append("\t\t - ").append(index.getName()).append(" (").append(index.getColumn().getName()).append(")\n");
            }
        }

        return sb.toString();
    }
}
